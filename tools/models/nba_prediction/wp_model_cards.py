"""Model cards for the NBA / WNBA in-game WP boosters, and the holdout gate for the rated pregame prior (BB-D25).

Two steps, so every number on a card comes from a committed artefact:

1. ``evaluate`` (network: the ESPN pbp / schedule and the stats possessions releases). For each league:

   * **Prior gate.** Every completed game of the prior's holdout seasons gets four pregame home
     probabilities through :func:`~sportsdataverse.nba.nba_game_page.home_prior`: home court alone
     (``hfa``), the default before this change (``line_or_hfa``), the rated prior alone (``rated``:
     :func:`~sportsdataverse.nba.nba_rated_prior.nba_rated_priors`, as of the game's date) and the
     prior a caller gets by passing ``rated_home_prob`` (``line_or_rated``). The rated prior has no
     fitted constant of its own: its penalty, probit scale and regression factor are the season
     sim's, fit on other seasons (but chosen after a look at these; see ``CAVEATS``). The gate:
     ``rated`` scores at least as well as ``hfa``, and ``line_or_rated`` at least as well as
     ``line_or_hfa``, on Brier AND log loss; calibration is reported, not gated. The
     per-game rows are the test fixture ``tests/fixtures/nba_prediction/<lg>_rated_prior_games.parquet``.
   * **Card metrics.** Every game of the booster's holdout season (``train_in_game_wp.LEAGUES``)
     through :func:`~sportsdataverse.nba.nba_game_page.score_win_prob`: Brier, log loss, Cox
     calibration slope and calibration-in-the-large by period (live rows; the settled horn rows are
     scored apart as ``final``), the final minute, the decile reliability table, and the horn /
     final-minute behaviour of ``train_in_game_wp.late_game_metrics``. Scored with ``line_or_hfa``
     and ``line_or_rated``, and (NBA, where nearly every game has a line) with the line withheld,
     which is the case the rated prior serves.

   Writes ``in_game_wp_<lg>_card_metrics.json`` beside this script, with the sha256 of the booster it
   scored and of the fit report, and the season sim constants the rated prior used.

2. ``card`` (offline): writes ``sportsdataverse/nba/models/<lg>_in_game_wp.card.json`` from that json,
   the fit's ``in_game_wp_<lg>_holdout.json`` and the bundled booster (its sha256 and attributes).
   ``tests/nba/test_nba_wp_model_cards.py`` rebuilds the cards and fails on any drift.

Usage::

    uv run python tools/models/nba_prediction/wp_model_cards.py evaluate --league nba
    uv run python tools/models/nba_prediction/wp_model_cards.py evaluate --league wnba
    uv run python tools/models/nba_prediction/wp_model_cards.py card
    uv run python tools/models/nba_prediction/wp_model_cards.py asof-fixture   # the as-of test fixture
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from sportsdataverse._common.metrics import brier_score, calibration_table, log_loss_score
from sportsdataverse.nba.nba_game_page import _game_end, game_regulation_periods, home_prior, score_win_prob
from sportsdataverse.nba.nba_rated_prior import nba_rated_priors

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
MODELS_DIR = REPO / "sportsdataverse" / "nba" / "models"
FIXTURE_DIR = REPO / "tests" / "fixtures" / "nba_prediction"
_spec = importlib.util.spec_from_file_location("train_in_game_wp", HERE / "train_in_game_wp.py")
assert _spec is not None and _spec.loader is not None
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)
sys.path.insert(0, str(REPO / "tools"))
from fit_preseason_prior import paired  # noqa: E402  (game-clustered paired log-loss difference)

LEAGUE_IDS = {"nba": "00", "wnba": "10"}
#: The rated prior's holdout: seasons disjoint from those its constants were fit on
#: (``nba_season_sim.SIM_CONSTANTS`` / ``PRIOR_CONSTANTS``: NBA train 2018 2019 2022 2023, WNBA 2018 2019 2021).
#: WNBA 2026 stays reserved by the 2026-11-02 sim-vs-market spec.
PRIOR_HOLDOUT = {"nba": [2024, 2025, 2026], "wnba": [2024, 2025]}
PRIORS = ("hfa", "line_or_hfa", "rated", "line_or_rated")
GAME_COLS = ["season", "game_id", "neutral_site", "home_team_spread", "game_spread_available", "rated_home_prob", "y"]
CAVEATS = [
    "Inputs are the margin, the clock, possession and the pregame prior only: no fouls, timeouts, bonus or lineups.",
    "The prior is a single number per game; injuries and rest after the line or rating are not seen.",
    "NBA trains on the spread prior; a lineless NBA game gets the rated prior, which the booster saw only "
    "through the spread's scale.",
    "WNBA trains on the weekly box-ratings prior and serves the possession-ridge rated prior: same scale, "
    "different estimator.",
    "Train and holdout seasons are whole seasons, so the holdout is out of sample in time but shares rules eras.",
    "The rated prior's constants were fit on other seasons, but the NBA penalty and probit scale were kept after "
    "a look at the 2024-2026 seasons the prior gate scores (nba_season_sim.PRIOR_CONSTANTS).",
    "The home-court baseline's constants were fit on 2024, a prior-gate season, which favours the baseline.",
    "The prior gate pools seasons; a single WNBA season is too small to clear two standard errors alone "
    "(gate_by_season).",
    "The rated prior is opt-in: score_win_prob and game_page_tables serve it only when the caller passes "
    "rated_home_prob; without it a lineless game gets home court alone.",
]


def _p(y: np.ndarray, p: np.ndarray) -> dict[str, float]:
    return {"n": int(y.size), "brier": round(brier_score(y, p), 5), "log_loss": round(log_loss_score(y, p), 5)}


def _cal(y: np.ndarray, p: np.ndarray) -> dict[str, float]:
    return _p(y, p) | {
        # home court alone is (nearly) one value per game: no slope to fit
        "calibration_slope": round(fit.calibration_slope(y, p), 4) if float(p.std()) > 0.01 else None,
        "calibration_in_the_large": round(float(p.mean() - y.mean()), 4),
    }


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rated_prior_constants(league_id: str) -> dict[str, list[float]]:
    """The season sim constants the rated prior is computed with (a test pins them to the scored ones)."""
    from sportsdataverse.nba.nba_season_sim import PRIOR_CONSTANTS, SIM_CONSTANTS

    return {"sim_constants": list(SIM_CONSTANTS[league_id]), "prior_constants": list(PRIOR_CONSTANTS[league_id])}


def season_pbp(lg: str, season: int) -> pl.DataFrame:
    return fit._load_pbp(LEAGUE_IDS[lg], season)


def season_games(lg: str, season: int, pbp: pl.DataFrame, cache: Path) -> pl.DataFrame:
    """One row per finished, untied game of ``pbp``: the line, the rated prior, the outcome (cached)."""
    path = cache / f"{lg}_rated_games_{season}.parquet"
    if path.exists():
        return pl.read_parquet(path)
    finals = pbp.group_by("game_id").agg(
        pl.col("home_team_spread", "game_spread_available").first(),
        pl.col("home_score").last().alias("h"),
        pl.col("away_score").last().alias("a"),
    )
    finals = finals.filter(pl.col("h") != pl.col("a")).with_columns(
        (pl.col("h") > pl.col("a")).cast(pl.Float64).alias("y")
    )
    rated = nba_rated_priors(season, league_id=LEAGUE_IDS[lg]).select("game_id", "neutral_site", "rated_home_prob")
    assert finals.schema["game_id"] == rated.schema["game_id"], "game_id dtype differs between pbp and priors"
    out = finals.join(rated, on="game_id", how="left", validate="1:1").with_columns(
        pl.lit(season, dtype=pl.Int32).alias("season"), pl.col("neutral_site").fill_null(False)
    )
    cache.mkdir(parents=True, exist_ok=True)
    out.select(GAME_COLS).sort("game_id").write_parquet(path)
    return out.select(GAME_COLS).sort("game_id")


def prior_probs(games: pl.DataFrame, lg: str) -> pl.DataFrame:
    """The four pregame probabilities of the gate, each through ``home_prior``."""
    cols: dict[str, list[float]] = {k: [] for k in PRIORS}
    for r in games.iter_rows(named=True):
        line, avail, neutral, rated = (
            r["home_team_spread"],
            r["game_spread_available"],
            r["neutral_site"],
            r["rated_home_prob"],
        )
        cols["hfa"].append(home_prior(None, False, league=lg, neutral_site=neutral)[0])
        cols["line_or_hfa"].append(home_prior(line, avail, league=lg, neutral_site=neutral)[0])
        cols["rated"].append(home_prior(None, False, league=lg, neutral_site=neutral, rated_home_prob=rated)[0])
        cols["line_or_rated"].append(home_prior(line, avail, league=lg, neutral_site=neutral, rated_home_prob=rated)[0])
    return games.with_columns(pl.Series(f"p_{k}", v, dtype=pl.Float64) for k, v in cols.items())


def prior_gate(frame: pl.DataFrame) -> dict[str, Any]:
    """Scores of the four priors, the game-clustered paired log-loss differences, and the pass flags."""
    y = frame["y"].to_numpy()
    scores = {k: _cal(y, frame[f"p_{k}"].to_numpy()) for k in PRIORS}
    lined = frame.filter(pl.col("game_spread_available").fill_null(False))
    out: dict[str, Any] = {
        "games": frame.height,
        "games_with_line": lined.height,
        "games_rated": int(frame["rated_home_prob"].is_not_null().sum()),
        "seasons": sorted(set(frame["season"].to_list())),
        "scores": scores,
        "paired_log_loss": {
            "rated - hfa": paired(frame, "p_rated", "p_hfa"),
            # None: no gate game uses the rating in place of a line (NBA: only the unrated All-Star games lack one)
            "line_or_rated - line_or_hfa": paired(frame, "p_line_or_rated", "p_line_or_hfa")
            if not frame["p_line_or_rated"].equals(frame["p_line_or_hfa"])
            else None,
        },
        "pass": {
            "rated_vs_hfa": scores["rated"]["brier"] <= scores["hfa"]["brier"]
            and scores["rated"]["log_loss"] <= scores["hfa"]["log_loss"],
            "line_or_rated_vs_line_or_hfa": scores["line_or_rated"]["brier"] <= scores["line_or_hfa"]["brier"]
            and scores["line_or_rated"]["log_loss"] <= scores["line_or_hfa"]["log_loss"],
        },
    }
    if lined.height:  # what the rating gives up against a line, where there is one
        yl = lined["y"].to_numpy()
        out["games_with_line_scores"] = {k: _p(yl, lined[f"p_{k}"].to_numpy()) for k in ("rated", "line_or_hfa")}
    return out


def serve(lg: str, pbp: pl.DataFrame, games: pl.DataFrame, *, rated: bool, withhold_line: bool) -> pl.DataFrame:
    """Every game of ``pbp`` through ``score_win_prob`` with the prior the flags pick, plus the outcome ``y``."""
    reg = game_regulation_periods(lg, season=int(pbp["season"][0]))
    facts = {r["game_id"]: r for r in games.iter_rows(named=True)}
    if withhold_line:
        pbp = pbp.with_columns(pl.lit(False).alias("game_spread_available"))
    out = []
    for (gid,), g in pbp.group_by("game_id", maintain_order=True):
        f = facts.get(gid)
        if f is None:  # tied or unfinished
            continue
        wp = score_win_prob(
            g,
            league=lg,
            regulation_periods=reg,
            neutral_site=f["neutral_site"],
            rated_home_prob=f["rated_home_prob"] if rated else None,
        )
        out.append(wp.with_columns(pl.lit(f["y"]).alias("y")))
    return pl.concat(out, how="diagonal_relaxed")


def card_metrics(lg: str, scored: pl.DataFrame) -> dict[str, Any]:
    """Pregame, by-period (live), final-minute, final (settled) and horn metrics of one scoring pass."""
    reg = game_regulation_periods(lg, season=int(scored["season"][0]))
    s = scored.with_columns(_game_end(reg).alias("_settled"))
    live = s.filter(pl.col("_settled") == False)  # noqa: E712
    first = s.group_by("game_id", maintain_order=True).agg(pl.col("pregame_home_prob", "y").first())

    def block(sub: pl.DataFrame) -> dict[str, float]:
        return _cal(sub["y"].to_numpy(), sub["home_win_prob"].to_numpy())

    by_period = {str(k): block(live.filter(pl.col("period_number") == k)) for k in range(1, reg + 1)}
    ot = live.filter(pl.col("period_number") > reg)
    if ot.height:
        by_period["ot"] = block(ot)
    settled = s.filter(pl.col("_settled") == True)  # noqa: E712
    rel = calibration_table(live["y"].to_numpy(), live["home_win_prob"].to_numpy(), n_bins=10)
    return {
        "games": first.height,
        "pregame": _cal(first["y"].to_numpy(), first["pregame_home_prob"].to_numpy()),
        "live": block(live),
        "by_period": by_period,
        "final_minute_of_regulation": block(
            live.filter((pl.col("period_number") == reg) & (pl.col("start_game_seconds_remaining") <= 60))
        ),
        "final": {
            "rows": settled.height,
            "brier": round(brier_score(settled["y"].to_numpy(), settled["home_win_prob"].to_numpy()), 5),
        },
        "reliability_deciles": [
            {k: round(float(v), 4) if isinstance(v, float) else v for k, v in r.items()}
            for r in rel.iter_rows(named=True)
        ],
        "late_game": {
            k: round(v, 5) if isinstance(v, float) else v for k, v in fit.late_game_metrics(scored, reg).items()
        },
    }


def evaluate(lg: str, cache: Path) -> dict[str, Any]:
    lid = LEAGUE_IDS[lg]
    holdout = fit.LEAGUES[lid]["holdout"]
    frames, hold_pbp = [], None
    for season in sorted(set(PRIOR_HOLDOUT[lg]) | {holdout}):
        pbp = season_pbp(lg, season)
        games = season_games(lg, season, pbp, cache)
        if season in PRIOR_HOLDOUT[lg]:
            frames.append(games)
        if season == holdout:
            hold_pbp, hold_games = pbp, games
    gate_rows = prior_probs(pl.concat(frames), lg)
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    gate_rows.select(GAME_COLS).write_parquet(FIXTURE_DIR / f"{lg}_rated_prior_games.parquet", compression="zstd")
    passes = {"line_or_hfa": (False, False), "line_or_rated": (True, False)}
    if lg == "nba":  # nearly every NBA game has a line: score the lineless case the rated prior serves
        passes |= {"lineless_hfa": (False, True), "lineless_rated": (True, True)}
    assert hold_pbp is not None
    import xgboost as xgb

    report = {
        "league": lg,
        "booster_holdout_season": holdout,
        "booster_sha256": _sha(MODELS_DIR / fit.LEAGUES[lid]["artifact"]),
        "fit_report_sha256": _sha(HERE / f"in_game_wp_{lg}_holdout.json"),
        "rated_prior_constants": rated_prior_constants(lid),
        "xgboost_version": xgb.__version__,
        "prior_gate": prior_gate(gate_rows),
        "prior_gate_by_season": {
            str(s): prior_gate(gate_rows.filter(pl.col("season") == s)) for s in PRIOR_HOLDOUT[lg]
        },
        "holdout": {
            name: card_metrics(lg, serve(lg, hold_pbp, hold_games, rated=r, withhold_line=w))
            for name, (r, w) in passes.items()
        },
    }
    (HERE / f"in_game_wp_{lg}_card_metrics.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


#: The as-of fixture: the first three weeks of the WNBA 2025 regular season.
ASOF_SEASON, ASOF_DAYS = 2025, 21


def write_asof_fixture() -> Path:
    """Possession rows and stats games of the first ``ASOF_DAYS`` of WNBA ``ASOF_SEASON`` (the as-of test fixture)."""
    from sportsdataverse.nba.nba_rated_prior import _possession_rows

    rows, games = _possession_rows("10", ASOF_SEASON)
    games = games.filter(pl.col("season_type") == "regular-season")
    end = games["game_date"].min() + dt.timedelta(days=ASOF_DAYS)  # type: ignore[operator]
    keep = games.filter(pl.col("game_date") < end)
    path = FIXTURE_DIR / f"wnba_rated_prior_asof_{ASOF_SEASON}.parquet"
    rows.join(keep.select(pl.col("stats_game_id").alias("game_id")), on="game_id", how="semi").sort(
        "game_date", "game_id", "period", "possession_number"
    ).write_parquet(path, compression="zstd")
    keep.write_parquet(path.with_name(f"wnba_rated_prior_asof_games_{ASOF_SEASON}.parquet"))
    return path


def build_card(lg: str) -> dict[str, Any]:
    """The card, from the committed holdout json, card-metrics json and booster only (no network)."""
    lid = LEAGUE_IDS[lg]
    artifact = MODELS_DIR / fit.LEAGUES[lid]["artifact"]
    fitted = json.loads((HERE / f"in_game_wp_{lg}_holdout.json").read_text(encoding="utf-8"))
    metrics = json.loads((HERE / f"in_game_wp_{lg}_card_metrics.json").read_text(encoding="utf-8"))
    import xgboost as xgb

    booster = xgb.Booster()
    booster.load_model(str(artifact))
    assert booster.attr("holdout_season") == str(fitted["holdout_season"]) == str(metrics["booster_holdout_season"])
    assert booster.feature_names == fitted["features"], "booster features differ from the fit report"
    # the metrics must have been scored with THIS booster and fit report: rerun ``evaluate`` after a refit
    assert metrics["booster_sha256"] == _sha(artifact), f"{lg} card metrics were scored with another booster"
    assert metrics["fit_report_sha256"] == _sha(HERE / f"in_game_wp_{lg}_holdout.json"), "fit report changed"
    params = fitted["params"]
    return {
        "model_type": f"{lg}_in_game_wp",
        "xgboost_version": metrics["xgboost_version"],
        "objective": params["objective"],
        "features": fitted["features"],
        "n_features": len(fitted["features"]),
        "label": "home team won (ties dropped)",
        "training_seasons": fitted["train_seasons"],
        "early_stopping": {"validation_season": fitted["validation_season"], "refit_on_all_training_seasons": True},
        "holdout_seasons": [fitted["holdout_season"]],
        "n_training_rows": fitted["n_train_plays"],
        "hyperparameters": params,
        "num_boost_round": fitted["rounds"],
        "source": f"sportsdataverse-data espn_{lg}_pbp releases (published)",
        "fitting_script": booster.attr("fitting_script"),
        "card_script": "tools/models/nba_prediction/wp_model_cards.py",
        "sha256": _sha(artifact),
        "fit_holdout": {
            "baseline_ref": fitted["baseline_ref"],
            "baseline": fitted["baseline"],
            "refit": fitted["refit"],
        },
        "holdout_metrics": metrics["holdout"],
        "pregame_prior": {
            "line_or_rated": "home_prior with rated_home_prob passed: the line when game_spread_available, else "
            "the rating (nba_rated_prior.nba_rated_priors, as of the game's date), else home court",
            "line_or_hfa": "home_prior without it (the default): the line, else home court",
            "constants": metrics["rated_prior_constants"],
            "holdout_seasons": PRIOR_HOLDOUT[lg],
            "gate": metrics["prior_gate"],
            "gate_by_season": metrics["prior_gate_by_season"],
        },
        "caveats": CAVEATS,
    }


def write_cards() -> None:
    for lg in LEAGUE_IDS:
        path = MODELS_DIR / f"{lg}_in_game_wp.card.json"
        path.write_text(json.dumps(build_card(lg), indent=2) + "\n", encoding="utf-8", newline="\n")
        print(path)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    ev = sub.add_parser("evaluate")
    ev.add_argument("--league", choices=sorted(LEAGUE_IDS), required=True)
    ev.add_argument("--cache", type=Path, default=Path(".cache/wp_cards"))
    sub.add_parser("card")
    sub.add_parser("asof-fixture")
    args = ap.parse_args(argv)
    if args.cmd == "evaluate":
        report = evaluate(args.league, args.cache)
        print(json.dumps({"prior_gate": report["prior_gate"]}, indent=2))
    elif args.cmd == "asof-fixture":
        print(write_asof_fixture())
    else:
        write_cards()
    return 0


if __name__ == "__main__":
    sys.exit(main())
