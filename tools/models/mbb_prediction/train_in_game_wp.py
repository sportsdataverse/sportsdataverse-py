"""Fit, gate and bundle the men's / women's college in-game win-probability booster.

Run (any IP: the ESPN pbp / schedule / team-box releases)::

    uv run python tools/models/mbb_prediction/train_in_game_wp.py --league mbb
    uv run python tools/models/mbb_prediction/train_in_game_wp.py --league wbb

Defaults, both leagues (train and holdout seasons disjoint): train 2022, 2023, 2025 (2025 is also the
early-stopping validation season); holdout 2026; confirmation season 2024. 2024 is the pregame-constants
and ``pbp_sample_2024`` decile-gate season (tests/mbb, tests/wbb ``*_prediction_backtest.py``), so it
stays out of training and is scored once as a second out-of-sample season. 2021 (the COVID season) is
not used.

This is the NBA / WNBA refit's contract (``tools/models/nba_prediction/train_in_game_wp.py``, #1075),
applied to college: the same features (``_IN_GAME_FEATURES``), monotone constraints, xgboost params,
early stopping on the last training season then a refit on every training season, the holdout scored
through the serving path (``nba_game_page.score_win_prob``), and the same late-game metrics, imported
from that script rather than re-implemented. College specifics:

1. Prior. The weekly as-of ratings prior the college release enrichment serves
   (``mbb_win_prob._pregame_probs``, games rated on games strictly before the Monday of their week).
   Games with no as-of rating (about 11% of MBB games, the season's first days) are NOT trained on:
   the enrichment gives them a constant HFA-only fallback, and those games are mostly buy games the
   home side wins (MBB 2022 / 2023 / 2025: 0.90 / 0.90 / 0.88 home wins against a 0.60 fallback,
   0.62-0.65 in rated games), so with them in the trees learn "the HFA logit means a likely home win"
   and overpredict every game the game page serves with that same HFA prior. On the 2025 validation
   season (fit on 2022 + 2023, the holdout not looked at), served with the HFA prior: calibration
   intercept -0.276 -> -0.038, Brier 0.1370 -> 0.1343; with the rated prior unchanged (0.1207 ->
   0.1208). Requiring 8+ prior games per team as well (the old dev script's rule) was no better
   there (intercept -0.067). College lines are absent
   from the releases (MBB 2024-2026 and every WBB season carry no ``game_spread_available``), so a
   spread prior is not an option. The game page serves ``home_prior`` (spread, else HFA), which is
   what the holdout below is scored with.
2. Clock. Halves for MBB, quarters for WBB from 2016 (``game_regulation_periods``); overtime plays
   carry the period clock (``ot_adjusted_seconds``), as at serve time.
3. Two serving priors. The game page scores college games with an HFA-only prior (no lines) and the
   release enrichment with the rated prior, so a seeded random share of the rated training games
   (``HFA_PRIOR_SHARE``) is fit with the HFA prior instead: an uninformative but, unlike the
   fallback games, unbiased prior. The share is picked per league from {0, 0.25, 0.5} by the 2025
   validation log loss averaged over the two priors (fit on 2022 + 2023, the holdout not looked at):
   MBB 0 (0.39546; 0.39605 at 0.25, 0.39662 at 0.5), WBB 0.25 (0.35134; 0.35280 at 0, 0.35162 at 0.5).
   For WBB that moves the HFA-prior calibration intercept -0.226 -> -0.022 and Brier 0.12557 ->
   0.12446, with the rated prior's Brier 0.10565 -> 0.10570.

Replaces ``dev/mbb_prediction/train_in_game_wp.py`` (untracked): a 2023-only fit with a daily as-of
prior and no clock transform, whose booster carried no lineage and held home WP flat in the final
minute (no monotone constraint, OT folded to ``sec_left = 0``).

The holdout is scored for three models: the refit, the baseline booster read from git
(``--baseline-ref``, the shipped model before this refit), and a naive closed form (Stern 1994: the
final margin is normal around the current lead plus the prior's expected margin scaled by the time
left, ``P = Phi((lead + mu * t) / (sigma * sqrt t))`` with ``t`` the fraction of regulation left,
``sigma`` the league's ``margin_sd`` and ``mu = sigma * Phi^-1(prior)``). Metrics go to
``in_game_wp_<lg>_holdout.json`` beside this script; the late-game rows of the holdout go to the
offline test fixture ``tests/fixtures/<lg>_prediction/<lg>_late_game_<season>.parquet``.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Optional, Union

import numpy as np
import polars as pl
import xgboost as xgb
from scipy.stats import norm

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from sportsdataverse.mbb import mbb_game_predict  # noqa: E402
from sportsdataverse.mbb.mbb_game_predict import in_game_features, predict_margin, win_prob_from_margin  # noqa: E402
from sportsdataverse.mbb.mbb_prediction_constants import calibration_table, get_constants  # noqa: E402
from sportsdataverse.nba import nba_game_page  # noqa: E402
from sportsdataverse.nba.nba_game_page import game_regulation_periods, ot_adjusted_seconds, score_win_prob  # noqa: E402
from sportsdataverse.nba.nba_game_predict import _IN_GAME_FEATURES  # noqa: E402
from tools.models.nba_prediction import train_in_game_wp as shared  # noqa: E402

MODELS_DIR = REPO / "sportsdataverse" / "mbb" / "models"
HERE = Path(__file__).resolve().parent

LEAGUES: dict[str, dict[str, Any]] = {
    "mbb": {"league": "mens", "artifact": "mbb_in_game_wp.ubj", "train": [2022, 2023, 2025], "holdout": 2026},
    "wbb": {"league": "womens", "artifact": "wbb_in_game_wp.ubj", "train": [2022, 2023, 2025], "holdout": 2026},
}
# Share of rated training games fit with the HFA-only prior the game page serves (module docstring, 3).
HFA_PRIOR_SHARE = {"mbb": 0.0, "wbb": 0.25}
CONFIRMATION_SEASON = 2024
# The last commit before this refit: the baseline the holdout gate compares against.
BASELINE_REF = "eb4207476e"
REGULATION_SECONDS = 2400.0  # 40 minutes, both college leagues
_CLOCKS = ("start_quarter_seconds_remaining", "start_period_seconds_remaining")

Model = Union[xgb.Booster, str]  # a booster, or "naive" for the closed form


def _load_pbp(lg: str, season: int) -> pl.DataFrame:
    if lg == "wbb":
        from sportsdataverse.wbb.wbb_loaders import load_wbb_pbp as load
    else:
        from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp as load
    raw = load([season])
    cols = [c for c in (*shared.FIXTURE_COLS, *_CLOCKS) if c in raw.columns]
    pbp = (
        raw.select(list(dict.fromkeys(cols)))
        .with_columns(pl.col("game_id").cast(pl.Int64).cast(pl.Utf8))
        .sort("game_id", "game_play_number")
    )
    # a null score means "no change" (score_win_prob's rule), carried per game
    return pbp.with_columns(pl.col("home_score", "away_score").forward_fill().over("game_id").fill_null(0))


def _priors(lg: str, season: int) -> dict[str, float]:
    """The release enrichment's leakage-free weekly as-of prior per rated game_id (module docstring, 1)."""
    from sportsdataverse.mbb.mbb_team_ratings import _league_loaders, _normalize_schedule
    from sportsdataverse.mbb.mbb_win_prob import _pregame_probs

    league = LEAGUES[lg]["league"]
    load_schedule, load_box = _league_loaders(league)
    pre = _pregame_probs(_normalize_schedule(load_schedule([season])), load_box([season]), league=league)
    return {g: float(p) for g, p in zip(pre["game_id"].to_list(), pre["pregame_home_prob"].to_list())}


def _finished(tagged: pl.DataFrame) -> pl.DataFrame:
    """``game_id`` + realized ``home_win`` for games that ended untied (unfinished / tied games dropped)."""
    finals = tagged.group_by("game_id").agg(pl.col("home_score").last(), pl.col("away_score").last())
    return finals.filter(pl.col("home_score") != pl.col("away_score")).select(
        "game_id", (pl.col("home_score") > pl.col("away_score")).cast(pl.Float64).alias("home_win")
    )


def season_frame(lg: str, season: int) -> pl.DataFrame:
    """Per-play features + realized ``home_win`` for one season's rated games (ties / unfinished dropped)."""
    pbp = _load_pbp(lg, season)
    tagged = ot_adjusted_seconds(pbp, regulation_periods=game_regulation_periods(lg, season=season))
    priors = _priors(lg, season)
    tagged = tagged.filter(pl.col("game_id").is_in(list(priors)))
    rated = sorted(priors)
    rng = np.random.default_rng(season)
    hfa = rng.choice(rated, size=round(HFA_PRIOR_SHARE[lg] * len(rated)), replace=False).tolist()
    priors |= dict.fromkeys(hfa, _hfa_prior(lg))
    out = []
    for (gid,), g in tagged.join(_finished(tagged), on="game_id").group_by("game_id", maintain_order=True):
        f = in_game_features(g, priors[gid])
        out.append(f.select(_IN_GAME_FEATURES).with_columns(g["home_win"], pl.lit(season).alias("season")))
    return pl.concat(out)


def naive_win_prob(lg: str, tagged: pl.DataFrame, prior: float) -> pl.Series:
    """The naive closed form (module docstring): a normal final margin around lead + the prior's margin share."""
    sd = get_constants(LEAGUES[lg]["league"]).margin_sd
    mu = sd * float(norm.ppf(min(max(prior, 1e-6), 1 - 1e-6)))
    t = tagged["start_game_seconds_remaining"].cast(pl.Float64).clip(lower_bound=1.0).to_numpy() / REGULATION_SECONDS
    lead = (tagged["home_score"] - tagged["away_score"]).cast(pl.Float64).to_numpy()
    return pl.Series("home_win_prob", norm.cdf((lead + mu * t) / (sd * np.sqrt(t))), dtype=pl.Float64)


def serve(lg: str, pbp: pl.DataFrame, model: Model, priors: Optional[dict[str, float]] = None) -> pl.DataFrame:
    """Every game of ``pbp`` through ``score_win_prob`` with ``model`` swapped in (the serving path).

    ``priors`` (game_id -> pregame home prob) replaces the game page's ``home_prior`` (HFA, the
    college releases carry no lines) with the release enrichment's prior; a game absent from it
    takes the enrichment's HFA fallback.
    """
    orig_art, orig_wp, orig_prior = (
        mbb_game_predict._load_wp_artifact,
        nba_game_page._in_game_win_prob,
        nba_game_page.home_prior,
    )
    if isinstance(model, xgb.Booster):
        art = {"kind": "xgboost", "booster": model, "features": model.feature_names}
        mbb_game_predict._load_wp_artifact = lambda league: art  # type: ignore[assignment]
    elif model == "naive":
        nba_game_page._in_game_win_prob = naive_win_prob  # type: ignore[assignment]
    else:
        raise ValueError(f"unknown model {model!r}")
    fallback = _hfa_prior(lg)
    out = []
    try:
        for (gid,), g in pbp.group_by("game_id", maintain_order=True):
            if priors is not None:
                p0 = priors.get(gid, fallback)
                nba_game_page.home_prior = lambda *a, _p=p0, **k: (_p, "ratings")  # type: ignore[assignment]
            out.append(score_win_prob(g, league=lg))
    finally:
        mbb_game_predict._load_wp_artifact, nba_game_page._in_game_win_prob = orig_art, orig_wp
        nba_game_page.home_prior = orig_prior
    return pl.concat(out, how="diagonal_relaxed")


def _hfa_prior(lg: str) -> float:
    """The HFA-only pregame home prob: the game page's lineless prior and the enrichment's fallback."""
    league = LEAGUES[lg]["league"]
    return win_prob_from_margin(predict_margin(0.0, 0.0, neutral=False, league=league), league=league)


def _scored_rows(scored: pl.DataFrame, reg: int) -> pl.DataFrame:
    """Model rows of finished, untied games with label ``y`` (the settled horn rows dropped)."""
    finals = scored.group_by("game_id").agg(
        (pl.col("home_score").last() > pl.col("away_score").last()).cast(pl.Float64).alias("y"),
        (pl.col("home_score").last() == pl.col("away_score").last()).alias("tie"),
    )
    settle = (
        pl.col("type_text").is_in(["End Game", "End Period"])
        & (pl.col("period_number") >= reg)
        & (pl.col("home_score") != pl.col("away_score"))
    ).fill_null(False)
    return scored.join(finals.filter(~pl.col("tie")), on="game_id").filter(~settle)


def probability_metrics(y: np.ndarray, p: np.ndarray) -> dict[str, float]:
    """Brier, log loss, Cox calibration intercept / slope and the worst decile gap of ``p`` for labels ``y``."""
    a, b = shared.calibration_line(y, p)
    deciles = calibration_table(y, p, n_bins=10)
    pc = np.clip(p, 1e-9, 1 - 1e-9)
    return {
        "plays": int(len(y)),
        "brier": float(np.mean((p - y) ** 2)),
        "log_loss": float(-np.mean(y * np.log(pc) + (1 - y) * np.log(1 - pc))),
        "calibration_intercept": a,
        "calibration_slope": b,
        "max_decile_gap": float((deciles["mean_pred"] - deciles["mean_actual"]).abs().max()),
    }


def holdout_metrics(
    lg: str, pbp: pl.DataFrame, model: Model, priors: Optional[dict[str, float]] = None
) -> dict[str, Any]:
    season = int(pbp["season"].drop_nulls()[0])
    reg = game_regulation_periods(lg, season=season)
    scored = serve(lg, pbp, model, priors)
    rows = _scored_rows(scored, reg)
    y, p = rows["y"].to_numpy(), rows["home_win_prob"].to_numpy()
    period = pl.when(pl.col("period_number") > reg).then(pl.lit("ot")).otherwise(pl.col("period_number").cast(pl.Utf8))
    by_period = {
        k: float(v)
        for k, v in rows.group_by(period.alias("k"))
        .agg(((pl.col("home_win_prob") - pl.col("y")) ** 2).mean().alias("b"))
        .sort("k")
        .iter_rows()
    }
    out: dict[str, Any] = probability_metrics(y, p) | {"brier_by_period": by_period}
    return out | shared.late_game_metrics(scored, reg)


def _baseline(artifact: str, ref: str) -> xgb.Booster:
    raw = subprocess.run(
        ["git", "show", f"{ref}:sportsdataverse/mbb/models/{artifact}"], cwd=REPO, capture_output=True, check=True
    ).stdout
    b = xgb.Booster()
    b.load_model(bytearray(raw))
    return b


def _grid(model: Model) -> Union[int, None]:
    return shared.grid_monotone_violations(model) if isinstance(model, xgb.Booster) else None


def _score_season(lg: str, season: int, models: dict[str, Model]) -> dict[str, Any]:
    """Each model on one season, with the game page's HFA prior (``hfa_prior``) and the rated prior (``rated_prior``)."""
    pbp = _load_pbp(lg, season)
    priors = _priors(lg, season)
    return {
        prior: {name: holdout_metrics(lg, pbp, m, pm) | {"grid_violations": _grid(m)} for name, m in models.items()}
        for prior, pm in (("hfa_prior", None), ("rated_prior", priors))
    }


def train(
    lg: str, train_seasons: list[int], holdout: int, baseline_ref: str, log: Callable[[str], None] = print
) -> dict[str, Any]:
    cfg = LEAGUES[lg]
    if holdout in train_seasons or CONFIRMATION_SEASON in train_seasons:
        raise ValueError("train seasons must exclude the holdout and the confirmation season")
    if len(train_seasons) < 2:
        raise ValueError(f"need at least two training seasons (fit + early-stopping validation), got {train_seasons}")
    frames = {}
    for s in train_seasons:
        frames[s] = season_frame(lg, s)
        log(f"{lg} {s}: {frames[s].height} training plays")
    *fit_s, val_s = sorted(train_seasons)
    rounds = shared._early_stop_rounds(pl.concat([frames[s] for s in fit_s]), frames[val_s])
    booster = xgb.train(shared._XGB_PARAMS, shared._dmat(pl.concat(frames.values())), num_boost_round=rounds)
    booster.set_attr(
        train_seasons=",".join(map(str, train_seasons)),
        holdout_season=str(holdout),
        fitting_script="tools/models/mbb_prediction/train_in_game_wp.py",
    )
    log(f"{lg}: {rounds} rounds")

    models: dict[str, Model] = {
        "refit": booster,
        "baseline": _baseline(cfg["artifact"], baseline_ref),
        "naive": "naive",
    }
    report = {
        "league": lg,
        "train_seasons": train_seasons,
        "validation_season": val_s,
        "holdout_season": holdout,
        "confirmation_season": CONFIRMATION_SEASON,
        "baseline_ref": baseline_ref,
        "n_train_plays": int(sum(f.height for f in frames.values())),
        "rounds": rounds,
        "params": shared._XGB_PARAMS,
        "features": _IN_GAME_FEATURES,
        "hfa_prior_share": HFA_PRIOR_SHARE[lg],
        "holdout": _score_season(lg, holdout, models),
        "confirmation": _score_season(lg, CONFIRMATION_SEASON, models),
    }
    booster.save_model(str(MODELS_DIR / cfg["artifact"]))
    (HERE / f"in_game_wp_{lg}_holdout.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    hold = _load_pbp(lg, holdout)
    reg = game_regulation_periods(lg, season=holdout)
    late = hold.filter(
        (pl.col("period_number") > reg)
        | ((pl.col("period_number") == reg) & (pl.col("start_game_seconds_remaining") <= shared.LATE_SECONDS))
    )
    late.write_parquet(REPO / "tests" / "fixtures" / f"{lg}_prediction" / f"{lg}_late_game_{holdout}.parquet")
    log(json.dumps({k: report[k] for k in ("rounds", "n_train_plays", "holdout", "confirmation")}, indent=2))
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", default="mbb", choices=sorted(LEAGUES))
    ap.add_argument("--train-seasons", type=int, nargs="+")
    ap.add_argument("--holdout-season", type=int)
    ap.add_argument("--baseline-ref", default=BASELINE_REF)
    args = ap.parse_args()
    cfg = LEAGUES[args.league]
    train(args.league, args.train_seasons or cfg["train"], args.holdout_season or cfg["holdout"], args.baseline_ref)
