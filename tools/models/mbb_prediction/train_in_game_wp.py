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
   (``mbb_win_prob._pregame_probs``: games rated on games strictly before the Monday of their week).
   College lines are absent from the releases (MBB 2024-2026 and every WBB season carry no
   ``game_spread_available``), so the game page serves ``home_prior``'s HFA-only prior (0.5 at a
   neutral site). Games with no as-of rating (about 10% of games, the season's first days) get the
   enrichment's HFA fallback, and they are mostly buy games the home side wins (MBB 2022 / 2023 / 2025:
   0.90 / 0.90 / 0.88 home wins against a 0.60 fallback; 0.62-0.65 in rated games). Trained on as
   they are, the trees learn "the HFA logit means a likely home win" and overpredict every game the
   game page serves with that same prior.
2. Clock. Halves for MBB, quarters for WBB from 2016 (``game_regulation_periods``); overtime plays
   carry the period clock (``ot_adjusted_seconds``), as at serve time.
3. Two serving priors. A seeded random share of the rated training games (``HFA_PRIOR_SHARE``) is fit
   with the game page's prior instead: uninformative but, unlike the unrated games, not a biased
   subset. ``KEEP_UNRATED`` says whether unrated games are trained on. Both are picked per league by
   ``--validate`` (``in_game_wp_<lg>_validation.json``): fit on 2022 + 2023, score every finished
   2025 game (the holdout and confirmation seasons are not looked at) with each prior, and take the
   lowest mean of the two log losses over share {0, 0.25, 0.5} x keep {no, yes}; cells whose paired
   game-clustered 95% interval against that best includes 0 are ties, and the simplest tie wins
   (unrated dropped, then the smallest share). MBB: share 0, unrated dropped (the best, 0.38793; every
   other cell is worse, keeping unrated games at share 0 moves the HFA-prior calibration intercept
   +0.021 -> -0.187). WBB: share 0.25, unrated dropped (0.34819, a tie with the best, 0.34795 with
   unrated kept, whose HFA-prior intercept is -0.100 against +0.029).

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
import hashlib
import json
import math
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
from sportsdataverse.mbb.mbb_game_predict import in_game_features  # noqa: E402
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
# The 2025 validation grid's pick (``--validate``, in_game_wp_<lg>_validation.json; module docstring,
# 1 and 3): the share of rated training games fit with the game page's HFA-only prior, and whether
# games with no as-of rating are trained on.
HFA_PRIOR_SHARE = {"mbb": 0.0, "wbb": 0.25}
KEEP_UNRATED = {"mbb": False, "wbb": False}
CONFIRMATION_SEASON = 2024
# The last commit before this refit: the baseline the holdout gate compares against.
BASELINE_REF = "eb4207476e"
REGULATION_SECONDS = 2400.0  # 40 minutes, both college leagues
_CLOCKS = ("start_quarter_seconds_remaining", "start_period_seconds_remaining")

Model = Union[xgb.Booster, str]  # a booster, or "naive" for the closed form
# The validation grid (module docstring, 1 and 3): HFA-prior share x whether unrated games are kept.
GRID_SHARES = (0.0, 0.25, 0.5)
BOOTSTRAP_DRAWS = 2000


def _schedule(lg: str, season: int) -> pl.DataFrame:
    from sportsdataverse.mbb.mbb_team_ratings import (
        _league_loaders,
        _normalize_schedule,
    )

    return _normalize_schedule(_league_loaders(LEAGUES[lg]["league"])[0]([season]))


def _load_pbp(lg: str, season: int) -> pl.DataFrame:
    """One season's pbp, sorted, scores forward-filled per game, with the schedule's ``neutral_site``."""
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
    neutral = (
        _schedule(lg, season)
        .select(
            pl.col("game_id").cast(pl.Int64).cast(pl.Utf8),
            pl.col("neutral_site").cast(pl.Boolean),
        )
        .unique("game_id")
    )
    # a null score means "no change" (score_win_prob's rule), carried per game
    return (
        pbp.with_columns(pl.col("home_score", "away_score").forward_fill().over("game_id").fill_null(0))
        .join(neutral, on="game_id", how="left")
        .with_columns(pl.col("neutral_site").fill_null(False))
    )


def _priors(lg: str, season: int) -> dict[str, float]:
    """The release enrichment's leakage-free weekly as-of prior per rated game_id (module docstring, 1)."""
    from sportsdataverse.mbb.mbb_team_ratings import _league_loaders
    from sportsdataverse.mbb.mbb_win_prob import _pregame_probs

    league = LEAGUES[lg]["league"]
    pre = _pregame_probs(_schedule(lg, season), _league_loaders(league)[1]([season]), league=league)
    return {g: float(p) for g, p in zip(pre["game_id"].to_list(), pre["pregame_home_prob"].to_list())}


def _hfa_prior(lg: str, neutral: bool = False) -> float:
    """The game page's lineless prior: HFA only, 0.5 at a neutral site (``home_prior``)."""
    return nba_game_page.home_prior(None, None, league=lg, neutral_site=neutral)[0]


def _finished(tagged: pl.DataFrame) -> pl.DataFrame:
    """``game_id`` + realized ``home_win`` for games that ended untied (unfinished / tied games dropped)."""
    finals = tagged.group_by("game_id").agg(pl.col("home_score").last(), pl.col("away_score").last())
    return finals.filter(pl.col("home_score") != pl.col("away_score")).select(
        "game_id",
        (pl.col("home_score") > pl.col("away_score")).cast(pl.Float64).alias("home_win"),
    )


def _logit(p: float) -> float:
    p = min(max(p, 1e-6), 1 - 1e-6)
    return math.log(p / (1 - p))


def season_raw(lg: str, season: int) -> tuple[pl.DataFrame, dict[str, int]]:
    """Every finished game's per-play features with its rated prior (the game page's HFA prior if unrated).

    Also carries ``game_id``, ``rated``, ``hfa_logit`` (the game page's prior for the game, neutral
    site aware) and the label, so a training set or a validation score is a filter / prior swap of
    this frame. The stats count the rows the overtime clock transform changed and the rated games.
    """
    pbp = _load_pbp(lg, season)
    tagged = ot_adjusted_seconds(pbp, regulation_periods=game_regulation_periods(lg, season=season))
    changed = pbp["start_game_seconds_remaining"].cast(pl.Float64) != tagged["start_game_seconds_remaining"]
    priors = _priors(lg, season)
    out = []
    for (gid,), g in tagged.join(_finished(tagged), on="game_id").group_by("game_id", maintain_order=True):
        hfa = _hfa_prior(lg, bool(g["neutral_site"][0]))
        f = in_game_features(g, priors.get(gid, hfa)).select(_IN_GAME_FEATURES)
        out.append(
            f.with_columns(
                g["home_win"],
                pl.lit(season).alias("season"),
                pl.lit(gid).alias("game_id"),
                pl.lit(gid in priors).alias("rated"),
                pl.lit(_logit(hfa)).alias("hfa_logit"),
            )
        )
    raw = pl.concat(out)
    games = raw.group_by("game_id").agg(pl.col("rated").first())
    stats = {
        "games": games.height,
        "rated_games": int(games["rated"].sum()),
        "ot_rows_reclocked": int(changed.fill_null(False).sum()),
    }
    return raw, stats


def training_frame(
    raw: pl.DataFrame, share: float, keep_unrated: bool, seed: int
) -> tuple[pl.DataFrame, dict[str, int]]:
    """A season's training rows: unrated games dropped (unless kept), ``share`` of rated games on the HFA prior."""
    df = raw if keep_unrated else raw.filter(pl.col("rated"))
    rated = sorted(df.filter(pl.col("rated"))["game_id"].unique().to_list())
    swap = np.random.default_rng(seed).choice(rated, size=round(share * len(rated)), replace=False).tolist()
    df = df.with_columns(
        pl.when(pl.col("game_id").is_in(swap))
        .then(pl.col("hfa_logit"))
        .otherwise(pl.col("pregame_logit"))
        .alias("pregame_logit")
    )
    dropped = 0 if keep_unrated else raw.filter(~pl.col("rated"))["game_id"].n_unique()
    return df, {
        "unrated_games_dropped": dropped,
        "hfa_swapped_games": len(swap),
        "rows": df.height,
    }


def _fit(frames: dict[int, pl.DataFrame]) -> tuple[xgb.Booster, int]:
    """Early-stop on the last season (fit on the rest), then refit on every season with that many rounds."""
    *fit_s, val_s = sorted(frames)
    rounds = shared._early_stop_rounds(pl.concat([frames[s] for s in fit_s]), frames[val_s])
    return xgb.train(
        shared._XGB_PARAMS,
        shared._dmat(pl.concat(frames.values())),
        num_boost_round=rounds,
    ), rounds


def _design_key(cell: dict[str, Any]) -> tuple[bool, float]:
    """Simplest design first: unrated games dropped (no biased fallback prior), then the smallest HFA share."""
    return (cell["keep_unrated"], cell["hfa_prior_share"])


def validate(lg: str, log: Callable[[str], None] = print) -> dict[str, Any]:
    """The design grid on the 2025 validation season: fit 2022 + 2023, score every finished 2025 game.

    Scored on features (not through ``score_win_prob``) with the game page's prior (``hfa_logit``) and
    the rated prior; a cell's loss is the mean of the two log losses. 2025 is also each cell's
    early-stopping season, an optimism every cell shares. The best cell's paired game-clustered
    bootstrap interval against each other cell decides ties: of the cells whose interval includes 0,
    the pick is the simplest (:func:`_design_key`). ``HFA_PRIOR_SHARE`` / ``KEEP_UNRATED`` must equal
    the pick (tests/mbb/test_mbb_in_game_wp_late_game.py).
    """
    raws = {s: season_raw(lg, s)[0] for s in (2022, 2023, 2025)}
    served = {
        "hfa_prior": raws[2025].with_columns(pl.col("hfa_logit").alias("pregame_logit")),
        "rated_prior": raws[2025],
    }
    grid, sums = [], []
    for keep in (False, True):
        for share in GRID_SHARES:
            frames = {s: training_frame(raws[s], share, keep, s)[0] for s in raws}
            tr = pl.concat([frames[2022], frames[2023]])
            rounds = shared._early_stop_rounds(tr, frames[2025])
            booster = xgb.train(shared._XGB_PARAMS, shared._dmat(tr), num_boost_round=rounds)
            cell: dict[str, Any] = {"hfa_prior_share": share, "keep_unrated": keep, "rounds": rounds}
            per_game = []
            for prior, df in served.items():
                p = booster.inplace_predict(df.select(_IN_GAME_FEATURES).to_numpy()).astype(np.float64)
                cell[prior] = probability_metrics(df["home_win"].to_numpy(), p)
                rows = df.select("game_id", pl.col("home_win").alias("y")).with_columns(pl.Series("home_win_prob", p))
                per_game.append(_game_sums(rows))
            # per game, the mean of the two priors' summed losses (the cell's loss, game by game)
            sums.append(
                per_game[0]
                .join(per_game[1], on="game_id", suffix="_r")
                .select(
                    "game_id",
                    "n",
                    ((pl.col("se") + pl.col("se_r")) / 2).alias("se"),
                    ((pl.col("ll") + pl.col("ll_r")) / 2).alias("ll"),
                )
            )
            cell["mean_log_loss"] = (cell["hfa_prior"]["log_loss"] + cell["rated_prior"]["log_loss"]) / 2
            log(f"{lg} validate keep_unrated={keep} share={share}: mean log loss {cell['mean_log_loss']:.5f}")
            grid.append(cell)
    if not all(math.isfinite(c["mean_log_loss"]) for c in grid):
        raise ValueError(f"non-finite validation log loss: {[c['mean_log_loss'] for c in grid]}")
    b = min(range(len(grid)), key=lambda k: grid[k]["mean_log_loss"])
    for k, cell in enumerate(grid):
        cell["best_minus_this"] = paired_deltas(sums[b], sums[k])["log_loss"]
        cell["tied_with_best"] = k == b or cell["best_minus_this"]["ci95"][1] >= 0
    pick = min((c for c in grid if c["tied_with_best"]), key=_design_key)
    report = {
        "league": lg,
        "fit_seasons": [2022, 2023],
        "validation_season": 2025,
        "rule": (
            "lowest mean of the game-page-prior and rated-prior log losses; cells whose paired game-clustered 95% "
            "interval against it includes 0 are ties, and the simplest tie is picked (unrated dropped, then the "
            "smallest HFA share)"
        ),
        "best": {"hfa_prior_share": grid[b]["hfa_prior_share"], "keep_unrated": grid[b]["keep_unrated"]},
        "pick": {"hfa_prior_share": pick["hfa_prior_share"], "keep_unrated": pick["keep_unrated"]},
        "grid": grid,
    }
    (HERE / f"in_game_wp_{lg}_validation.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def naive_win_prob(lg: str, tagged: pl.DataFrame, prior: float) -> pl.Series:
    """The naive closed form (module docstring): a normal final margin around lead + the prior's margin share."""
    sd = get_constants(LEAGUES[lg]["league"]).margin_sd
    mu = sd * float(norm.ppf(min(max(prior, 1e-6), 1 - 1e-6)))
    t = tagged["start_game_seconds_remaining"].cast(pl.Float64).clip(lower_bound=1.0).to_numpy() / REGULATION_SECONDS
    lead = (tagged["home_score"] - tagged["away_score"]).cast(pl.Float64).to_numpy()
    return pl.Series("home_win_prob", norm.cdf((lead + mu * t) / (sd * np.sqrt(t))), dtype=pl.Float64)


def serve(lg: str, pbp: pl.DataFrame, model: Model, priors: Optional[dict[str, float]] = None) -> pl.DataFrame:
    """Every game of ``pbp`` through ``score_win_prob`` with ``model`` swapped in (the serving path).

    Each game passes its ``neutral_site`` (a frame without the column is not neutral), as the game
    page does. ``priors`` (game_id -> pregame home prob) replaces the game page's ``home_prior`` (HFA,
    the college releases carry no lines) with the release enrichment's prior; a game absent from it
    takes the enrichment's flat HFA fallback.
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
    fallback = _hfa_prior(lg)  # mbb_win_prob._compile_season_wp's flat fallback
    out = []
    try:
        for (gid,), g in pbp.group_by("game_id", maintain_order=True):
            neutral = bool(g["neutral_site"][0]) if "neutral_site" in g.columns else False
            if priors is not None:
                p0 = priors.get(gid, fallback)
                nba_game_page.home_prior = lambda *a, _p=p0, **k: (_p, "ratings")  # type: ignore[assignment]
            out.append(score_win_prob(g, league=lg, neutral_site=neutral))
    finally:
        mbb_game_predict._load_wp_artifact, nba_game_page._in_game_win_prob = (
            orig_art,
            orig_wp,
        )
        nba_game_page.home_prior = orig_prior
    return pl.concat(out, how="diagonal_relaxed")


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
    # float64 first: xgboost predicts float32, where 1 - 1e-9 rounds to 1.0 and the log loss turns NaN
    y, p = np.asarray(y, dtype=np.float64), np.asarray(p, dtype=np.float64)
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


def _game_sums(rows: pl.DataFrame) -> pl.DataFrame:
    """Per game: plays, summed squared error and summed log loss (the bootstrap's resampling unit)."""
    pc = pl.col("home_win_prob").clip(1e-9, 1 - 1e-9)
    return (
        rows.group_by("game_id")
        .agg(
            pl.len().alias("n"),
            ((pl.col("home_win_prob") - pl.col("y")) ** 2).sum().alias("se"),
            (-(pl.col("y") * pc.log() + (1 - pl.col("y")) * (1 - pc).log())).sum().alias("ll"),
        )
        .sort("game_id")
    )


def _score(
    lg: str, pbp: pl.DataFrame, model: Model, priors: Optional[dict[str, float]] = None
) -> tuple[dict[str, Any], pl.DataFrame]:
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
    metrics: dict[str, Any] = probability_metrics(y, p) | {
        "brier_by_period": by_period,
        "games_scored": rows["game_id"].n_unique(),
    }
    return metrics | shared.late_game_metrics(scored, reg), _game_sums(rows)


def holdout_metrics(
    lg: str, pbp: pl.DataFrame, model: Model, priors: Optional[dict[str, float]] = None
) -> dict[str, Any]:
    """One model on one season through the serving path (see :func:`serve` for ``priors``)."""
    return _score(lg, pbp, model, priors)[0]


def paired_deltas(a: pl.DataFrame, b: pl.DataFrame, seed: int = 0) -> dict[str, dict[str, Any]]:
    """``a`` minus ``b`` Brier and log loss on the same plays, with a 95% game-clustered bootstrap interval."""
    j = a.join(b, on="game_id", suffix="_b")
    if not (j.height == a.height == b.height and (j["n"] == j["n_b"]).all()):
        raise ValueError("both models must score the same games and plays")
    n = j["n"].to_numpy().astype(float)
    w = np.random.default_rng(seed).multinomial(j.height, np.full(j.height, 1 / j.height), size=BOOTSTRAP_DRAWS)
    out = {}
    for metric, col in (("brier", "se"), ("log_loss", "ll")):
        d = j[col].to_numpy() - j[f"{col}_b"].to_numpy()
        lo, hi = np.percentile((w @ d) / (w @ n), [2.5, 97.5])
        out[metric] = {
            "delta": float(d.sum() / n.sum()),
            "ci95": [float(lo), float(hi)],
        }
    return out


def _grid(model: Model) -> Union[int, None]:
    return shared.grid_monotone_violations(model) if isinstance(model, xgb.Booster) else None


def _baseline(artifact: str, ref: str) -> xgb.Booster:
    raw = subprocess.run(
        ["git", "show", f"{ref}:sportsdataverse/mbb/models/{artifact}"],
        cwd=REPO,
        capture_output=True,
        check=True,
    ).stdout
    b = xgb.Booster()
    b.load_model(bytearray(raw))
    return b


def _score_season(lg: str, season: int, models: dict[str, Model]) -> dict[str, Any]:
    """Each model on one season with the game page's prior (``hfa_prior``) and the rated prior (``rated_prior``).

    ``refit_vs_<model>`` holds the paired game-clustered bootstrap deltas (negative: the refit is better).
    """
    pbp = _load_pbp(lg, season)
    priors = _priors(lg, season)
    out: dict[str, Any] = {}
    for prior, pm in (("hfa_prior", None), ("rated_prior", priors)):
        cell: dict[str, Any] = {}
        sums = {}
        for name, m in models.items():
            metrics, sums[name] = _score(lg, pbp, m, pm)
            cell[name] = metrics | {"grid_violations": _grid(m)}
        for other in models:
            if other != "refit":
                cell[f"refit_vs_{other}"] = paired_deltas(sums["refit"], sums[other])
        out[prior] = cell
    return out


def train(
    lg: str,
    train_seasons: list[int],
    holdout: int,
    baseline_ref: str,
    log: Callable[[str], None] = print,
) -> dict[str, Any]:
    cfg = LEAGUES[lg]
    if holdout in train_seasons or CONFIRMATION_SEASON in train_seasons:
        raise ValueError("train seasons must exclude the holdout and the confirmation season")
    if len(train_seasons) < 2:
        raise ValueError(f"need at least two training seasons (fit + early-stopping validation), got {train_seasons}")
    frames, stats = {}, {}
    for s in train_seasons:
        raw, raw_stats = season_raw(lg, s)
        frames[s], tf_stats = training_frame(raw, HFA_PRIOR_SHARE[lg], KEEP_UNRATED[lg], s)
        stats[str(s)] = raw_stats | tf_stats
        log(f"{lg} {s}: {stats[str(s)]}")
    booster, rounds = _fit(frames)
    booster.set_attr(
        train_seasons=",".join(map(str, train_seasons)),
        holdout_season=str(holdout),
        fitting_script="tools/models/mbb_prediction/train_in_game_wp.py",
    )
    log(f"{lg}: {rounds} rounds")
    artifact = MODELS_DIR / cfg["artifact"]
    booster.save_model(str(artifact))

    models: dict[str, Model] = {
        "refit": booster,
        "baseline": _baseline(cfg["artifact"], baseline_ref),
        "naive": "naive",
    }
    report = {
        "league": lg,
        "train_seasons": train_seasons,
        "validation_season": max(train_seasons),
        "holdout_season": holdout,
        "confirmation_season": CONFIRMATION_SEASON,
        "confirmation_note": (
            "2024 is in no training season, but the pregame constants (hfa, margin_sd, em_scale) that the rated "
            "prior and the naive model read were fit on it, and it precedes the 2025 training season"
        ),
        "baseline_ref": baseline_ref,
        "artifact_sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
        "rounds": rounds,
        "n_train_plays": int(sum(f.height for f in frames.values())),
        "train_stats": stats,
        "params": shared._XGB_PARAMS,
        "features": _IN_GAME_FEATURES,
        "hfa_prior_share": HFA_PRIOR_SHARE[lg],
        "keep_unrated": KEEP_UNRATED[lg],
        "holdout": _score_season(lg, holdout, models),
        "confirmation": _score_season(lg, CONFIRMATION_SEASON, models),
    }
    (HERE / f"in_game_wp_{lg}_holdout.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    hold = _load_pbp(lg, holdout)
    reg = game_regulation_periods(lg, season=holdout)
    late = hold.filter(
        (pl.col("period_number") > reg)
        | ((pl.col("period_number") == reg) & (pl.col("start_game_seconds_remaining") <= shared.LATE_SECONDS))
    )
    late.write_parquet(REPO / "tests" / "fixtures" / f"{lg}_prediction" / f"{lg}_late_game_{holdout}.parquet")
    log(json.dumps({k: report[k] for k in ("rounds", "n_train_plays", "holdout")}, indent=2))
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", default="mbb", choices=sorted(LEAGUES))
    ap.add_argument("--train-seasons", type=int, nargs="+")
    ap.add_argument("--holdout-season", type=int)
    ap.add_argument("--baseline-ref", default=BASELINE_REF)
    ap.add_argument(
        "--validate",
        action="store_true",
        help="run the 2025 validation grid instead of the fit",
    )
    args = ap.parse_args()
    cfg = LEAGUES[args.league]
    if args.validate:
        validate(args.league)
    else:
        train(
            args.league,
            args.train_seasons or cfg["train"],
            args.holdout_season or cfg["holdout"],
            args.baseline_ref,
        )
