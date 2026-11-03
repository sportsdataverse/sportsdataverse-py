"""Fit, gate and bundle the NBA / WNBA in-game win-probability booster (model 4).

Run (any IP: the ESPN pbp releases, no stats.nba.com)::

    uv run python tools/models/nba_prediction/train_in_game_wp.py --league-id 00
    uv run python tools/models/nba_prediction/train_in_game_wp.py --league-id 10

Defaults (train and holdout seasons disjoint):

* NBA  ``00``: train 2022, 2023, 2025; holdout 2026. 2024 is left out so the ``pbp_sample_2024``
  calibration gate (tests/nba/test_nba_in_game_wp.py) stays out-of-sample.
* WNBA ``10``: train 2021-2024; holdout 2025. 2024 is IN training: without it the 2025 holdout
  calibration slope was 0.79-0.85 (worse than the baseline's 1.12 by distance from 1); with it
  0.92. So the ``wnba_pbp_sample_2024`` Brier gate (tests/wnba/test_wnba_win_prob.py) is in-sample
  for this booster; the 2025 holdout gate is the out-of-sample one.

What changed from the 2026-07 single-season fit (the late-game flatness, BB-D28 finding 1):

1. Prior. The old fit encoded each game's FULL-SEASON-ratings pregame probability, a per-game
   continuous constant fit on the same results it labels. The trees split on it ~2x as often as
   on the margin (game identification plus in-sample outcome leakage) and spent their depth there,
   leaving no time split under 61 s (NBA) / 127 s (WNBA). The fit now uses a leakage-free prior:
   NBA the pregame spread through :func:`home_prior` (what :func:`score_win_prob` serves),
   WNBA (no lines in the release) the weekly as-of ratings prior the WNBA release enrichment
   serves (``wnba_win_prob._pregame_probs`` with the same HFA fallback).
2. Clock. Plays are tagged with :func:`ot_adjusted_seconds` before featurising, the same
   transform ``score_win_prob`` applies at serve time (overtime plays carry the period clock).
3. Feature. ``margin_per_sqrt_sec`` = ``score_diff / sqrt(sec_left + 1)`` (the Brownian-motion
   lead-over-remaining-volatility term) lets a tree express "a fixed lead is worth more as time
   runs out" without an axis split per second.
4. Monotone constraints: home WP non-decreasing in ``score_diff``, ``pregame_logit``,
   ``home_has_ball`` and ``margin_per_sqrt_sec``; the clock ``sqrt_sec_left_60`` (floored at one
   minute) free. Inside the final minute every input but ``margin_per_sqrt_sec`` is fixed at a given
   lead, prior and possession, so the WP can only move toward the leader as the clock runs down.
5. Several seasons pooled; boosting rounds picked by early stopping on the last training season
   (a validation season, never the holdout), then refit on every training season.

The holdout is scored through the serving path (``score_win_prob`` with the booster swapped in),
for the new booster and for the baseline booster read from git (``--baseline-ref``). Metrics go to
``in_game_wp_<lg>_holdout.json`` beside this script; the late-game rows of the holdout go to the
offline test fixture ``tests/fixtures/nba_prediction/<lg>_late_game_<season>.parquet``.
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import xgboost as xgb

from sportsdataverse.nba import nba_game_predict
from sportsdataverse.nba.nba_game_page import game_regulation_periods, home_prior, ot_adjusted_seconds, score_win_prob
from sportsdataverse.nba.nba_game_predict import _IN_GAME_FEATURES, in_game_features

REPO = Path(__file__).resolve().parents[3]
MODELS_DIR = REPO / "sportsdataverse" / "nba" / "models"
FIXTURE_DIR = REPO / "tests" / "fixtures" / "nba_prediction"
HERE = Path(__file__).resolve().parent

LEAGUES: dict[str, dict[str, Any]] = {
    "00": {"lg": "nba", "artifact": "nba_in_game_wp.ubj", "train": [2022, 2023, 2025], "holdout": 2026},
    "10": {"lg": "wnba", "artifact": "wnba_in_game_wp.ubj", "train": [2021, 2022, 2023, 2024], "holdout": 2025},
}
# The commit before this refit: the baseline the holdout gate compares against.
BASELINE_REF = "3e967eb40d"

# monotone constraint per _IN_GAME_FEATURES entry (+1 = home WP non-decreasing in the feature)
_MONOTONE = {"score_diff": 1, "sqrt_sec_left_60": 0, "pregame_logit": 1, "home_has_ball": 1, "margin_per_sqrt_sec": 1}
_XGB_PARAMS: dict[str, Any] = {
    "max_depth": 4,
    "eta": 0.1,
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "subsample": 0.8,
    "tree_method": "hist",
    "seed": 42,
    "monotone_constraints": "(" + ",".join(str(_MONOTONE[f]) for f in _IN_GAME_FEATURES) + ")",
}
_MAX_ROUNDS = 1000
LATE_SECONDS = 120.0  # fixture window: the last two minutes of regulation, plus every overtime play
FIXTURE_COLS = [
    "game_id",
    "season",
    "game_play_number",
    "period_number",
    "start_game_seconds_remaining",
    "start_quarter_seconds_remaining",
    "home_score",
    "away_score",
    "team_id",
    "home_team_id",
    "home_team_spread",
    "game_spread_available",
    "type_text",
]


def _load_pbp(league_id: str, season: int) -> pl.DataFrame:
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_pbp as load
    else:
        from sportsdataverse.nba.nba_loaders import load_nba_pbp as load
    pbp = load([season]).sort("game_id", "game_play_number")
    # a null score means "no change" (score_win_prob's rule), carried per game
    return pbp.with_columns(pl.col("home_score", "away_score").forward_fill().over("game_id").fill_null(0))


def _priors(league_id: str, season: int, pbp: pl.DataFrame) -> dict[Any, float]:
    """Leakage-free pregame home prob per game_id (see module docstring, change 1)."""
    if league_id == "10":
        from sportsdataverse.nba.nba_team_ratings import _normalize_schedule
        from sportsdataverse.wnba import wnba_win_prob as wwp
        from sportsdataverse.wnba.wnba_loaders import load_wnba_schedule, load_wnba_team_boxscore

        pre = wwp._pregame_probs(_normalize_schedule(load_wnba_schedule([season])), load_wnba_team_boxscore([season]))
        pmap = dict(zip(pre["game_id"].to_list(), pre["pregame_home_prob"].to_list()))
        fallback = wwp.wnba_win_prob_from_margin(
            wwp.wnba_predict_margin(0.0, 0.0, home_pace=1.0, away_pace=1.0, neutral=False)
        )
        return {g: float(pmap.get(str(g), fallback)) for g in pbp["game_id"].unique().to_list()}
    first = pbp.group_by("game_id").agg(pl.col("home_team_spread", "game_spread_available").first())
    return {
        r["game_id"]: home_prior(r["home_team_spread"], r["game_spread_available"], league="nba")[0]
        for r in first.iter_rows(named=True)
    }


def season_frame(league_id: str, season: int) -> pl.DataFrame:
    """Per-play features + the game's realized ``home_win`` for one season (ties / unfinished dropped)."""
    lg = LEAGUES[league_id]["lg"]
    pbp = _load_pbp(league_id, season)
    tagged = ot_adjusted_seconds(pbp, regulation_periods=game_regulation_periods(lg, season=season))
    finals = tagged.group_by("game_id").agg(pl.col("home_score").last(), pl.col("away_score").last())
    finals = finals.filter(pl.col("home_score") != pl.col("away_score")).select(
        "game_id", (pl.col("home_score") > pl.col("away_score")).cast(pl.Float64).alias("home_win")
    )
    priors = _priors(league_id, season, pbp)
    out = []
    for (gid,), g in tagged.join(finals, on="game_id").group_by("game_id", maintain_order=True):
        f = in_game_features(g, priors[gid])
        out.append(f.select(_IN_GAME_FEATURES).with_columns(g["home_win"], pl.lit(season).alias("season")))
    return pl.concat(out)


def _dmat(df: pl.DataFrame) -> xgb.DMatrix:
    x = df.select(_IN_GAME_FEATURES).to_numpy()
    return xgb.DMatrix(x, label=df["home_win"].to_numpy(), feature_names=_IN_GAME_FEATURES)


def _early_stop_rounds(train: pl.DataFrame, val: pl.DataFrame) -> int:
    """Boosting rounds by early stopping on the validation season's log loss."""
    dval = _dmat(val)
    b = xgb.train(
        _XGB_PARAMS, _dmat(train), _MAX_ROUNDS, evals=[(dval, "val")], early_stopping_rounds=30, verbose_eval=False
    )
    return int(b.best_iteration) + 1


def calibration_slope(y: np.ndarray, p: np.ndarray) -> float:
    """Cox calibration slope: the ``b`` in ``logit P(y) = a + b * logit(p)`` (IRLS, 1.0 is ideal)."""
    p = np.clip(p, 1e-6, 1 - 1e-6)
    X = np.column_stack([np.ones_like(p), np.log(p / (1 - p))])
    beta = np.zeros(2)
    for _ in range(50):
        mu = 1 / (1 + np.exp(-X @ beta))
        w = mu * (1 - mu)
        step = np.linalg.solve(X.T @ (X * w[:, None]), X.T @ (y - mu))
        beta += step
        if np.abs(step).max() < 1e-10:
            break
    return float(beta[1])


def _swap(booster: xgb.Booster) -> Any:
    """Point the serving path at ``booster``; returns the original loader to restore."""
    orig = nba_game_predict._load_in_game_artifact
    art = {"kind": "xgboost", "booster": booster, "features": booster.feature_names}
    nba_game_predict._load_in_game_artifact = lambda league_id="00": art  # type: ignore[assignment]
    return orig


def serve(league_id: str, pbp: pl.DataFrame, booster: xgb.Booster | None = None) -> pl.DataFrame:
    """Every game of ``pbp`` through ``score_win_prob`` (optionally with ``booster`` swapped in), plus labels."""
    lg = LEAGUES[league_id]["lg"]
    orig = _swap(booster) if booster is not None else None
    try:
        out = [score_win_prob(g, league=lg) for _, g in pbp.group_by("game_id", maintain_order=True)]
    finally:
        if orig is not None:
            nba_game_predict._load_in_game_artifact = orig
    return pl.concat(out, how="diagonal_relaxed")


def late_game_metrics(scored: pl.DataFrame, reg: int) -> dict[str, float]:
    """Horn-jump share + final-minute monotonicity from ``score_win_prob`` output (one or many games)."""
    s = scored.with_columns(
        (pl.col("home_score") - pl.col("away_score")).alias("_m"),
        (pl.col("team_id") == pl.col("home_team_id")).fill_null(False).alias("_hb"),
        (
            pl.col("type_text").is_in(["End Game", "End Period"])
            & (pl.col("period_number") >= reg)
            & (pl.col("home_score") != pl.col("away_score"))
        )
        .fill_null(False)
        .alias("_settle"),
    )
    horn = s.filter(pl.col("_settle")).group_by("game_id").agg(pl.col("wp_delta").abs().max().alias("step"))
    # final minute of the last regulation period or any overtime: consecutive model rows with the same
    # nonzero lead and the same possession flag, clock strictly running down
    m = s.filter(~pl.col("_settle") & (pl.col("period_number") >= reg)).with_columns(
        pl.col("_m", "_hb", "start_game_seconds_remaining", "home_win_prob", "period_number")
        .shift(1)
        .over("game_id")
        .name.suffix("_prev")
    )
    pairs = m.filter(
        (pl.col("start_game_seconds_remaining_prev") <= 60)
        & (pl.col("period_number") == pl.col("period_number_prev"))
        & (pl.col("start_game_seconds_remaining") < pl.col("start_game_seconds_remaining_prev"))
        & (pl.col("_m") == pl.col("_m_prev"))
        & (pl.col("_m") != 0)
        & (pl.col("_hb") == pl.col("_hb_prev"))
    ).with_columns(((pl.col("home_win_prob") - pl.col("home_win_prob_prev")) * pl.col("_m").sign()).alias("toward"))
    n = max(pairs.height, 1)
    return {
        "games": horn.height,
        "horn_jump_share": float((horn["step"] >= 0.20).mean()),
        "horn_step_median": float(horn["step"].median()),
        "final_minute_pairs": pairs.height,
        "final_minute_toward_share": float((pairs["toward"] > 1e-6).sum() / n),
        "final_minute_away_share": float((pairs["toward"] < -1e-6).sum() / n),
    }


def holdout_metrics(league_id: str, pbp: pl.DataFrame, booster: xgb.Booster) -> dict[str, float]:
    lg = LEAGUES[league_id]["lg"]
    season = int(pbp["season"][0])
    reg = game_regulation_periods(lg, season=season)
    scored = serve(league_id, pbp, booster)
    finals = scored.group_by("game_id").agg(
        (pl.col("home_score").last() > pl.col("away_score").last()).cast(pl.Float64).alias("y")
    )
    finals = finals.join(
        scored.group_by("game_id").agg((pl.col("home_score").last() == pl.col("away_score").last()).alias("tie")),
        on="game_id",
    )
    rows = scored.join(finals.filter(~pl.col("tie")), on="game_id").filter(
        ~(
            pl.col("type_text").is_in(["End Game", "End Period"])
            & (pl.col("period_number") >= reg)
            & (pl.col("home_score") != pl.col("away_score"))
        ).fill_null(False)
    )
    y, p = rows["y"].to_numpy(), rows["home_win_prob"].to_numpy()
    return {
        "plays": int(rows.height),
        "brier": float(np.mean((p - y) ** 2)),
        "log_loss": float(-np.mean(y * np.log(np.clip(p, 1e-9, 1)) + (1 - y) * np.log(np.clip(1 - p, 1e-9, 1)))),
        "calibration_slope": calibration_slope(y, p),
        **late_game_metrics(scored, reg),
    }


def _baseline(artifact: str, ref: str) -> xgb.Booster:
    raw = subprocess.run(
        ["git", "show", f"{ref}:sportsdataverse/nba/models/{artifact}"], cwd=REPO, capture_output=True, check=True
    ).stdout
    b = xgb.Booster()
    b.load_model(bytearray(raw))
    return b


def grid_monotone_violations(booster: xgb.Booster) -> int:
    """Count final-minute grid steps where WP moves away from the leader as the clock runs down."""
    secs = np.round(np.arange(60.0, 0.0, -0.1), 1)
    bad = 0
    for lead in [*range(-15, 0), *range(1, 16)]:
        for logit in (-1.5, 0.0, 1.5):
            for hb in (0, 1):
                f = pl.DataFrame(
                    {"score_diff": float(lead), "sec_left": secs, "pregame_logit": logit, "home_has_ball": hb}
                ).with_columns(
                    pl.col("sec_left").sqrt().alias("sqrt_sec_left"),
                    pl.col("sec_left").clip(lower_bound=60.0).sqrt().alias("sqrt_sec_left_60"),
                    (pl.col("score_diff") / (pl.col("sec_left") + 1.0).sqrt()).alias("margin_per_sqrt_sec"),
                )
                p = booster.inplace_predict(f.select(booster.feature_names).to_numpy())
                bad += int((np.diff(p) * math.copysign(1, lead) < -1e-6).sum())
    return bad


def train(league_id: str, train_seasons: list[int], holdout: int, artifact: str, baseline_ref: str) -> dict[str, Any]:
    lg = LEAGUES[league_id]["lg"]
    assert holdout not in train_seasons, "train and holdout seasons must be disjoint"
    frames = {s: season_frame(league_id, s) for s in train_seasons}
    *fit_s, val_s = sorted(train_seasons)
    rounds = _early_stop_rounds(pl.concat([frames[s] for s in fit_s]), frames[val_s])
    booster = xgb.train(_XGB_PARAMS, _dmat(pl.concat(frames.values())), num_boost_round=rounds)
    booster.set_attr(
        train_seasons=",".join(map(str, train_seasons)),
        holdout_season=str(holdout),
        fitting_script="tools/models/nba_prediction/train_in_game_wp.py",
    )

    hold = _load_pbp(league_id, holdout)
    base = _baseline(artifact, baseline_ref)
    report = {
        "league_id": league_id,
        "league": lg,
        "train_seasons": train_seasons,
        "validation_season": val_s,
        "holdout_season": holdout,
        "baseline_ref": baseline_ref,
        "n_train_plays": int(sum(f.height for f in frames.values())),
        "rounds": rounds,
        "params": _XGB_PARAMS,
        "features": _IN_GAME_FEATURES,
        "baseline": holdout_metrics(league_id, hold, base) | {"grid_violations": grid_monotone_violations(base)},
        "refit": holdout_metrics(league_id, hold, booster) | {"grid_violations": grid_monotone_violations(booster)},
    }
    booster.save_model(str(MODELS_DIR / artifact))
    (HERE / f"in_game_wp_{lg}_holdout.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    reg = game_regulation_periods(lg, season=holdout)
    late = hold.filter(
        (pl.col("period_number") > reg)
        | ((pl.col("period_number") == reg) & (pl.col("start_game_seconds_remaining") <= LATE_SECONDS))
    ).select(FIXTURE_COLS)
    late.write_parquet(FIXTURE_DIR / f"{lg}_late_game_{holdout}.parquet", compression="zstd")
    print(json.dumps({k: report[k] for k in ("rounds", "n_train_plays", "baseline", "refit")}, indent=2))
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--league-id", default="00", choices=sorted(LEAGUES))
    ap.add_argument("--train-seasons", type=int, nargs="+")
    ap.add_argument("--holdout-season", type=int)
    ap.add_argument("--baseline-ref", default=BASELINE_REF)
    args = ap.parse_args()
    cfg = LEAGUES[args.league_id]
    train(
        args.league_id,
        args.train_seasons or cfg["train"],
        args.holdout_season or cfg["holdout"],
        cfg["artifact"],
        args.baseline_ref,
    )
