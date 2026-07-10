"""Fit the availability (games-available %) logistic artifact.

Reads `tests/fixtures/nba_draft/season_stats_raw.parquet` (per-player-season
GP + age), builds `(player_id, season)` availability features, splits
train/holdout on season (as-of-season leakage boundary -- `prior_gp_pct` for
a held-out season only ever uses seasons strictly before it, join-based so a
genuine multi-season gap in a player's captured history nulls out rather
than silently reading a wrong season), and fits a fractional-logistic
("quasi-binomial") IRLS model of realized GP% via the shared
`logistic_fit_irls` (a continuous target in [0,1] is a standard
fractional-response use of the same IRLS update).

**Debugging record:** a first pass used `.shift(1).over("player_id")` for
`prior_gp_pct`, which is row-position-based and would silently misattribute
a wrong season across any real multi-year gap -- fixed to a `season - 1`
self-join (matching `nba_aging_curve.build_aging_deltas`'s proven pattern) in
`nba_availability.availability_features`. Spot-checking individual players'
season sequences after the fix confirmed the join is now correct (no
gap-crossing). The resulting `prior_gp_pct`/`career_gp_pct` correlations
with realized GP% are still weak (~0.01-0.02) on this corpus -- year-to-year
games-played is evidently noisy (coaching decisions, role changes, one-off
injuries) even among NBA players who cleared the combine-class draft-outcome
bar. Despite the weak raw feature correlations, the fitted model still
**beats the naive career-mean baseline on held-out seasons** (0.2515 vs
0.2876 MAE) -- the actual Task-3.2 gate requirement -- so it is shipped as
a real, if modest, improvement over the naive baseline.

Run: ``uv run python dev/nba_draft/fit_availability.py`` (offline).
"""

from __future__ import annotations

import json

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_availability import _FEATURE_COLS, availability_features
from sportsdataverse.nba.nba_draft_constants import as_of_class_split, calibration_table, logistic_fit_irls, mae

FIXTURE_DIR = "tests/fixtures/nba_draft"
ARTIFACT_PATH = "sportsdataverse/nba/models/nba_availability.json"
CUTOFF_SEASON = 2016  # matches the draft-model cutoff era; holdout = 2017-2025 seasons
# 13/7378 raw rows (pre-dedup) carried a malformed player_id ("199") from an
# upstream id-join glitch in the combine capture, with season labels back to
# 1987-88 (decades before any combine class in this corpus) -- excluded as a
# data-quality anomaly, not a modeling choice. Separately, 772/7378 raw rows
# were mid-season-trade duplicates (a per-team row + a team_id=0 "TOT" row
# for the same player-season) -- deduplicated in-place in
# season_stats_raw.parquet (kept the TOT row) before any fit reads it, since
# the duplication silently double/triple-counted traded players' box totals
# and minutes across every downstream label (career_value, aging curve,
# availability).
MIN_SEASON = 2000


def main() -> None:
    season_stats = pl.read_parquet(f"{FIXTURE_DIR}/season_stats_raw.parquet")
    career = season_stats.with_columns(
        season_stats["season_id"].str.slice(0, 4).cast(pl.Int64).alias("season"),
        pl.col("player_age").alias("age"),
    ).filter(pl.col("season") >= MIN_SEASON)

    # The imputation medians must come from TRAIN seasons only (leak-free
    # as-of-season boundary). Features themselves are built on the FULL frame
    # because prior_gp_pct/career_gp_pct need each player's whole observed
    # time series (a holdout row's prior season legitimately lives in train);
    # only the NULL-fill scalar is train-derived. season_stats has no bmi
    # column, so bmi is entirely null -> the 24.0 default (unchanged behavior).
    train_raw = career.filter(pl.col("season") <= CUTOFF_SEASON)
    gp_median = float((train_raw["gp"].cast(pl.Float64) / 82.0).clip(0.0, 1.0).median() or 0.75)
    median_ref = {"gp_pct": gp_median, "bmi": 24.0}

    feats = availability_features(career, median_ref=median_ref)
    labeled = feats.with_columns((career["gp"].cast(pl.Float64) / 82.0).clip(0.0, 1.0).alias("realized_gp_pct"))

    train, holdout = as_of_class_split(labeled, cutoff_year=CUTOFF_SEASON, year_col="season")
    print(f"train rows: {train.height}, holdout rows: {holdout.height}")
    print(f"train-derived gp_pct impute median: {gp_median:.4f}")

    X_train = train.select(_FEATURE_COLS).to_numpy()
    y_train = train["realized_gp_pct"].to_numpy()
    beta = logistic_fit_irls(X_train, y_train)

    def _score(X: np.ndarray) -> np.ndarray:
        logit = beta[0] + X @ beta[1:]
        return 1.0 / (1.0 + np.exp(-logit))

    fitted_train = _score(X_train)
    print(f"train MAE: {mae(fitted_train, y_train):.4f}")

    X_holdout = holdout.select(_FEATURE_COLS).to_numpy()
    y_holdout = holdout["realized_gp_pct"].to_numpy()
    fitted_holdout = _score(X_holdout)
    holdout_mae = mae(fitted_holdout, y_holdout)
    print(f"holdout MAE: {holdout_mae:.4f}")

    baseline_pred = holdout["career_gp_pct"].to_numpy()  # naive "avail = career-mean GP%" baseline
    baseline_mae = mae(baseline_pred, y_holdout)
    print(f"holdout MAE (career-mean baseline): {baseline_mae:.4f}")

    calib = calibration_table(y_holdout, fitted_holdout, n_bins=10)
    print(calib)

    artifact = {
        "league": "nba",
        "features": _FEATURE_COLS,
        "coef": beta[1:].tolist(),
        "intercept": float(beta[0]),
        "cutoff_season": CUTOFF_SEASON,
    }
    with open(ARTIFACT_PATH, "w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2)
    print(f"wrote {ARTIFACT_PATH}")


if __name__ == "__main__":
    main()
