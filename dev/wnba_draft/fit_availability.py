"""Fit the WNBA availability (games-available %) logistic artifact on real WNBA data.

Genuine re-fit -- the shipped T3.4 artifact was a hand-picked seed
(``coef=[0.0, 2.0, 1.0, 0.0, 0.0]``, ``intercept=1.0``), not an NBA relabel. Reads
``tests/fixtures/wnba_draft/season_stats_raw.parquet`` (real per-drafted-player-season GP +
age, captured live from ``stats.wnba.com``), builds ``(player_id, season)`` availability
features via the shared :func:`sportsdataverse.nba.nba_availability.availability_features`
(bound to ``league="wnba"`` so the GP% denominator uses the 40-game WNBA season), splits
train/holdout on season (as-of-season leakage boundary -- same gap-safe join pattern as the
NBA fit), and fits the fractional-logistic IRLS model of realized GP%.

Population caveat: like the draft-value fit, this corpus is *drafted WNBA players only* (there
is no separate bulk full-league capture in this script) -- narrower than the NBA fit's combine
population but the same kind of population (a roster corpus, not literally every player who
ever suited up), and it is the same corpus every other WNBA artifact in this re-fit uses.

Run: ``uv run python dev/wnba_draft/fit_availability.py`` (offline).
"""

from __future__ import annotations

import json

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_availability import _FEATURE_COLS, availability_features
from sportsdataverse.nba.nba_draft_constants import as_of_class_split, calibration_table, logistic_fit_irls, mae

FIXTURE_DIR = "tests/fixtures/wnba_draft"
ARTIFACT_PATH = "sportsdataverse/nba/models/wnba_availability.json"
# See progress.md for the observed train/holdout split diagnostics that picked this cutoff.
CUTOFF_SEASON = 2018
MIN_SEASON = 1997


def main() -> None:
    season_stats = pl.read_parquet(f"{FIXTURE_DIR}/season_stats_raw.parquet")
    # maintain_order=True: see fit_aging_curve.py's comment on this same dedup pattern --
    # polars' default sort is unstable under threads, so ties in "min" would otherwise pick a
    # non-deterministic survivor across runs.
    season_stats = season_stats.sort("min", descending=True, maintain_order=True).unique(
        subset=["player_id", "season_id"], keep="first", maintain_order=True
    )
    career = season_stats.with_columns(
        season_stats["season_id"].str.slice(0, 4).cast(pl.Int64).alias("season"),
        pl.col("player_age").alias("age"),
    ).filter(pl.col("season") >= MIN_SEASON)

    train_raw = career.filter(pl.col("season") <= CUTOFF_SEASON)
    gp_median = float((train_raw["gp"].cast(pl.Float64) / 40.0).clip(0.0, 1.0).median() or 0.75)
    median_ref = {"gp_pct": gp_median, "bmi": 24.0}

    feats = availability_features(career, league="wnba", median_ref=median_ref)
    labeled = feats.with_columns((career["gp"].cast(pl.Float64) / 40.0).clip(0.0, 1.0).alias("realized_gp_pct"))

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

    baseline_pred = holdout["career_gp_pct"].to_numpy()
    baseline_mae = mae(baseline_pred, y_holdout)
    print(f"holdout MAE (career-mean baseline): {baseline_mae:.4f}")

    calib = calibration_table(y_holdout, fitted_holdout, n_bins=10)
    print(calib)

    artifact = {
        "league": "wnba",
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
