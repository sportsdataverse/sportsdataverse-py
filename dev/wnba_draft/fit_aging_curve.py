"""Fit the WNBA delta-method aging curve from the committed real WNBA season-stats corpus.

Genuine re-fit -- this is the artifact that shipped in T3.4 as a byte-for-byte copy of
``nba_aging_curve.json`` relabeled ``"league": "wnba"``. Reads
``tests/fixtures/wnba_draft/season_stats_raw.parquet`` (real per-drafted-player-season box
totals + ``player_age``, captured live from ``stats.wnba.com`` 2026-07-11), scores each
player-season through the shared (still NBA-borrowed, see ``fit_box_value.py``) box-value
formula, runs ``build_aging_deltas`` on the real WNBA age trajectories, and writes
``sportsdataverse/nba/models/wnba_aging_curve.json``.

Run: ``uv run python dev/wnba_draft/fit_aging_curve.py`` (offline).
"""

from __future__ import annotations

import json

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_aging_curve import build_aging_deltas
from sportsdataverse.nba.nba_draft_constants import box_value_per100

FIXTURE_DIR = "tests/fixtures/wnba_draft"
ARTIFACT_PATH = "sportsdataverse/nba/models/wnba_aging_curve.json"
# WNBA careers are shorter and the drafted-player-only corpus is smaller than NBA's -- swept
# 100/150/200/300 minimum-minutes thresholds; see progress.md for the observed n_pairs/shape
# trade-off that picked this value.
MIN_MINUTES = 150.0


def _smooth_quadratic(curve: pl.DataFrame) -> pl.DataFrame:
    """Weighted-quadratic smooth of the raw age-by-age delta curve (same as the NBA fit)."""
    ages = curve["age"].to_numpy().astype(float)
    rel = curve["rel_value"].to_numpy().astype(float)
    weights = curve["n_pairs"].to_numpy().astype(float)
    coeffs = np.polyfit(ages, rel, deg=2, w=np.sqrt(weights))
    smoothed = np.polyval(coeffs, ages)
    floor_frac = 0.4
    smoothed_range = smoothed.max() - smoothed.min()
    if smoothed_range <= 0:
        smoothed = np.ones_like(smoothed)
    else:
        unit = (smoothed - smoothed.min()) / smoothed_range
        smoothed = floor_frac + (1.0 - floor_frac) * unit
    return curve.with_columns(pl.Series("rel_value", smoothed, dtype=pl.Float64))


def main() -> None:
    season_stats = pl.read_parquet(f"{FIXTURE_DIR}/season_stats_raw.parquet")
    # maintain_order=True: polars' default sort is unstable under threads, so ties in "min"
    # (genuine mid-season-trade duplicates with identical minutes) would otherwise pick a
    # non-deterministic survivor across separate runs of this script -- verified by re-running
    # fit_availability.py twice and observing the holdout MAE drift by ~0.004 between runs.
    season_stats = season_stats.sort("min", descending=True, maintain_order=True).unique(
        subset=["player_id", "season_id"], keep="first", maintain_order=True
    )

    raw_numeric = ["fga", "fta", "tov", "pts", "reb", "ast", "stl", "blk", "min"]
    season_stats = season_stats.with_columns(
        [pl.col(c).cast(pl.Float64, strict=False) for c in raw_numeric if c in season_stats.columns]
    )
    pos_est = pl.col("fga") + 0.44 * pl.col("fta") + pl.col("tov")
    rates = season_stats.with_columns(
        pos_est.alias("_pos_est"),
        pl.col("player_age").round(0).cast(pl.Int64).alias("age"),
    ).with_columns(
        (pl.col("pts") / pl.col("_pos_est").clip(1.0, None) * 100).alias("pts100"),
        (pl.col("reb") / pl.col("_pos_est").clip(1.0, None) * 100).alias("reb100"),
        (pl.col("ast") / pl.col("_pos_est").clip(1.0, None) * 100).alias("ast100"),
        (pl.col("stl") / pl.col("_pos_est").clip(1.0, None) * 100).alias("stl100"),
        (pl.col("blk") / pl.col("_pos_est").clip(1.0, None) * 100).alias("blk100"),
        (pl.col("tov") / pl.col("_pos_est").clip(1.0, None) * 100).alias("tov100"),
        (pl.col("pts") / (2 * (pl.col("fga") + 0.44 * pl.col("fta")).clip(1.0, None))).alias("ts_pct"),
        (pl.col("_pos_est") / pl.col("min").clip(1.0, None) * 100).alias("usg"),
        pl.col("min").alias("minutes"),
    )
    rates = rates.with_columns(box_value_per100(rates, league="wnba").alias("season_value"))

    season_values = rates.select("player_id", "age", "season_value", "minutes")
    curve = build_aging_deltas(season_values, min_minutes=MIN_MINUTES)
    print(f"raw curve rows: {curve.height}")
    print(curve)

    smoothed = _smooth_quadratic(curve)
    print("smoothed curve:")
    print(smoothed)

    peak_row = smoothed.filter(pl.col("rel_value") == pl.col("rel_value").max())
    peak_age = int(peak_row["age"][0])
    print(f"peak_age (smoothed): {peak_age}")

    artifact = {
        "league": "wnba",
        "age": smoothed["age"].to_list(),
        "rel_value": smoothed["rel_value"].to_list(),
        "peak_age": peak_age,
    }
    with open(ARTIFACT_PATH, "w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2)
    print(f"wrote {ARTIFACT_PATH}")


if __name__ == "__main__":
    main()
