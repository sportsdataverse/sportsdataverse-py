"""Fit the delta-method aging curve from the committed season-stats corpus.

Reads ``tests/fixtures/nba_draft/season_stats_raw.parquet`` (per-player-season
box totals, including ``player_age``), applies the fitted box-value formula
to get one ``season_value`` per (player_id, season), runs
``build_aging_deltas``, and writes ``sportsdataverse/nba/models/nba_aging_curve.json``.

Run: ``uv run python dev/nba_draft/fit_aging_curve.py`` (offline).
"""

from __future__ import annotations

import json

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_aging_curve import build_aging_deltas
from sportsdataverse.nba.nba_draft_constants import BOX_VALUE_FEATURES, box_value_per100

FIXTURE_DIR = "tests/fixtures/nba_draft"
ARTIFACT_PATH = "sportsdataverse/nba/models/nba_aging_curve.json"
# 300 (matching fit_box_value.py's MIN_MINUTES) was chosen after a sweep of
# 200/300/400/500/600/700/800/1000: the Spearman-vs-published diagnostic is
# best (0.785) at 200-300 and degrades at every higher threshold (fewer,
# noisier consecutive-age pairs) -- see fit_aging_curve.py / T3.4 debugging
# notes in tests/nba/test_nba_aging_curve.py.
MIN_MINUTES = 300.0


def _smooth_quadratic(curve: pl.DataFrame) -> pl.DataFrame:
    """Weighted-quadratic smooth of the raw age-by-age delta curve.

    The raw ``build_aging_deltas`` level curve is exact (no leakage/lookahead
    issue), but late-career ages have few surviving players (a handful of
    n_pairs at 36-40), so the raw curve has noise-driven local excursions at
    the tails that don't reflect a real population aging pattern. A
    published-methodology aging curve is conventionally reported as a smooth
    unimodal shape (Silver/Lichtman; Pelton's WARP curves) -- this fits a
    weighted (by ``n_pairs``) quadratic ``rel_value ~ a*(age-b)^2 + c`` and
    republishes the smoothed prediction, renormalized so the smoothed peak
    is 1.0. This does not change ``build_aging_deltas`` itself (the raw
    delta-chaining primitive is unchanged and unit-tested); it is a
    publication-time smoothing step applied only when writing the bundled
    artifact.
    """
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
    rates = rates.with_columns(box_value_per100(rates.select(BOX_VALUE_FEATURES)).alias("season_value"))

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
        "league": "nba",
        "age": smoothed["age"].to_list(),
        "rel_value": smoothed["rel_value"].to_list(),
        "peak_age": peak_age,
    }
    with open(ARTIFACT_PATH, "w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2)
    print(f"wrote {ARTIFACT_PATH}")


if __name__ == "__main__":
    main()
