"""Fit the rookie/sophomore residual artifact (composes ①②③ + a per-tier residual).

Reads the committed fixtures (offline), scores the training-era combine
classes through the *bundled* draft model (①) + aging curve (②), composes the
same way `nba_rookie_projection` does at runtime, and fits
``residual[pro_tier] = mean(realized_rookie_value - composed_value)`` on
those training classes only (the holdout classes are reserved for the
Task-4.2 gate). Writes `sportsdataverse/nba/models/nba_rookie_projection.json`.

Run: ``uv run python dev/nba_draft/fit_rookie_residual.py`` (offline).
"""

from __future__ import annotations

import json

import polars as pl

from sportsdataverse.nba.nba_aging_curve import nba_aging_curve
from sportsdataverse.nba.nba_draft_constants import as_of_class_split
from sportsdataverse.nba.nba_draft_model import _load_artifact, _score

FIXTURE_DIR = "tests/fixtures/nba_draft"
ARTIFACT_PATH = "sportsdataverse/nba/models/nba_rookie_projection.json"
CUTOFF_YEAR = 2015
ROOKIE_AGE = 19.0


def main() -> None:
    combine = pl.read_parquet(f"{FIXTURE_DIR}/combine_2016_2019.parquet")
    rookie = pl.read_parquet(f"{FIXTURE_DIR}/rookie_values.parquet")
    career = pl.read_parquet(f"{FIXTURE_DIR}/career_values.parquet")

    raw_numeric_cols = [
        c
        for c in [
            "height_wo_shoes",
            "weight",
            "wingspan",
            "standing_reach",
            "body_fat_pct",
            "hand_length",
            "hand_width",
            "lane_agility",
            "three_quarter_sprint",
            "standing_vertical",
            "max_vertical",
            "spot_fifteen_corner_left_pct",
            "offdrib_fifteen_top_pct",
        ]
        if c in combine.columns
    ]
    combine = combine.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in raw_numeric_cols])
    combine = combine.with_columns(
        (pl.col("weight") / (pl.col("height_wo_shoes") ** 2) * 703.0).alias("bmi"),
        (pl.col("wingspan") - pl.col("height_wo_shoes")).alias("wingspan_diff"),
    )
    art = _load_artifact("nba")
    feature_median = art.get("feature_median", {})
    for col in art["features"]:
        combine = combine.with_columns(pl.col(col).fill_null(feature_median.get(col, 0.0)))
    scored = _score(combine, art)  # player_id, draft_year, proj_career_value, draft_prob, projected_pick, pro_tier

    train_scored, _ = as_of_class_split(scored, cutoff_year=CUTOFF_YEAR)

    curve = nba_aging_curve().select("age", "rel_value")
    rel_rookie = float(curve.filter(pl.col("age") == int(ROOKIE_AGE))["rel_value"][0])
    rel_peak = 1.0  # curve is normalized so its peak is 1.0
    rookie_fraction_candidates = career.join(
        rookie.select("player_id", "rookie_value"), on="player_id", how="inner"
    ).filter(pl.col("career_value") > 0)
    rookie_fraction = float(
        (rookie_fraction_candidates["rookie_value"] / rookie_fraction_candidates["career_value"]).median()
    )
    rookie_fraction = max(0.02, min(rookie_fraction, 0.5))  # sane bounds
    print(f"rookie_fraction (median rookie_value/career_value): {rookie_fraction:.4f}")

    composed = train_scored.with_columns(
        (pl.col("proj_career_value") * rookie_fraction * (rel_rookie / rel_peak)).alias("composed_value")
    )
    composed = composed.join(rookie.select("player_id", "rookie_value"), on="player_id", how="left").with_columns(
        pl.col("rookie_value").fill_null(0.0)
    )
    residual_by_tier = (
        composed.group_by("pro_tier")
        .agg((pl.col("rookie_value") - pl.col("composed_value")).mean().alias("residual"), pl.len().alias("n"))
        .sort("pro_tier")
    )
    print(residual_by_tier)

    artifact = {
        "league": "nba",
        "rookie_fraction": rookie_fraction,
        "residual": dict(zip(residual_by_tier["pro_tier"].to_list(), residual_by_tier["residual"].to_list())),
    }
    with open(ARTIFACT_PATH, "w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2)
    print(f"wrote {ARTIFACT_PATH}")


if __name__ == "__main__":
    main()
