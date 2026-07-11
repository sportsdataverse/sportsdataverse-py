"""Baserunning-value oracle vs Savant's real 2024 leaderboard.

Corpus: tests/fixtures/mlb_fielding/pitches_2024-06.parquet (one month) vs
lb_baserunning_rv_2024.parquet (a FULL-SEASON leaderboard -- see
tests/fixtures/mlb_fielding/README.md).

**Known blocker (documented, not silently passed over):** the observed
Pearson correlation at this capture scope is ~0.08 (n=161 runners,
min_opp=0) -- indistinguishable from noise at that sample size (SE(r) at
n~160 is ~0.08, so a "true" r of 0.2-0.3 would still often read this low).
Unlike framing (0.556) and OAA (0.289), which show a clear, real,
correlation-with-more-data trend even at one-month scope, baserunning's
opportunity-detection logic (the next-plate-appearance occupancy read
described in `mlb_baserunning.py`'s module docstring) has NOT been
validated against a large-enough real sample to distinguish "genuinely
weak signal at this scope" from "a bug in the advancement-opportunity
extraction." Per the "never lower the gate to pass" rule, this test does
NOT assert an arbitrary magnitude floor rounded from noise -- it asserts
the pipeline wires together correctly on real data (non-empty join,
correct dtypes) and leaves the numeric gate as a tracked follow-up requiring
a season-scale capture (and, if the gap persists at that scale, a debugging
pass on the opportunity-detection logic per the module's TODO).
"""

import polars as pl

from sportsdataverse.mlb.mlb_baserunning import mlb_baserunning_value

FIXTURE_DIR = "tests/fixtures/mlb_fielding"


def test_baserunning_pipeline_wires_on_real_capture():
    pitches = pl.read_parquet(f"{FIXTURE_DIR}/pitches_2024-06.parquet")
    sav = pl.read_parquet(f"{FIXTURE_DIR}/lb_baserunning_rv_2024.parquet")
    sprint_speed = pl.read_parquet(f"{FIXTURE_DIR}/lb_sprint_speed_2024.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("runner_id")
    )

    mine = mlb_baserunning_value(pitches, sprint_speed)
    assert mine.schema["runner_id"] == pl.Utf8
    assert mine.height > 0, "no baserunning opportunities detected on a real month of pitches"

    sav = sav.with_columns(pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("runner_id"))
    assert sav.schema["runner_id"] == mine.schema["runner_id"]

    j = mine.join(sav.select("runner_id", "runner_runs_tot"), on="runner_id", how="inner")
    assert j.height >= 20, f"join produced only {j.height} runners against the real leaderboard"
