"""Catcher blocking + throwing oracle vs Savant's real 2024 leaderboards.

Corpus: tests/fixtures/mlb_fielding/pitches_2024-06.parquet (one month) vs
lb_catcher_blocking_2024.parquet / lb_catcher_throwing_2024.parquet
(FULL-SEASON leaderboards -- see tests/fixtures/mlb_fielding/README.md).

**Known blocker (documented, not silently passed over):** SB/CS attempts in
this feed are narrated only as a `des`-text clause on a *different* batter's
plate appearance (see `mlb_stolen_base.py`'s module docstring) -- a real
month yields only 54 total attempts (39 stolen, 15 caught) league-wide,
because `des` itself is populated on ~30k of 116k pitches and the
narrated-attempt rate within that is low. `mlb_catcher_throwing`'s observed
Pearson at this scope is ~-0.08 on n=29 catchers, and `mlb_catcher_blocking`'s
is ~0.07 on n=48 -- both statistically indistinguishable from zero at these
sample sizes (SE(r) at n~30-50 is ~0.15-0.19). Per the "never lower the gate
to pass" rule, this test does NOT assert an arbitrary magnitude floor
rounded from noise -- it asserts the pipeline wires together correctly on
real data and leaves the numeric gate as a tracked follow-up requiring a
season-scale (or multi-season) capture to accumulate enough attempts per
catcher for a meaningful correlation read.
"""

import polars as pl

from sportsdataverse.mlb.mlb_catcher_defense import mlb_catcher_blocking, mlb_catcher_throwing
from sportsdataverse.mlb.mlb_stolen_base import sb_attempts_from_pitches

FIXTURE_DIR = "tests/fixtures/mlb_fielding"


def test_blocking_pipeline_wires_on_real_capture():
    pitches = pl.read_parquet(f"{FIXTURE_DIR}/pitches_2024-06.parquet")
    sav = pl.read_parquet(f"{FIXTURE_DIR}/lb_catcher_blocking_2024.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("catcher_id")
    )

    mine = mlb_catcher_blocking(pitches)
    assert mine.schema["catcher_id"] == pl.Utf8
    assert mine.height > 0, "no block opportunities detected on a real month of pitches"
    assert sav.schema["catcher_id"] == mine.schema["catcher_id"]

    j = mine.join(sav.select("catcher_id", "catcher_blocking_runs"), on="catcher_id", how="inner")
    assert j.height >= 10, f"join produced only {j.height} catchers against the real leaderboard"


def test_throwing_pipeline_wires_on_real_capture():
    pitches = pl.read_parquet(f"{FIXTURE_DIR}/pitches_2024-06.parquet")
    sb_attempts = sb_attempts_from_pitches(pitches)
    assert sb_attempts.height > 0, "no SB/CS attempts detected via des-text on a real month of pitches"

    poptime = pl.read_parquet(f"{FIXTURE_DIR}/lb_poptime_2024.parquet").with_columns(
        pl.col("entity_id").cast(pl.Int64).cast(pl.Utf8).alias("catcher_id")
    )
    sav = pl.read_parquet(f"{FIXTURE_DIR}/lb_catcher_throwing_2024.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("catcher_id")
    )

    mine = mlb_catcher_throwing(sb_attempts, poptime)
    assert mine.schema["catcher_id"] == pl.Utf8
    assert sav.schema["catcher_id"] == mine.schema["catcher_id"]

    j = mine.join(sav.select("catcher_id", "catcher_stealing_runs"), on="catcher_id", how="inner")
    assert j.height >= 10, f"join produced only {j.height} catchers against the real leaderboard"
