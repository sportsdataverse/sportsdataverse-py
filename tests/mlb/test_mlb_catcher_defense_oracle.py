"""Catcher blocking + throwing oracle vs Savant's real 2024 leaderboards.

Corpus: tests/fixtures/mlb_fielding/pitches_2024-06.parquet (one month) vs
lb_catcher_blocking_2024.parquet / lb_catcher_throwing_2024.parquet
(FULL-SEASON leaderboards -- see tests/fixtures/mlb_fielding/README.md).

**Throwing model bug fixed (0.0.x):** ``mlb_catcher_throwing`` previously
computed expected caught-stealing by binning the catcher's OWN pop time, which
cancels exactly the pop-time skill Savant credits -- it correlated ~-0.01 with
the leaderboard full-season. Expected CS is now the catcher-INDEPENDENT
per-base league CS rate, so a catcher's caught-stealings above that baseline
(driven by their arm) survive as signal. That moved the full-season Pearson to
~+0.073 (see ``test_mlb_fielding_oracle_live.py``).

**Known DATA ceiling (documented, not silently passed over):** SB/CS attempts
in this feed are narrated only as a `des`-text clause on a *different* batter's
plate appearance (see `mlb_stolen_base.py`'s module docstring); the `events`
column carries none. Only ~401 of ~1773 real season attempts are recoverable,
so per-catcher samples stay thin and the full-season throwing correlation is
data-capped at ~0.073 (n=52). Per the "never lower the gate to pass" rule,
these OFFLINE month tests do NOT assert an arbitrary magnitude floor rounded
from noise -- they assert the pipelines wire together correctly on real data;
the numeric full-season floors live in the ``@skip_if_no_live`` gate.
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
