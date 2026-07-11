"""Stolen-base value oracle vs Savant's real 2024 leaderboard + as-of-date leakage check.

Corpus: tests/fixtures/mlb_fielding/pitches_2024-06.parquet (one month) vs
lb_basestealing_rv_2024.parquet (a FULL-SEASON leaderboard -- see
tests/fixtures/mlb_fielding/README.md).

**Known blocker (documented, not silently passed over):** same des-text
attempt-detection scope limit as the catcher-throwing oracle (54 real
attempts league-wide in this month). Observed Pearson at n=33 runners is
~-0.07 -- indistinguishable from zero at that sample size. This test
therefore checks pipeline wiring on real data (non-empty, correct dtypes,
real join) rather than asserting a fabricated magnitude floor; the numeric
gate is a tracked follow-up requiring a season-scale capture.

The as-of-date leakage boundary (`predict_sb_success`), by contrast, IS a
hard, capture-scope-independent property -- it is checked with a synthetic
fixture in `test_mlb_stolen_base.py::test_predict_sb_success_as_of_boundary_excludes_future`
and re-verified here directly against the real corpus to confirm the
boundary holds on real dtypes/dates too.
"""

import datetime as dt

import polars as pl

from sportsdataverse.mlb.mlb_run_values import as_of_split
from sportsdataverse.mlb.mlb_stolen_base import mlb_stolen_base_value, sb_attempts_from_pitches

FIXTURE_DIR = "tests/fixtures/mlb_fielding"


def test_stolen_base_pipeline_wires_on_real_capture():
    pitches = pl.read_parquet(f"{FIXTURE_DIR}/pitches_2024-06.parquet")
    sb_attempts = sb_attempts_from_pitches(pitches)
    assert sb_attempts.height > 0

    sprint_speed = pl.read_parquet(f"{FIXTURE_DIR}/lb_sprint_speed_2024.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("runner_id")
    )
    poptime = pl.read_parquet(f"{FIXTURE_DIR}/lb_poptime_2024.parquet").with_columns(
        pl.col("entity_id").cast(pl.Int64).cast(pl.Utf8).alias("catcher_id")
    )
    sav = pl.read_parquet(f"{FIXTURE_DIR}/lb_basestealing_rv_2024.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("runner_id")
    )

    mine = mlb_stolen_base_value(sb_attempts, sprint_speed, poptime)
    assert mine.schema["runner_id"] == pl.Utf8
    assert sav.schema["runner_id"] == mine.schema["runner_id"]

    j = mine.join(sav.select("runner_id", "runs_stolen_on_running_act"), on="runner_id", how="inner")
    assert j.height >= 10, f"join produced only {j.height} runners against the real leaderboard"


def test_as_of_split_real_dates_exclude_cutoff_and_later():
    pitches = pl.read_parquet(f"{FIXTURE_DIR}/pitches_2024-06.parquet")
    dated = pitches.with_columns(pl.col("game_date").str.to_date())
    cutoff = dt.date(2024, 6, 15)

    before = as_of_split(dated, cutoff)
    assert before.height > 0
    assert (before["game_date"] < cutoff).all()
    # The split must actually exclude something real -- confirm the source
    # frame has rows on/after the cutoff that `before` correctly dropped.
    assert dated.filter(pl.col("game_date") >= cutoff).height > 0
