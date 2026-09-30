"""Pace counts each ESPN drive clock once, for the drive's own offense, in regulation only.

Real fixture: two 2025 CFB games from the released pbp.

* 401762461 -- North Texas's overtime drive carries ESPN ``drive.timeElapsed`` "15:00"
  (900 s over 3 plays); overtime has no game clock, and ESPN files most OT drives as 0:00.
* 401757277 -- UTEP at Liberty: four ESPN drive ids hold standing snaps by BOTH offenses, so
  grouping by (team, drive id) handed each offense the whole drive clock and play count.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.tendencies import tendencies

FIX = Path(__file__).parent / "fixtures" / "cfb_2025_pace_ot_and_shared_drives.parquet"
PACE = ["drive_seconds", "drive_plays", "drive_seconds_neutral", "drive_plays_neutral", "drives_with_clock"]


@pytest.fixture(scope="module")
def plays() -> pl.DataFrame:
    return pl.read_parquet(FIX)


def _standing(p: pl.DataFrame) -> pl.DataFrame:
    return p.filter(pl.col("scrimmage_play") & ~pl.col("penalty_no_play") & pl.col("drive.id").is_not_null())


def _secs(c: str) -> pl.Expr:
    parts = pl.col(c).str.split(":")
    return parts.list.get(0).cast(pl.Float64) * 60 + parts.list.get(1).cast(pl.Float64)


def test_fixture_holds_the_ot_drive_and_shared_drive_ids(plays):
    s = _standing(plays)
    ot = s.filter((pl.col("period") > 4) & (pl.col("drive.timeElapsed.displayValue") == "15:00"))
    assert ot["pos_team"].unique().to_list() == ["North Texas Mean Green"]
    shared = s.group_by("game_id", "drive.id").agg(n=pl.col("pos_team").n_unique()).filter(pl.col("n") > 1)
    assert shared.height == 4 and shared["game_id"].unique().to_list() == [401757277]


def test_overtime_drives_carry_no_pace_clock(plays):
    no_ot_clock = plays.with_columns(
        pl.when(pl.col("period") > 4)
        .then(None)
        .otherwise(pl.col("drive.timeElapsed.displayValue"))
        .alias("drive.timeElapsed.displayValue")
    )
    got = tendencies(plays, league="cfb").select("pos_team", *PACE, "sec_per_play")
    want = tendencies(no_ot_clock, league="cfb").select("pos_team", *PACE, "sec_per_play")
    assert got.equals(want)


def test_each_drive_clock_counts_once(plays):
    """Per game, the teams' clocked seconds sum to the distinct regulation drives' clocks."""
    s = _standing(plays).filter(pl.col("period") <= 4)
    drives = s.group_by("game_id", "drive.id").agg(
        secs=_secs("drive.timeElapsed.displayValue").first(), n=pl.col("drive.offensivePlays").first()
    )
    want = drives.group_by("game_id").agg(pl.col("secs").sum(), pl.col("n").sum()).sort("game_id")
    t = tendencies(plays, league="cfb")
    games = plays.select("game_id", "pos_team").unique()
    got = (
        t.join(games, on="pos_team")
        .group_by("game_id")
        .agg(secs=pl.col("drive_seconds").sum(), n=pl.col("drive_plays").sum())
        .sort("game_id")
    )
    assert got["secs"].to_list() == want["secs"].to_list()
    assert got["n"].to_list() == want["n"].cast(pl.Float64).to_list()
