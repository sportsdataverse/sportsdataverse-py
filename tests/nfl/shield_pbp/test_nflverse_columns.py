"""The nflverse play_by_play columns the Shield builder publishes, checked against nflverse's own rows.

Oracle: ``tests/fixtures/nfl_shield/2024_01_BAL_KC_nflverse.parquet`` (the game's nflverse pbp rows), joined to
the built ``2024_01_BAL_KC`` on ``play_id``.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest
from sportsdataverse.nfl.shield_pbp.build import build_pbp

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "nfl_shield"
GAME = FIXTURES / "2024_01_BAL_KC.json.gz"
ORACLE = FIXTURES / "2024_01_BAL_KC_nflverse.parquet"
pytestmark = pytest.mark.skipif(not (GAME.exists() and ORACLE.exists()), reason="2024_01_BAL_KC fixtures not present")

STAT_FLAGS = [
    "third_down_converted",
    "third_down_failed",
    "fourth_down_converted",
    "fourth_down_failed",
    "punt_blocked",
    "punt_inside_twenty",
    "punt_in_endzone",
    "kickoff_inside_twenty",
    "kickoff_in_endzone",
    "kickoff_downed",
    "own_kickoff_recovery",
    "own_kickoff_recovery_td",
    "lateral_reception",
    "lateral_rush",
    "lateral_return",
    "lateral_recovery",
    "defensive_two_point_attempt",
    "defensive_two_point_conv",
    "defensive_extra_point_attempt",
    "defensive_extra_point_conv",
]
STAT_PLAYERS = [
    f"{role}_player_{part}"
    for role in (
        "own_kickoff_recovery",
        "lateral_receiver",
        "lateral_rusher",
        "lateral_sack",
        "lateral_interception",
        "lateral_punt_returner",
        "lateral_kickoff_returner",
    )
    for part in ("id", "name")
]
STATE = [
    "side_of_field",
    "yrdln",
    "time",
    "quarter_end",
    "home_timeouts_remaining",
    "away_timeouts_remaining",
    "total_home_score",
    "total_away_score",
    "ydsnet",
    "special_teams_play",
    "order_sequence",
]
POSSESSION = ["posteam_type", "posteam_score_post", "defteam_score_post"]
DESCRIPTION = ["touchback", "out_of_bounds", "replay_or_challenge", "replay_or_challenge_result", "penalty_type"]
FEED = [
    "play_clock",
    "play_deleted",
    "time_of_day",
    "end_clock_time",
    "drive_real_start_time",
    "st_play_type",
    "end_yard_line",
]


@pytest.fixture(scope="module")
def joined() -> pl.DataFrame:
    built = build_pbp(json.loads(gzip.open(GAME).read()), game_id="2024_01_BAL_KC")
    oracle = pl.read_parquet(ORACLE).with_columns(pl.col("play_id").cast(pl.Int64))
    cols = STAT_FLAGS + STAT_PLAYERS + STATE + POSSESSION + DESCRIPTION + FEED + ["posteam"]
    return built.select(["play_id", *cols]).join(
        oracle.select(["play_id", *[pl.col(c).alias(f"nv_{c}") for c in cols]]), on="play_id", how="inner"
    )


def _mismatches(df: pl.DataFrame, cols: list[str]) -> dict[str, int]:
    """Per column, the joined rows where the built value differs from nflverse's (two nulls agree)."""
    out = {}
    for c in cols:
        a, b = (pl.col(x).cast(pl.Utf8).str.replace(r"\.0$", "") for x in (c, f"nv_{c}"))
        same = (a == b).fill_null(False) | (pl.col(c).is_null() & pl.col(f"nv_{c}").is_null())
        out[c] = df.filter(same == False).height  # noqa: E712
    return {c: n for c, n in out.items() if n}


def test_every_play_joins(joined):
    assert joined.height == 168  # the builder drops the timeout rows


def test_stat_id_flags_match_nflverse(joined):
    # nflverse leaves the stat columns null on the rows with no stats (game start, quarter ends) where the builder
    # publishes 0, as it does for every other stat flag
    stated = joined.filter(pl.col("nv_third_down_converted").is_not_null())
    assert stated.height == 161
    assert _mismatches(stated, STAT_FLAGS + STAT_PLAYERS) == {}
    # String even in a game without a lateral, so seasons stack without a Null column
    assert {joined.schema[c] for c in STAT_PLAYERS} == {pl.Utf8}
    assert stated["third_down_converted"].sum() == 11 and stated["punt_inside_twenty"].sum() == 3


def test_game_state_columns_match_nflverse(joined):
    assert _mismatches(joined, STATE) == {}
    # 'MID 50', and a timeout's empty yard line filled from the next play while its side stays null
    assert joined.filter(pl.col("yrdln") == "MID 50").height > 0


def test_post_play_score_matches_where_the_possession_team_does(joined):
    same_ball = joined.filter(pl.col("posteam").eq_missing(pl.col("nv_posteam")))
    assert joined.height - same_ball.height == 1  # END QUARTER 2: nflfastR clears its posteam
    assert _mismatches(same_ball, POSSESSION) == {}


def test_description_flags_match_nflverse(joined):
    assert _mismatches(joined, DESCRIPTION) == {}
    assert joined["replay_or_challenge_result"].drop_nulls().to_list() == ["reversed"]
    assert joined["penalty_type"].drop_nulls().n_unique() > 3


def test_feed_columns_match_nflverse(joined):
    assert _mismatches(joined, FEED) == {}
