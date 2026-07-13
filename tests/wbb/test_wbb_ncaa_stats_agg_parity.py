"""WBB parity: bigballR get_player_stats / get_team_stats vs the wbigballR oracle.

Same transforms as the MBB suite (the functions are league-agnostic); only
the oracle input pbp differs. The WBB clock corruption (wbigballR applies MBB
halves math to quarter pages) lives entirely in the INPUT columns
(``is_transition``, ``poss_num``, ``event_length``, lineups, ...), so parity
against the R output is STRICT here too — the transforms consumed exactly
these rows.

Shared helpers (oracle load + comparison + the R->snake maps) come from the
MBB suite; see ``tests/mbb/test_mbb_ncaa_stats_agg_parity.py``.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_stats_agg import (
    PLAYER_STATS_COLUMNS,
    PLAYER_STATS_SIMPLE_COLUMNS,
    STATS_AGG_RENAME,
    TEAM_STATS_COLUMNS,
    TEAM_STATS_RENAME,
    ncaa_mbb_player_stats,
    ncaa_mbb_team_stats,
)
from tests.mbb._bigballr_oracle import load_oracle_pbp
from tests.mbb.test_mbb_ncaa_stats_agg_parity import assert_frame_parity, load_expected

LEAGUE = "wbb"


@pytest.fixture(scope="module")
def pbp() -> pl.DataFrame:
    return load_oracle_pbp(LEAGUE)


def test_player_stats_parity(pbp: pl.DataFrame) -> None:
    got = ncaa_mbb_player_stats(pbp, multi_games=True, fix_tip_in=False)
    exp = load_expected("player_stats", PLAYER_STATS_COLUMNS, LEAGUE, STATS_AGG_RENAME)
    assert_frame_parity(got, exp, PLAYER_STATS_COLUMNS)


def test_player_stats_simple_parity(pbp: pl.DataFrame) -> None:
    got = ncaa_mbb_player_stats(pbp, multi_games=True, simple=True, fix_tip_in=False)
    exp = load_expected("player_stats_simple", PLAYER_STATS_SIMPLE_COLUMNS, LEAGUE, STATS_AGG_RENAME)
    assert_frame_parity(got, exp, PLAYER_STATS_SIMPLE_COLUMNS)


def test_team_stats_parity(pbp: pl.DataFrame) -> None:
    got = ncaa_mbb_team_stats(pbp, fix_tip_in=False)
    exp = load_expected("team_stats", TEAM_STATS_COLUMNS, LEAGUE, TEAM_STATS_RENAME)
    assert_frame_parity(got, exp, TEAM_STATS_COLUMNS)
