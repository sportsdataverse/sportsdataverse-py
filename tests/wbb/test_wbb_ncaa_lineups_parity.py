"""WBB parity: the bigballR lineup-aggregation family vs the wbigballR oracle.

Same transforms as the MBB suite (the functions are league-agnostic); the
oracle input pbp differs. The WBB clock corruption (wbigballR applies MBB
halves math to quarter pages) lives entirely in the INPUT columns
(``is_transition``, ``poss_num``, lineups, ...), so parity against the R
output is STRICT here too — the transforms consumed exactly these rows.

Shared helpers (oracle load + comparison + the R->snake map) come from the
MBB suite; see ``tests/mbb/test_mbb_ncaa_lineups_parity.py``.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_lineups import (
    LINEUPS_COLUMNS,
    LINEUPS_TRANSITION_COLUMNS,
    ON_OFF_COLUMNS,
    ncaa_mbb_lineups,
    ncaa_mbb_on_off,
    ncaa_mbb_player_combos,
    ncaa_mbb_player_lineups,
)
from tests.mbb._bigballr_oracle import load_oracle_pbp
from tests.mbb.test_mbb_ncaa_lineups_parity import (
    PLAYER_COMBOS_2_COLUMNS,
    assert_frame_parity,
    load_expected,
)

LEAGUE = "wbb"
#: Player specified by the task brief for the wbb on/off oracle (matches the
#: oracle's own Status rows).
ON_OFF_PLAYERS = ["HANNAH.HIDALGO"]


@pytest.fixture(scope="module")
def pbp() -> pl.DataFrame:
    return load_oracle_pbp(LEAGUE)


@pytest.fixture(scope="module")
def oracle_lineups() -> pl.DataFrame:
    return load_expected("lineups", LINEUPS_COLUMNS, LEAGUE)


def test_lineups_parity(pbp: pl.DataFrame) -> None:
    got = ncaa_mbb_lineups(pbp, fix_tip_in=False)
    exp = load_expected("lineups", LINEUPS_COLUMNS, LEAGUE)
    assert_frame_parity(got, exp, LINEUPS_COLUMNS)


def test_lineups_transition_parity(pbp: pl.DataFrame) -> None:
    got = ncaa_mbb_lineups(pbp, include_transition=True, fix_tip_in=False)
    exp = load_expected("lineups_transition", LINEUPS_TRANSITION_COLUMNS, LEAGUE)
    assert_frame_parity(got, exp, LINEUPS_TRANSITION_COLUMNS)


def test_player_lineups_parity(oracle_lineups: pl.DataFrame) -> None:
    """Oracle invocation is the Included=NA/Excluded=NA passthrough."""
    got = ncaa_mbb_player_lineups(oracle_lineups)
    exp = load_expected("player_lineups", LINEUPS_COLUMNS, LEAGUE)
    assert_frame_parity(got, exp, LINEUPS_COLUMNS)


def test_player_combos_parity(oracle_lineups: pl.DataFrame) -> None:
    got = ncaa_mbb_player_combos(oracle_lineups, n=2)
    exp = load_expected("player_combos_2", PLAYER_COMBOS_2_COLUMNS, LEAGUE)
    assert_frame_parity(got, exp, PLAYER_COMBOS_2_COLUMNS)


def test_on_off_parity(oracle_lineups: pl.DataFrame) -> None:
    got = ncaa_mbb_on_off(ON_OFF_PLAYERS, oracle_lineups)
    exp = load_expected("on_off", ON_OFF_COLUMNS, LEAGUE)
    assert_frame_parity(got, exp, ON_OFF_COLUMNS)
