"""WBB parity: ``ncaa_mbb_possessions`` vs the wbigballR R oracle.

STRICT full-frame parity, same as MBB: the segmenter is input-deterministic,
and the WBB clock corruption (wbigballR applies MBB halves math to
quarter-format pages) lives entirely in the INPUT pbp — the R oracle was
produced from those same corrupted rows, so outputs must still match
cell-for-cell. See ``tests/mbb/_bigballr_oracle.py`` (WBB_CLOCK_TAINTED)
for the input-side caveat.

One transform call over the concatenated multi-game frame, matching oracle
generation (ungrouped ``startEventType`` lag, ``all_functions.R:3698``).
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_possession_seg import (
    POSSESSIONS_SCHEMA,
    POSSESSIONS_SIMPLE_SCHEMA,
    ncaa_mbb_possessions,
)
from tests.mbb._bigballr_oracle import load_oracle_pbp
from tests.mbb.test_mbb_ncaa_possession_seg_parity import (
    assert_frame_parity,
    load_expected,
)

LEAGUE = "wbb"


@pytest.fixture(scope="module")
def pbp() -> pl.DataFrame:
    return load_oracle_pbp(LEAGUE)


def test_full_parity(pbp: pl.DataFrame) -> None:
    got = ncaa_mbb_possessions(pbp)
    exp = load_expected("possessions", POSSESSIONS_SCHEMA, LEAGUE)
    assert_frame_parity(got, exp, POSSESSIONS_SCHEMA)


def test_simple_parity(pbp: pl.DataFrame) -> None:
    got = ncaa_mbb_possessions(pbp, simple=True)
    exp = load_expected("possessions_simple", POSSESSIONS_SIMPLE_SCHEMA, LEAGUE)
    assert_frame_parity(got, exp, POSSESSIONS_SIMPLE_SCHEMA)


def test_start_event_type_leaks_across_games(pbp: pl.DataFrame) -> None:
    """Faithful ungrouped lag: exactly one null start_event_type frame-wide."""
    got = ncaa_mbb_possessions(pbp)
    first_per_game = got.group_by("game_id", maintain_order=True).first()
    assert first_per_game["start_event_type"].null_count() == 1
