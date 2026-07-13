"""MBB parity: ``ncaa_mbb_possessions`` vs the bigballR R oracle.

Input is the oracle play_by_play renamed to the sdv-py contract — the exact
rows R's ``get_possessions`` (``all_functions.R:3686-3745``) consumed — so
this isolates the segmenter transform from scrape logic. The transform is
called ONCE over the concatenated multi-game frame, matching how the oracle
was generated — and in FAITHFUL mode (``fix_cross_game_leak=False``), because
R's ungrouped ``startEventType`` lag (``all_functions.R:3698``) leaks across
game boundaries. The shipped default fixes that leak (BUG-4); parity is
asserted against the faithful mode, and the fixed-vs-faithful delta is pinned
by ``test_faithful_mode_leaks_across_games_fixed_mode_does_not``.

Row order is deterministic (dplyr emits groups sorted by the group keys; the
keys are unique per row), so rows are compared in order without re-sorting.
Oracle numerics are all integral (R's ``as.numeric`` round-trip never
produces fractions here), so comparison is exact — no float rounding needed.
"""

from __future__ import annotations

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_possession_seg import (
    POSSESSIONS_RENAME,
    POSSESSION_SEG_SCHEMA,
    POSSESSIONS_SIMPLE_SCHEMA,
    ncaa_mbb_possessions,
)
from tests.mbb._bigballr_oracle import load_oracle, load_oracle_pbp

LEAGUE = "mbb"


def load_expected(name: str, schema: pl.Schema, league: str) -> pl.DataFrame:
    """Oracle CSV renamed to snake_case + cast to the module's contract.

    The only dtype adjustments are R-artifact ones (e.g. ``ID`` read as
    Int64 -> ``game_id`` Utf8); values are compared exactly.
    """
    exp = load_oracle(name, league)
    exp = exp.rename({k: v for k, v in POSSESSIONS_RENAME.items() if k in exp.columns})
    exp = exp.with_columns([pl.col(c).cast(dtype) for c, dtype in schema.items()])
    return exp.select(list(schema))


def assert_frame_parity(got: pl.DataFrame, exp: pl.DataFrame, schema: pl.Schema) -> None:
    assert got.columns == list(schema)
    assert got.schema == schema
    assert got.height == exp.height, f"row count {got.height} != oracle {exp.height}"
    for col in got.columns:
        assert got[col].to_list() == exp[col].to_list(), f"column {col!r} diverges"


@pytest.fixture(scope="module")
def pbp() -> pl.DataFrame:
    return load_oracle_pbp(LEAGUE)


def test_full_parity(pbp: pl.DataFrame) -> None:
    got = ncaa_mbb_possessions(pbp, fix_cross_game_leak=False)
    exp = load_expected("possessions", POSSESSION_SEG_SCHEMA, LEAGUE)
    assert_frame_parity(got, exp, POSSESSION_SEG_SCHEMA)


def test_simple_parity(pbp: pl.DataFrame) -> None:
    got = ncaa_mbb_possessions(pbp, simple=True)
    exp = load_expected("possessions_simple", POSSESSIONS_SIMPLE_SCHEMA, LEAGUE)
    assert_frame_parity(got, exp, POSSESSIONS_SIMPLE_SCHEMA)


def test_faithful_mode_leaks_across_games_fixed_mode_does_not(pbp: pl.DataFrame) -> None:
    """BUG-4: the ungrouped lag (all_functions.R:3698) leaks; the fix windows it.

    Faithful mode (``fix_cross_game_leak=False``) reproduces R: only the very
    first row of the whole frame gets a null ``start_event_type``, so every
    OTHER game's possession #1 inherits the previous game's last event type.
    The default (fixed) mode nulls each game's first possession instead.
    """
    n_games = pbp["game_id"].n_unique()
    assert n_games > 1, "leak is only observable in a multi-game frame"

    faithful = ncaa_mbb_possessions(pbp, fix_cross_game_leak=False)
    first_faithful = faithful.group_by("game_id", maintain_order=True).first()
    assert first_faithful["start_event_type"].null_count() == 1

    fixed = ncaa_mbb_possessions(pbp)
    first_fixed = fixed.group_by("game_id", maintain_order=True).first()
    assert first_fixed["start_event_type"].null_count() == n_games
    # Nothing else moves: only start_event_type differs between the modes.
    assert fixed.drop("start_event_type").equals(faithful.drop("start_event_type"))


def test_return_as_pandas(pbp: pl.DataFrame) -> None:
    pdf = ncaa_mbb_possessions(pbp, simple=True, return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)
    assert list(pdf.columns) == list(POSSESSIONS_SIMPLE_SCHEMA)


def test_empty_input_carries_schema(pbp: pl.DataFrame) -> None:
    empty = pbp.head(0)
    assert ncaa_mbb_possessions(empty).schema == POSSESSION_SEG_SCHEMA
    assert ncaa_mbb_possessions(empty, simple=True).schema == POSSESSIONS_SIMPLE_SCHEMA
