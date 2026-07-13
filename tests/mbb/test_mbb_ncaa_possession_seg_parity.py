"""MBB parity: ``ncaa_mbb_possessions`` vs the bigballR R oracle.

Input is the oracle play_by_play renamed to the sdv-py contract — the exact
rows R's ``get_possessions`` (``all_functions.R:3686-3745``) consumed — so
this isolates the segmenter transform from scrape logic. The transform is
called ONCE over the concatenated multi-game frame, matching how the oracle
was generated: the ungrouped ``startEventType`` lag (``all_functions.R:3698``)
leaks across game boundaries by design and the parity assertions cover it.

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
    POSSESSIONS_SCHEMA,
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
    got = ncaa_mbb_possessions(pbp)
    exp = load_expected("possessions", POSSESSIONS_SCHEMA, LEAGUE)
    assert_frame_parity(got, exp, POSSESSIONS_SCHEMA)


def test_simple_parity(pbp: pl.DataFrame) -> None:
    got = ncaa_mbb_possessions(pbp, simple=True)
    exp = load_expected("possessions_simple", POSSESSIONS_SIMPLE_SCHEMA, LEAGUE)
    assert_frame_parity(got, exp, POSSESSIONS_SIMPLE_SCHEMA)


def test_start_event_type_leaks_across_games(pbp: pl.DataFrame) -> None:
    """The ungrouped lag (all_functions.R:3698) is ported faithfully."""
    got = ncaa_mbb_possessions(pbp)
    first_per_game = got.group_by("game_id", maintain_order=True).first()
    # Exactly one game (the first in pbp order) starts with a null; the other
    # games' first possessions inherit the previous game's last event type.
    assert first_per_game["start_event_type"].null_count() == 1


def test_return_as_pandas(pbp: pl.DataFrame) -> None:
    pdf = ncaa_mbb_possessions(pbp, simple=True, return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)
    assert list(pdf.columns) == list(POSSESSIONS_SIMPLE_SCHEMA)


def test_empty_input_carries_schema(pbp: pl.DataFrame) -> None:
    empty = pbp.head(0)
    assert ncaa_mbb_possessions(empty).schema == POSSESSIONS_SCHEMA
    assert ncaa_mbb_possessions(empty, simple=True).schema == POSSESSIONS_SIMPLE_SCHEMA
