"""MBB parity: the bigballR lineup-aggregation family vs the R oracle.

Input for ``ncaa_mbb_lineups`` is the oracle play_by_play renamed to the
sdv-py contract — the exact rows R's ``get_lineups`` consumed. The
lineup-consuming transforms (``ncaa_mbb_player_lineups``,
``ncaa_mbb_player_combos``, ``ncaa_mbb_on_off``) are fed the ORACLE lineups
frame (renamed/cast) so each transform's parity is isolated from the others.

All parity calls pass ``fix_tip_in=False`` — the R functions test the literal
``"Tip-In"`` while the pbp emits ``"Tip In"``, so faithful oracle equality
requires reproducing the bug (see ``RIM_TYPES_LITERAL``).

Row order is deterministic (dplyr emits groups in C-locale sorted key order;
combo/on-off enumeration order is fixed by the R source), so rows compare in
order without re-sorting. Floats compare via ``math.isclose`` with 1e-12
tolerances: both sides round identically (half-to-even), but R ``write.csv``
emits only 15 significant digits, which is not round-trip exact for the
UNROUNDED ``o_trans_pct``/``d_trans_pct`` columns.
"""

from __future__ import annotations

import math

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_lineups import (
    LINEUPS_COLUMNS,
    LINEUPS_RENAME,
    LINEUPS_TRANSITION_COLUMNS,
    ON_OFF_COLUMNS,
    STAT_COLUMNS,
    ncaa_mbb_lineups,
    ncaa_mbb_on_off,
    ncaa_mbb_player_combos,
    ncaa_mbb_player_lineups,
)
from tests.mbb._bigballr_oracle import load_oracle, load_oracle_pbp

LEAGUE = "mbb"
#: Max-MINS player in the oracle's own on_off rows (Status column).
ON_OFF_PLAYERS = ["KEATON.WAGLER"]

#: Combos oracle contract for the fixture invocation (n=2).
PLAYER_COMBOS_2_COLUMNS: tuple[str, ...] = ("team", "p1", "p2", *STAT_COLUMNS)

_STR_COLS = frozenset({"p1", "p2", "p3", "p4", "p5", "team", "status"})


def load_expected(name: str, columns: tuple[str, ...], league: str) -> pl.DataFrame:
    """Oracle CSV renamed via the module's shared R->snake map + cast."""
    exp = load_oracle(name, league)
    exp = exp.rename({k: v for k, v in LINEUPS_RENAME.items() if k in exp.columns})
    exp = exp.with_columns([pl.col(c).cast(pl.Float64) for c in columns if c not in _STR_COLS])
    return exp.select(list(columns))


def assert_frame_parity(got: pl.DataFrame, exp: pl.DataFrame, columns: tuple[str, ...]) -> None:
    assert got.columns == list(columns)
    assert got.height == exp.height, f"row count {got.height} != oracle {exp.height}"
    for col in columns:
        g, e = got[col].to_list(), exp[col].to_list()
        if col in _STR_COLS:
            assert g == e, f"column {col!r} diverges"
        else:
            bad = [
                (i, a, b) for i, (a, b) in enumerate(zip(g, e)) if not math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12)
            ]
            assert not bad, f"column {col!r} diverges: {bad[:5]}"


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
    """Oracle invocation is the Included=NA/Excluded=NA passthrough
    (player_lineups.csv is byte-identical to lineups.csv)."""
    got = ncaa_mbb_player_lineups(oracle_lineups)
    exp = load_expected("player_lineups", LINEUPS_COLUMNS, LEAGUE)
    assert_frame_parity(got, exp, LINEUPS_COLUMNS)


def test_player_lineups_filter_semantics(oracle_lineups: pl.DataFrame) -> None:
    player = ON_OFF_PLAYERS[0]
    on = ncaa_mbb_player_lineups(oracle_lineups, included=player)
    off = ncaa_mbb_player_lineups(oracle_lineups, excluded=player)
    p_cols = ["p1", "p2", "p3", "p4", "p5"]
    assert on.height + off.height == oracle_lineups.height
    assert (
        on.filter(
            pl.any_horizontal([pl.col(c) == player for c in p_cols]) == False  # noqa: E712
        ).height
        == 0
    )
    assert off.filter(pl.any_horizontal([pl.col(c) == player for c in p_cols])).height == 0


def test_player_combos_parity(oracle_lineups: pl.DataFrame) -> None:
    got = ncaa_mbb_player_combos(oracle_lineups, n=2)
    exp = load_expected("player_combos_2", PLAYER_COMBOS_2_COLUMNS, LEAGUE)
    assert_frame_parity(got, exp, PLAYER_COMBOS_2_COLUMNS)


def test_on_off_parity(oracle_lineups: pl.DataFrame) -> None:
    got = ncaa_mbb_on_off(ON_OFF_PLAYERS, oracle_lineups)
    exp = load_expected("on_off", ON_OFF_COLUMNS, LEAGUE)
    assert_frame_parity(got, exp, ON_OFF_COLUMNS)


def test_fix_tip_in_recovers_tip_ins(pbp: pl.DataFrame) -> None:
    """fix_tip_in=True counts the real "Tip In" vocabulary R never matches."""
    n_tip = pbp.filter(pl.col("event_type") == "Tip In").height
    fixed = ncaa_mbb_lineups(pbp, fix_tip_in=True)
    literal = ncaa_mbb_lineups(pbp, fix_tip_in=False)
    rim_fixed = fixed.select(pl.col("rima").sum() + pl.col("d_rima").sum()).item()
    rim_literal = literal.select(pl.col("rima").sum() + pl.col("d_rima").sum()).item()
    if n_tip > 0:
        assert rim_fixed > rim_literal
    else:
        assert rim_fixed == rim_literal


def test_on_off_unknown_player_raises(oracle_lineups: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="team not found"):
        ncaa_mbb_on_off(["NO.SUCH.PLAYER"], oracle_lineups)


def test_player_combos_invalid_n_raises(oracle_lineups: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="1 to 5"):
        ncaa_mbb_player_combos(oracle_lineups, n=0)


def test_return_as_pandas(pbp: pl.DataFrame, oracle_lineups: pl.DataFrame) -> None:
    pdf = ncaa_mbb_lineups(pbp, return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)
    assert list(pdf.columns) == list(LINEUPS_COLUMNS)
    odf = ncaa_mbb_on_off(ON_OFF_PLAYERS, oracle_lineups, return_as_pandas=True)
    assert isinstance(odf, pd.DataFrame)


def test_empty_input_carries_schema(pbp: pl.DataFrame) -> None:
    empty = ncaa_mbb_lineups(pbp.head(0))
    assert empty.height == 0
    assert empty.columns == list(LINEUPS_COLUMNS)
    empty_t = ncaa_mbb_lineups(pbp.head(0), include_transition=True)
    assert empty_t.columns == list(LINEUPS_TRANSITION_COLUMNS)
