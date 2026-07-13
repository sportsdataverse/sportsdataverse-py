"""MBB parity: bigballR get_player_stats / get_mins / get_team_stats vs oracle.

Input for both transforms is the oracle play_by_play renamed to the sdv-py
contract — the exact rows R's ``get_player_stats`` / ``get_team_stats``
consumed. The oracle CSVs were generated with ``multi.games = T``, so the
parity calls exercise the multi-game path (which internally runs the
per-game path first).

All parity calls pass ``fix_tip_in=False`` — the R functions test the
literal ``"Tip-In"`` while the pbp emits ``"Tip In"``, so faithful oracle
equality requires reproducing the bug.

Row order: dplyr ``summarise`` emits groups C-locale sorted — (Player, Team)
for player stats, (ID, Home, Away) then Team for team stats — and the ports
sort identically (the Utf8 ``game_id`` byte sort equals R's numeric ID sort
because all fixture ids are equal-width), so rows compare in order without
re-sorting. Floats compare via ``math.isclose`` 1e-12: both sides round with
R's ``fround`` semantics, and R ``write.csv``'s 15-significant-digit output
is round-trip exact for 3-rounded values.
"""

from __future__ import annotations

import math

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_stats_agg import (
    PLAYER_GAME_STATS_COLUMNS,
    PLAYER_GAME_STATS_SIMPLE_COLUMNS,
    PLAYER_STATS_COLUMNS,
    PLAYER_STATS_SIMPLE_COLUMNS,
    STATS_AGG_RENAME,
    TEAM_STATS_COLUMNS,
    TEAM_STATS_RENAME,
    TEAM_STATS_TRANSITION_COLUMNS,
    ncaa_mbb_player_stats,
    ncaa_mbb_team_stats,
)
from tests.mbb._bigballr_oracle import load_oracle, load_oracle_pbp

LEAGUE = "mbb"

_STR_COLS = frozenset({"player", "team", "game_id", "game_date", "home", "away"})


def load_expected(name: str, columns: tuple[str, ...], league: str, rename: dict[str, str]) -> pl.DataFrame:
    """Oracle CSV renamed via the module's shared R->snake map + cast."""
    exp = load_oracle(name, league)
    exp = exp.rename({k: v for k, v in rename.items() if k in exp.columns})
    if "game_id" in exp.columns and exp.schema["game_id"] != pl.Utf8:
        # R's ID is numeric; the sdv-py contract keeps game_id Utf8.
        exp = exp.with_columns(pl.col("game_id").cast(pl.Int64).cast(pl.Utf8))
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


def test_single_game_contract_and_sums(pbp: pl.DataFrame) -> None:
    """multi_games=False keeps the game keys; per-game counters sum to the
    multi-game counters (rates are recomputed, so only counters roll up)."""
    per_game = ncaa_mbb_player_stats(pbp, fix_tip_in=False)
    assert per_game.columns == list(PLAYER_GAME_STATS_COLUMNS)
    assert per_game["game_id"].n_unique() == pbp["game_id"].n_unique()
    multi = ncaa_mbb_player_stats(pbp, multi_games=True, fix_tip_in=False)
    rolled = per_game.group_by(["player", "team"]).agg(pl.col("pts").sum(), pl.len().alias("gp")).sort("player")
    joined = rolled.join(multi.select("player", "team", "gp", "pts"), on=["player", "team"], suffix="_m")
    assert joined.height == multi.height
    assert (joined["pts"] - joined["pts_m"]).abs().max() == 0.0
    assert joined.filter(pl.col("gp").cast(pl.Int64) != pl.col("gp_m")).height == 0

    simple = ncaa_mbb_player_stats(pbp, simple=True, fix_tip_in=False)
    assert simple.columns == list(PLAYER_GAME_STATS_SIMPLE_COLUMNS)


def test_fix_tip_in_recovers_tip_ins(pbp: pl.DataFrame) -> None:
    """fix_tip_in=True counts the real "Tip In" vocabulary R never matches."""
    n_tip = pbp.filter(pl.col("event_type") == "Tip In").height
    fixed = ncaa_mbb_player_stats(pbp, multi_games=True, fix_tip_in=True)
    literal = ncaa_mbb_player_stats(pbp, multi_games=True, fix_tip_in=False)
    if n_tip > 0:
        assert fixed["rima"].sum() > literal["rima"].sum()
    else:
        assert fixed["rima"].sum() == literal["rima"].sum()


def test_team_stats_transition_contract(pbp: pl.DataFrame) -> None:
    got = ncaa_mbb_team_stats(pbp, include_transition=True, fix_tip_in=False)
    assert got.columns == list(TEAM_STATS_TRANSITION_COLUMNS)
    assert got.height == 2 * pbp["game_id"].n_unique()


def test_return_as_pandas(pbp: pl.DataFrame) -> None:
    pdf = ncaa_mbb_player_stats(pbp, multi_games=True, return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)
    assert list(pdf.columns) == list(PLAYER_STATS_COLUMNS)
    tdf = ncaa_mbb_team_stats(pbp, return_as_pandas=True)
    assert isinstance(tdf, pd.DataFrame)
    assert list(tdf.columns) == list(TEAM_STATS_COLUMNS)


def test_empty_input_carries_schema(pbp: pl.DataFrame) -> None:
    empty = ncaa_mbb_player_stats(pbp.head(0), multi_games=True)
    assert empty.height == 0
    assert empty.columns == list(PLAYER_STATS_COLUMNS)
    empty_t = ncaa_mbb_team_stats(pbp.head(0))
    assert empty_t.height == 0
    assert empty_t.columns == list(TEAM_STATS_COLUMNS)
