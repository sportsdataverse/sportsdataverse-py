"""Tests for :func:`calculate_nfl_standings`.

A reduced, self-contained port of the win_pct -> head-to-head -> division
record -> conference record tiebreaker ladder nflfastR delegates entirely to
the external ``nflseedR`` package (``calculate_standings.R`` is a thin
dispatch/reshape wrapper with no tiebreaker logic of its own -- see reference
Sec 12). These tests inject a small synthetic ``teams`` frame (the
``load_nfl_teams`` shape: ``team_abbr`` / ``team_conf`` / ``team_division``)
so no network access is required.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.nfl import calculate_nfl_standings

# calculate_nfl_standings is deprecated in favor of nfl_season_standings; the
# behavior tests below still exercise the reduced ladder, so silence the
# DeprecationWarning here (a dedicated test asserts it still fires).
pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def _teams() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "team_abbr": ["T1", "T2", "T3", "T4"],
            "team_conf": ["T", "T", "T", "T"],
            "team_division": ["TEST", "TEST", "TEST", "TEST"],
        }
    )


def _games() -> pl.DataFrame:
    # A 4-team round robin (one division): T1 and T2 both finish 2-1 and tied
    # on win_pct, but T1 beat T2 head-to-head so T1 ranks ahead. T3 and T4 tie
    # each other (0.5 win apiece).
    rows = [
        {
            "game_id": "g1",
            "season": 2024,
            "game_type": "REG",
            "week": 1,
            "home_team": "T1",
            "away_team": "T2",
            "home_score": 24,
            "away_score": 17,
        },
        {
            "game_id": "g2",
            "season": 2024,
            "game_type": "REG",
            "week": 2,
            "home_team": "T1",
            "away_team": "T3",
            "home_score": 28,
            "away_score": 10,
        },
        {
            "game_id": "g3",
            "season": 2024,
            "game_type": "REG",
            "week": 3,
            "home_team": "T1",
            "away_team": "T4",
            "home_score": 14,
            "away_score": 21,
        },
        {
            "game_id": "g4",
            "season": 2024,
            "game_type": "REG",
            "week": 4,
            "home_team": "T2",
            "away_team": "T4",
            "home_score": 20,
            "away_score": 13,
        },
        {
            "game_id": "g5",
            "season": 2024,
            "game_type": "REG",
            "week": 5,
            "home_team": "T2",
            "away_team": "T3",
            "home_score": 17,
            "away_score": 6,
        },
        {
            "game_id": "g6",
            "season": 2024,
            "game_type": "REG",
            "week": 6,
            "home_team": "T3",
            "away_team": "T4",
            "home_score": 10,
            "away_score": 10,
        },
    ]
    return pl.DataFrame(rows)


def test_standings_two_way_tie_broken_by_head_to_head() -> None:
    df = calculate_nfl_standings(_games(), teams=_teams())

    def _row(team: str) -> dict:  # type: ignore[no-untyped-def]
        return df.filter(pl.col("team") == team).row(0, named=True)

    t1, t2, t3, t4 = _row("T1"), _row("T2"), _row("T3"), _row("T4")

    # T1 and T2 both go 2-1, tied on win_pct -- T1 beat T2 head-to-head.
    assert t1["wins"] == 2
    assert t1["losses"] == 1
    assert t1["ties"] == 0
    assert t1["win_pct"] == 2 / 3

    assert t2["wins"] == 2
    assert t2["losses"] == 1
    assert t2["ties"] == 0
    assert t2["win_pct"] == 2 / 3

    # T3 vs T4 tie counts as 0.5 win apiece.
    assert t3["wins"] == 0
    assert t3["losses"] == 2
    assert t3["ties"] == 1
    assert t3["win_pct"] == 0.5 / 3

    assert t4["wins"] == 1
    assert t4["losses"] == 1
    assert t4["ties"] == 1
    assert t4["win_pct"] == 0.5

    # Head-to-head breaks the T1/T2 tie: T1 ranks ahead of T2.
    assert t1["div_rank"] == 1
    assert t2["div_rank"] == 2
    assert t4["div_rank"] == 3
    assert t3["div_rank"] == 4

    # Single division/conference in this synthetic league -> seed mirrors div_rank.
    assert t1["seed"] == 1
    assert t2["seed"] == 2
    assert t4["seed"] == 3
    assert t3["seed"] == 4


def test_standings_empty_games_returns_zero_row_schema() -> None:
    empty = pl.DataFrame(
        schema={
            "game_id": pl.Utf8,
            "season": pl.Int64,
            "game_type": pl.Utf8,
            "week": pl.Int64,
            "home_team": pl.Utf8,
            "away_team": pl.Utf8,
            "home_score": pl.Int64,
            "away_score": pl.Int64,
        }
    )
    out = calculate_nfl_standings(empty, teams=_teams())
    assert out.height == 0
    assert "div_rank" in out.columns
    assert "seed" in out.columns


def test_standings_empty_games_teams_none_never_hits_network() -> None:
    # ``teams=None`` normally triggers a ``load_nfl_teams()`` network call, but
    # the empty-games short-circuit must fire first so this stays offline-safe.
    empty = pl.DataFrame(
        schema={
            "game_id": pl.Utf8,
            "season": pl.Int64,
            "game_type": pl.Utf8,
            "week": pl.Int64,
            "home_team": pl.Utf8,
            "away_team": pl.Utf8,
            "home_score": pl.Int64,
            "away_score": pl.Int64,
        }
    )
    out = calculate_nfl_standings(empty, teams=None)
    assert out.height == 0
    assert "div_rank" in out.columns
    assert "seed" in out.columns


def test_standings_return_as_pandas() -> None:
    import pandas as pd

    out = calculate_nfl_standings(_games(), teams=_teams(), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
    assert out.shape[0] == 4


@pytest.mark.filterwarnings("default::DeprecationWarning")
def test_standings_emits_deprecation_pointing_to_season_standings() -> None:
    with pytest.warns(DeprecationWarning, match="nfl_season_standings"):
        calculate_nfl_standings(_games(), teams=_teams())
