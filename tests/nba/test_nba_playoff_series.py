"""BB-D43: playoff series from the schedule, and their team and player totals, on real captures.

Fixture (not synthetic; README in the directory): ``tests/fixtures/bb_playoff_series``, the 2025
NBA and WNBA ESPN schedule (postseason and play-in rows), postseason team boxes, and the player
boxes (regular season and postseason) of every player who appeared in the postseason. The gates:
every postseason game is in exactly one series, play-in games in none, and the series totals are
the sum of the game boxes.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.playoff_series import (
    SERIES_GAMES_SCHEMA,
    SERIES_PLAYERS_SCHEMA,
    SERIES_TEAMS_SCHEMA,
    playoff_series_games,
    playoff_series_players,
    playoff_series_teams,
    wins_to_clinch,
)
from sportsdataverse.nba.nba_playoff_series import nba_playoff_series

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "bb_playoff_series"
LEAGUES = ("nba", "wnba")


@cache
def _read(lg: str, name: str) -> pl.DataFrame:
    return pl.read_parquet(FIXTURES / f"{lg}_{name}_2025.parquet")


@cache
def _games(lg: str) -> pl.DataFrame:
    return playoff_series_games(_read(lg, "schedule"), league=lg)


@pytest.mark.parametrize("lg", LEAGUES)
def test_every_postseason_game_is_in_exactly_one_series_and_play_in_in_none(lg: str) -> None:
    sched = _read(lg, "schedule")
    games = _games(lg)
    postseason = set(sched.filter(pl.col("season_type") == 3)["game_id"].to_list())
    assert games["game_id"].n_unique() == games.height == len(postseason)
    assert set(games["game_id"].to_list()) == postseason
    play_in = set(sched.filter(pl.col("season_type") == 5)["game_id"].to_list())
    assert lg == "wnba" or play_in, "the NBA fixture carries the play-in games"
    assert not play_in & set(games["game_id"].to_list())
    # one pair of teams per series, and its games numbered 1..n by tip-off
    per = games.group_by("series_id").agg(
        pl.concat_list("home_team_id", "away_team_id").explode().unique().sort().alias("teams"),
        pl.col("game_number").sort().alias("numbers"),
    )
    assert per["teams"].list.len().to_list() == [2] * per.height
    for numbers in per["numbers"].to_list():
        assert numbers == list(range(1, len(numbers) + 1))


def test_nba_2025_bracket_and_finals() -> None:
    last = _games("nba").group_by("series_id").agg(pl.all().last())
    assert last.height == 15
    assert last.group_by("round").len().sort("round")["len"].to_list() == [8, 4, 2, 1]
    finals = last.filter(pl.col("round") == 4).row(0, named=True)
    # Oklahoma City (25) beat Indiana (11) in seven, OKC with home court
    assert finals["series_id"] == "2025-11-25"
    assert (finals["team_a_id"], finals["team_a_wins"], finals["team_b_wins"]) == (25, 4, 3)
    assert finals["decided"] and finals["series_winner_team_id"] == 25
    assert finals["round_name"] == "NBA Finals"
    assert last["decided"].all()


def test_wnba_2025_finals_sweep() -> None:
    last = _games("wnba").group_by("series_id").agg(pl.all().last())
    finals = last.filter(pl.col("round") == 4).row(0, named=True)
    assert (finals["series_winner_team_id"], finals["team_a_wins"], finals["team_b_wins"]) == (17, 4, 0)
    assert last["decided"].all()


@pytest.mark.parametrize("lg", LEAGUES)
def test_a_series_in_progress_is_not_decided(lg: str) -> None:
    sched = _read(lg, "schedule")
    finals = _games(lg).filter(pl.col("round") == 4).sort("game_number")
    last_id = finals["game_id"][-1]
    live = sched.with_columns(
        pl.when(pl.col("game_id") == last_id)
        .then(False)
        .otherwise(pl.col("status_type_completed"))
        .alias("status_type_completed")
    )
    row = playoff_series_games(live, league=lg).filter(pl.col("game_id") == last_id).row(0, named=True)
    assert row["winner_team_id"] is None
    assert not row["decided"] and row["series_winner_team_id"] is None
    assert row["team_a_wins"] + row["team_b_wins"] == finals.height - 1


@pytest.mark.parametrize("lg", LEAGUES)
def test_series_team_totals_are_the_sum_of_the_game_boxes(lg: str) -> None:
    games, box = _games(lg), _read(lg, "team_box")
    teams = playoff_series_teams(games, box)
    assert teams.schema == pl.Schema(SERIES_TEAMS_SCHEMA)
    assert teams.height == 2 * games["series_id"].n_unique()
    expected = (
        box.with_columns(pl.col("game_id", "team_id").cast(pl.Int64))
        .join(games.select("series_id", "game_id"), on="game_id")
        .group_by("series_id", "team_id")
        .agg(
            pl.col("team_score").sum().alias("pts"),
            pl.col("total_rebounds").sum().alias("reb"),
            pl.col("assists").sum().alias("ast"),
        )
    )
    got = teams.join(expected, on=["series_id", "team_id"], suffix="_box")
    assert got.height == teams.height
    for c in ("pts", "reb", "ast"):
        assert (got[c] == got[f"{c}_box"]).all(), c
    # the series' points are the schedule's scores, and the W-L its final series score
    sched_pts = (
        pl.concat(
            [
                games.select("series_id", pl.col("home_team_id").alias("team_id"), pl.col("home_score").alias("p")),
                games.select("series_id", pl.col("away_team_id").alias("team_id"), pl.col("away_score").alias("p")),
            ]
        )
        .group_by("series_id", "team_id")
        .agg(pl.col("p").sum())
    )
    assert teams.join(sched_pts, on=["series_id", "team_id"]).filter(pl.col("pts") != pl.col("p")).height == 0
    last = games.group_by("series_id").agg(pl.col("team_a_id", "team_a_wins").last())
    a = teams.join(last, left_on=["series_id", "team_id"], right_on=["series_id", "team_a_id"])
    assert a.height == last.height and (a["wins"] == a["team_a_wins"]).all()
    assert (teams["games"] == teams["wins"] + teams["losses"]).all()


@pytest.mark.parametrize("lg", LEAGUES)
def test_series_player_totals_are_the_sum_of_the_game_boxes(lg: str) -> None:
    games, box = _games(lg), _read(lg, "player_box")
    players = playoff_series_players(games, box)
    assert players.schema == pl.Schema(SERIES_PLAYERS_SCHEMA)
    assert players.select("series_id", "athlete_id").is_unique().all()
    # the players add up to their team's series points (ESPN's player and team boxes agree)
    team = playoff_series_teams(games, _read(lg, "team_box")).select("series_id", "team_id", "pts")
    summed = players.group_by("series_id", "team_id").agg(pl.col("pts").sum().alias("player_pts"))
    joined = team.join(summed, on=["series_id", "team_id"])
    assert joined.height == team.height
    assert (joined["pts"] == joined["player_pts"]).all()
    # one player recounted by hand from the raw box rows
    top = players.sort("pts", descending=True).row(0, named=True)
    rows = box.filter((pl.col("athlete_id") == top["athlete_id"]) & (pl.col("minutes").fill_null(0) > 0)).with_columns(
        pl.col("game_id").cast(pl.Int64)
    )
    po = rows.join(games.filter(pl.col("series_id") == top["series_id"]).select("game_id"), on="game_id")
    reg = rows.filter(pl.col("season_type") == 2)
    assert (top["games"], top["pts"]) == (po.height, po["points"].sum())
    assert top["reg_games"] == reg.height
    assert top["reg_pts_pg"] == pytest.approx(reg["points"].sum() / reg.height)
    assert top["pts_pg_diff"] == pytest.approx(po["points"].sum() / po.height - reg["points"].sum() / reg.height)


def test_wins_to_clinch() -> None:
    assert wins_to_clinch("nba", 2025, final=False) == 4
    assert wins_to_clinch("nba", 2002, final=False) is None
    assert wins_to_clinch("nba", 2002, final=True) == 4
    assert wins_to_clinch("wnba", 2020, final=False) is None
    assert [wins_to_clinch("wnba", s, final=True) for s in (2004, 2005, 2024, 2025)] == [2, 3, 3, 4]


def test_empty_input_keeps_the_schema() -> None:
    empty = _read("nba", "schedule").filter(pl.col("season_type") == 2)
    games = playoff_series_games(empty, league="nba")
    assert games.height == 0 and games.schema == pl.Schema(SERIES_GAMES_SCHEMA)
    assert playoff_series_teams(games, _read("nba", "team_box")).height == 0
    assert playoff_series_players(games, _read("nba", "player_box")).height == 0


def test_bad_league_id_raises_before_any_load() -> None:
    with pytest.raises(ValueError, match="league_id"):
        nba_playoff_series(2025, league_id="20")
