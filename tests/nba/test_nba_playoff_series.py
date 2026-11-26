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
    SERIES_BOX_STATS,
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


def _unplay(sched: pl.DataFrame, game_ids: list[int]) -> pl.DataFrame:
    """The schedule with these games not yet played (as ESPN lists them before tip-off)."""
    return sched.with_columns(
        pl.when(pl.col("game_id").is_in(game_ids))
        .then(False)
        .otherwise(pl.col("status_type_completed"))
        .alias("status_type_completed")
    )


# the NBA Finals stood 3-2 Oklahoma City after game 5; the WNBA Finals 3-0 Las Vegas after game 3
@pytest.mark.parametrize(("lg", "unplayed", "score"), [("nba", 2, (3, 2)), ("wnba", 1, (3, 0))])
def test_a_series_whose_leader_has_not_clinched_is_not_decided(lg: str, unplayed: int, score: tuple[int, int]) -> None:
    finals = _games(lg).filter(pl.col("round") == 4).sort("game_number")
    later = finals["game_id"].to_list()[-unplayed:]
    live = playoff_series_games(_unplay(_read(lg, "schedule"), later), league=lg).filter(pl.col("round") == 4)
    last = live.filter(pl.col("game_id") == later[-1]).row(0, named=True)
    assert last["winner_team_id"] is None
    assert (last["team_a_wins"], last["team_b_wins"]) == score
    assert not last["decided"] and last["series_winner_team_id"] is None
    assert live["decided"].unique().to_list() == [False]
    # a tied series has no leader and is not decided either (the NBA Finals at 3-3)
    if lg == "nba":
        tied = playoff_series_games(_unplay(_read(lg, "schedule"), later[-1:]), league=lg).filter(pl.col("round") == 4)
        assert tied["decided"].unique().to_list() == [False]


def test_without_espn_rounds_the_latest_series_is_the_final() -> None:
    """Older seasons carry type_abbreviation STD on every postseason game: no round number, and the season's
    latest-starting series takes the Finals' clinch rule (WNBA 2025: four wins)."""
    std = _read("wnba", "schedule").with_columns(pl.lit("STD").alias("type_abbreviation"))
    games = playoff_series_games(std, league="wnba")
    assert games["round"].null_count() == games.height and games["round_code"].null_count() == games.height
    last = games.group_by("series_id").agg(pl.all().last())
    assert last["decided"].all()  # the early rounds through plays-on, the Finals through the clinch
    assert last.filter(pl.col("series_id") == "2025-11-17")["series_winner_team_id"].to_list() == [17]
    finals = games.filter(pl.col("series_id") == "2025-11-17").sort("game_number")
    live = playoff_series_games(_unplay(std, finals["game_id"].to_list()[-1:]), league="wnba")
    assert live.filter(pl.col("series_id") == "2025-11-17")["decided"].unique().to_list() == [False]
    # a first-round series stays decided through its winner playing on, with no clinch rule for its round
    assert last.filter(pl.col("series_id") == "2025-14-17")["series_winner_team_id"].to_list() == [17]


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


def test_a_zero_minute_cameo_with_a_stat_counts_as_played() -> None:
    """ESPN lists some cameos at 0.0 minutes with a box count; the NBA fixture holds twelve rows like that (one
    scores 2). They count as games played, and their counts reach the totals."""
    box = _read("nba", "player_box")
    counts = pl.sum_horizontal([pl.col(c).fill_null(0) for c in SERIES_BOX_STATS.values()])
    played = (pl.col("did_not_play").fill_null(False) == False) & ((pl.col("minutes").fill_null(0) > 0) | (counts > 0))
    cameo = box.filter((pl.col("minutes").fill_null(0) <= 0) & played)
    assert cameo.height == 12 and cameo["points"].max() == 2
    players = playoff_series_players(_games("nba"), box)
    checked = 0
    for athlete in cameo.filter(pl.col("season_type") == 2)["athlete_id"].unique().to_list():
        rows = box.filter((pl.col("athlete_id") == athlete) & (pl.col("season_type") == 2) & played)
        for p in players.filter(pl.col("athlete_id") == athlete).iter_rows(named=True):
            assert p["reg_games"] == rows.height
            assert p["reg_pts_pg"] * p["reg_games"] == pytest.approx(rows["points"].sum())
            checked += 1
    assert checked > 0
    # a cameo in a series game is one of the player's series games
    po = cameo.filter(pl.col("season_type") == 3).row(0, named=True)
    series = _games("nba").filter(pl.col("game_id") == po["game_id"])["series_id"][0]
    row = players.filter((pl.col("series_id") == series) & (pl.col("athlete_id") == po["athlete_id"])).row(
        0, named=True
    )
    assert (
        row["games"]
        == box.filter(
            (pl.col("athlete_id") == po["athlete_id"])
            & pl.col("game_id").is_in(
                _games("nba").filter(pl.col("series_id") == series)["game_id"].cast(pl.Int32).implode()
            )
            & played
        ).height
    )


@pytest.mark.parametrize("lg", LEAGUES)
def test_a_box_row_listed_twice_counts_once(lg: str) -> None:
    games = _games(lg)
    team, player = _read(lg, "team_box"), _read(lg, "player_box")
    assert playoff_series_teams(games, pl.concat([team, team.head(5)])).equals(playoff_series_teams(games, team))
    assert playoff_series_players(games, pl.concat([player, player.head(50)])).equals(
        playoff_series_players(games, player)
    )
