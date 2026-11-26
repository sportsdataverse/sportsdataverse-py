"""BB-D43 for MBB / WBB: college tournament runs from the schedule, and their totals, on real captures.

Fixture (not synthetic; README in the directory): ``tests/fixtures/bb_tournament_runs``, the 2024-25
men's and women's ESPN schedule rows with a ``tournament_id`` or a postseason type (every tournament
game, the early-season events among them), the team boxes of those games, and every player box row
(regular season and tournaments) of the four Final Four teams of each tournament. The gates: every
tournament game is in exactly one tournament and in one run per side, early-season events in none,
and the run totals are the sum of the game boxes.
"""

from __future__ import annotations

import datetime as dt
from functools import cache
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.tournament_runs import (
    CONFERENCE_TOURNAMENT_FROM,
    RUN_GAMES_SCHEMA,
    RUN_PLAYERS_SCHEMA,
    RUN_TEAMS_SCHEMA,
    tournament_run_games,
    tournament_run_players,
    tournament_run_teams,
)
from sportsdataverse.mbb.mbb_tournament_runs import mbb_tournament_runs

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "bb_tournament_runs"
LEAGUES = ("mbb", "wbb")
#: The 2025 NCAA champions (ESPN ids): Florida and UConn; each won six games.
NCAA_CHAMPION = {"mbb": 57, "wbb": 41}


@cache
def _read(lg: str, name: str) -> pl.DataFrame:
    return pl.read_parquet(FIXTURES / f"{lg}_{name}_2025.parquet")


@cache
def _games(lg: str) -> pl.DataFrame:
    return tournament_run_games(_read(lg, "schedule"))


def _expected_games(sched: pl.DataFrame) -> set[int]:
    """The tournament games, recounted from the rule: postseason, or a conference tournament game."""
    conference = (
        (pl.col("season_type") == 2)
        & pl.col("tournament_id").is_not_null()
        & (pl.col("groups_is_conference") == True)
        & (pl.col("game_date") >= dt.date(2025, 2, CONFERENCE_TOURNAMENT_FROM))
    )
    return set(sched.filter((pl.col("season_type") == 3) | conference)["game_id"].to_list())


@pytest.mark.parametrize("lg", LEAGUES)
def test_every_tournament_game_is_in_one_tournament_and_one_run_per_side(lg: str) -> None:
    games = _games(lg)
    assert games.schema == pl.Schema(RUN_GAMES_SCHEMA)
    assert set(games["game_id"].to_list()) == _expected_games(_read(lg, "schedule"))
    per = games.group_by("game_id").agg(
        pl.len().alias("rows"),
        pl.col("tournament_key").n_unique().alias("keys"),
        pl.col("team_id").n_unique().alias("teams"),
    )
    assert per["rows"].unique().to_list() == [2] and per["keys"].unique().to_list() == [1]
    assert per["teams"].unique().to_list() == [2]
    assert games.select("game_id", "team_id").is_unique().all()
    # each run is one team in one tournament, numbered 1..n by tip-off
    runs = games.group_by("run_id").agg(
        pl.col("team_id").n_unique(), pl.col("tournament_key").n_unique(), pl.col("game_number").sort()
    )
    assert runs["team_id"].unique().to_list() == [1] and runs["tournament_key"].unique().to_list() == [1]
    for numbers in runs["game_number"].to_list():
        assert numbers == list(range(1, len(numbers) + 1))


@pytest.mark.parametrize("lg", LEAGUES)
def test_early_season_events_are_not_runs(lg: str) -> None:
    sched = _read(lg, "schedule")
    early = sched.filter(
        (pl.col("season_type") == 2)
        & pl.col("tournament_id").is_not_null()
        & (pl.col("game_date") < dt.date(2025, 2, 1))
    )
    assert early.height > 50  # the fixture keeps them, so the rule is exercised
    # some of them carry a conference group (the reason for the date rule)
    assert early.filter(pl.col("groups_is_conference") == True).height > 0
    assert not set(early["game_id"].to_list()) & set(_games(lg)["game_id"].to_list())


@pytest.mark.parametrize("lg", LEAGUES)
def test_the_ncaa_champion_and_the_kinds(lg: str) -> None:
    last = _games(lg).group_by("run_id").agg(pl.all().last())
    ncaa = last.filter(pl.col("tournament_kind") == "ncaa")
    assert ncaa.height == 68 and ncaa["tournament_key"].unique().to_list() == ["22"]
    champ = ncaa.filter(pl.col("run_result") == "champion").row(0, named=True)
    assert (champ["team_id"], champ["wins"], champ["losses"]) == (NCAA_CHAMPION[lg], 6, 0)
    assert ncaa.filter(pl.col("run_result") == "champion").height == 1
    assert ncaa.filter(pl.col("run_result") == "eliminated").height == 67
    # a tournament has one champion when its last listed date holds one game (its final), else none: the 2025
    # women's release stops on 2025-03-10 (fixture README), so 11 conference tournaments and the WBIT end at a
    # round of several games and name no champion; every men's tournament is complete
    tournaments = last.group_by("tournament_key").agg((pl.col("run_result") == "champion").sum().alias("champions"))
    one_final = (
        _games(lg)
        .unique("game_id")
        .filter(pl.col("game_date") == pl.col("game_date").max().over("tournament_key"))
        .group_by("tournament_key")
        .agg((pl.len() == 1).alias("final"))
    )
    j = tournaments.join(one_final, on="tournament_key", validate="1:1")
    assert (j["champions"] == j["final"].cast(pl.UInt32)).all()
    assert j.filter(pl.col("final") == False).height == (0 if lg == "mbb" else 12)
    conference = last.filter(pl.col("tournament_kind") == "conference")
    assert conference["conference_id"].null_count() == 0
    assert conference["tournament_key"].n_unique() == (31 if lg == "mbb" else 21)
    postseason = sorted(last.filter(pl.col("tournament_kind") == "postseason")["tournament_name"].unique().to_list())
    expected = ["College Basketball Crown", "NIT", "Purple CBI"] if lg == "mbb" else ["WBIT", "Women's NIT"]
    assert postseason == expected


@pytest.mark.parametrize("lg", LEAGUES)
def test_with_the_final_unplayed_nobody_is_champion(lg: str) -> None:
    sched = _read(lg, "schedule")
    final = _games(lg).filter((pl.col("tournament_kind") == "ncaa") & (pl.col("round_name") == "National Championship"))
    final_id = final["game_id"][0]
    live = sched.with_columns(
        pl.when(pl.col("game_id") == final_id)
        .then(False)
        .otherwise(pl.col("status_type_completed"))
        .alias("status_type_completed")
    )
    last = (
        tournament_run_games(live).filter(pl.col("tournament_kind") == "ncaa").group_by("run_id").agg(pl.all().last())
    )
    assert last.filter(pl.col("run_result") == "champion").height == 0
    alive = last.filter(pl.col("run_result") == "alive")
    assert sorted(alive["team_id"].to_list()) == sorted(final["team_id"].to_list())
    assert alive["wins"].to_list() == [5, 5] and alive["losses"].to_list() == [0, 0]


def test_an_untagged_game_joins_its_tagged_tournament_or_keys_on_its_event_name() -> None:
    """Older seasons leave the tournament_id off some games: with the NIT's first half untagged they rejoin
    id 21 by the event name; with the CBI wholly untagged its key is the event name's slug."""
    sched = _read("mbb", "schedule")
    nit = sched.filter(pl.col("tournament_id") == 21).sort("game_date")["game_id"].to_list()
    untagged = sched.with_columns(
        pl.when(pl.col("game_id").is_in(nit[: len(nit) // 2]) | (pl.col("tournament_id") == 11))
        .then(None)
        .otherwise(pl.col("tournament_id"))
        .alias("tournament_id")
    )
    games = tournament_run_games(untagged)
    assert games.filter(pl.col("game_id").is_in(nit))["tournament_key"].unique().to_list() == ["21"]
    cbi = games.filter(pl.col("tournament_name") == "Purple CBI")
    assert cbi["tournament_key"].unique().to_list() == ["purple-cbi"]
    assert cbi["game_id"].n_unique() == _games("mbb").filter(pl.col("tournament_key") == "11")["game_id"].n_unique()


@pytest.mark.parametrize("lg", LEAGUES)
def test_run_team_totals_are_the_sum_of_the_game_boxes(lg: str) -> None:
    games, box = _games(lg), _read(lg, "team_box")
    teams = tournament_run_teams(games, box)
    assert teams.schema == pl.Schema(RUN_TEAMS_SCHEMA)
    assert teams.height == games["run_id"].n_unique()
    # each run carries its tournament's name and kind, the ones on every one of its game rows
    names = games.group_by("run_id").agg(
        pl.col("tournament_name").n_unique().alias("n_names"),
        pl.col("tournament_kind").n_unique().alias("n_kinds"),
        pl.col("tournament_name").first().alias("game_name"),
        pl.col("tournament_kind").first().alias("game_kind"),
    )
    assert (names["n_names"] == 1).all() and (names["n_kinds"] == 1).all()
    tagged = teams.join(names, on="run_id")
    assert tagged.height == teams.height and teams["tournament_kind"].null_count() == 0
    assert tagged["tournament_name"].eq_missing(tagged["game_name"]).all()
    assert tagged["tournament_kind"].eq_missing(tagged["game_kind"]).all()
    expected = (
        box.with_columns(pl.col("game_id", "team_id").cast(pl.Int64))
        .join(games.select("run_id", "game_id", "team_id"), on=["game_id", "team_id"])
        .group_by("run_id")
        .agg(
            pl.col("team_score").sum().alias("pts_box"),
            pl.col("total_rebounds").sum().alias("reb_box"),
            pl.len().alias("n"),
        )
    )
    got = teams.join(expected, on="run_id")
    assert got.height == teams.height
    assert (
        (got["pts"] == got["pts_box"]).all()
        and (got["reb"] == got["reb_box"]).all()
        and (got["games"] == got["n"]).all()
    )
    # the points are the schedule's scores, and the W-L the run's record after its last game
    scores = games.group_by("run_id").agg(
        pl.col("team_score").sum(), pl.col("wins").last().alias("w"), pl.col("losses").last().alias("l")
    )
    chk = teams.join(scores, on="run_id")
    assert (chk["pts"] == chk["team_score"]).all()
    assert (chk["wins"] == chk["w"]).all() and (chk["losses"] == chk["l"]).all()
    assert (teams["games"] == teams["wins"] + teams["losses"]).all()


@pytest.mark.parametrize("lg", LEAGUES)
def test_run_player_totals_are_the_sum_of_the_game_boxes(lg: str) -> None:
    games, box = _games(lg), _read(lg, "player_box")
    players = tournament_run_players(games, box)
    assert players.schema == pl.Schema(RUN_PLAYERS_SCHEMA)
    four = box["team_id"].unique().cast(pl.Int64).to_list()
    assert len(four) == 4
    # the players add up to their team's run points, in every run of the Final Four teams
    team = tournament_run_teams(games, _read(lg, "team_box")).filter(pl.col("team_id").is_in(four))
    summed = players.group_by("run_id").agg(pl.col("pts").sum().alias("player_pts"))
    joined = team.join(summed, on="run_id")
    assert joined.height == team.height and (joined["pts"] == joined["player_pts"]).all()
    # the champion's leading scorer, recounted: the regular season leaves out the conference tournament
    champ = games.filter((pl.col("tournament_kind") == "ncaa") & (pl.col("run_result") == "champion"))["run_id"][0]
    top = players.filter(pl.col("run_id") == champ).sort("pts", descending=True).row(0, named=True)
    conference = games.filter(pl.col("tournament_kind") == "conference")["game_id"].unique()
    rows = box.filter((pl.col("athlete_id") == top["athlete_id"]) & (pl.col("minutes").fill_null(0) > 0))
    reg = rows.filter(
        (pl.col("season_type") == 2) & (pl.col("game_id").cast(pl.Int64).is_in(conference.implode()) == False)
    )
    ncaa_ids = games.filter(pl.col("run_id") == champ)["game_id"]
    po = rows.filter(pl.col("game_id").cast(pl.Int64).is_in(ncaa_ids.implode()))
    assert (top["games"], top["pts"]) == (po.height, po["points"].sum()) and top["games"] == 6
    assert top["reg_games"] == reg.height
    assert top["reg_pts_pg"] == pytest.approx(reg["points"].sum() / reg.height)


def test_bad_league_raises_before_any_load() -> None:
    with pytest.raises(ValueError, match="league"):
        mbb_tournament_runs(2025, league="coed")


@pytest.mark.parametrize("lg", LEAGUES)
def test_a_release_without_the_final_names_no_champion(lg: str) -> None:
    """With the NCAA final missing, the last date holds the two semifinals: both winners are alive, not champion."""
    sched = _read(lg, "schedule")
    ncaa = _games(lg).filter(pl.col("tournament_kind") == "ncaa")
    final_id = ncaa.filter(pl.col("round_name") == "National Championship")["game_id"][0]
    last = (
        tournament_run_games(sched.filter(pl.col("game_id") != final_id))
        .filter(pl.col("tournament_kind") == "ncaa")
        .group_by("run_id")
        .agg(pl.all().last())
    )
    assert last.filter(pl.col("run_result") == "champion").height == 0
    assert last.filter(pl.col("run_result") == "alive").height == 2


@pytest.mark.parametrize("lg", LEAGUES)
def test_a_side_whose_team_is_not_named_yet_is_in_no_run(lg: str) -> None:
    """A bracket slot ESPN has not filled (null team id) is dropped, never pooled into one null run."""
    sched = _read(lg, "schedule")
    final_id = _games(lg).filter(pl.col("round_name") == "National Championship")["game_id"][0]
    tbd = sched.with_columns(
        pl.when(pl.col("game_id") == final_id).then(None).otherwise(pl.col("away_id")).alias("away_id")
    )
    games = tournament_run_games(tbd)
    assert games["run_id"].null_count() == 0 and games["team_id"].null_count() == 0
    assert games.filter(pl.col("game_id") == final_id).height == 1  # the named side only
    assert set(games["run_result"].unique().to_list()) <= {"champion", "eliminated", "alive"}
