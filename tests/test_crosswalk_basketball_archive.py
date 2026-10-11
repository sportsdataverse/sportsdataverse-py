"""Offline archive builders for the NBA / WNBA crosswalks, on real trimmed inputs.

Fixtures: ``tests/fixtures/crosswalk_basketball_archive/`` (provenance in its README):
2015 NBA Finals games 5-6 and 2015 WNBA Finals game 5, from the stats archives and
the ESPN schedule / game-roster parquet.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._crosswalk_basketball_archive import build_archive_crosswalks, read_stats_archive
from sportsdataverse.nba import nba_crosswalk, nba_crosswalks_from_archive
from sportsdataverse.wnba import wnba_crosswalk, wnba_crosswalks_from_archive

F = Path(__file__).parent / "fixtures" / "crosswalk_basketball_archive"
PLAYER_COLUMNS = 21


def _espn(league: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    return (
        pl.read_parquet(F / "espn" / f"{league}_schedule_2015.parquet"),
        pl.read_parquet(F / "espn" / f"{league}_game_rosters_2015.parquet"),
    )


def test_read_stats_archive_dates_from_summary_then_game_log() -> None:
    stats = read_stats_archive(F / "nba_stats" / "json", "nba", 2015)
    games = stats["games"].sort("nba_game_id")
    assert games.schema["game_date"] == pl.Date
    assert games["nba_game_id"].to_list() == ["0041400405", "0041400406"]
    # 405 has a boxscoresummaryv2 file; 406 does not, so its date comes from leaguegamelog.
    assert games["game_date"].to_list() == [date(2015, 6, 14), date(2015, 6, 16)]
    assert games["nba_game_code"].to_list() == ["20150614/CLEGSW", None]
    assert games["season_type"].unique().to_list() == ["Playoffs"]
    players = stats["players"]
    assert all(players.schema[c] == pl.Utf8 for c in players.columns)
    assert {"2544", "201939"} <= set(players["nba_player_id"].to_list())


def test_nba_archive_crosswalks_match_teams_games_and_players() -> None:
    schedule, rosters = _espn("nba")
    out = nba_crosswalks_from_archive(2015, schedule, rosters, F / "nba_stats" / "json")

    team = out["team"]
    assert team.columns == nba_crosswalk.TEAM_COLUMNS
    assert dict(zip(team["espn_team_id"], team["nba_team_id"])) == {5: "1610612739", 9: "1610612744"}
    assert team["match_method"].unique().to_list() == ["game_dates"]
    assert team["yahoo_team_id"].to_list() == ["nba.t.5", "nba.t.9"]

    sched = out["schedule"].sort("espn_game_id")
    assert sched.columns == nba_crosswalk.SCHEDULE_COLUMNS
    assert sched["match_method"].to_list() == ["both", "both"]
    assert sched["nba_game_id"].to_list() == ["0041400405", "0041400406"]
    assert sched["yahoo_game_id"].to_list() == ["nba.g.2015061409", "nba.g.2015061605"]

    player = out["player"]
    assert player.width == PLAYER_COLUMNS
    assert player.select("espn_team_id", "espn_athlete_id").is_duplicated().sum() == 0
    assert (player["match_method"] == "unmatched").sum() == 0
    ids = dict(zip(player["espn_athlete_id"], player["nba_player_id"]))
    assert ids["1966"] == "2544"  # LeBron James
    assert ids["3975"] == "201939"  # Stephen Curry
    for col in ("espn_athlete_id", "nba_player_id", "fox_athlete_id", "yahoo_player_id"):
        assert player.schema[col] == pl.Utf8


def test_unmatched_espn_game_leaves_the_stats_game_stats_only() -> None:
    schedule, rosters = _espn("nba")
    out = nba_crosswalks_from_archive(
        2015, schedule.filter(pl.col("game_id") != 400796850), rosters, F / "nba_stats" / "json"
    )
    sched = out["schedule"].sort("nba_game_id")
    assert sched["match_method"].to_list() == ["both", "stats_only"]
    assert sched["espn_game_id"].to_list() == ["400796849", None]


def test_wnba_archive_crosswalks_have_no_yahoo_ids() -> None:
    schedule, rosters = _espn("wnba")
    out = wnba_crosswalks_from_archive(2015, schedule, rosters, F / "wnba_stats" / "json")
    team = out["team"]
    assert team.columns == wnba_crosswalk.TEAM_COLUMNS
    assert dict(zip(team["espn_team_id"], team["wnba_team_tricode"])) == {5: "IND", 8: "MIN"}
    assert out["schedule"]["match_method"].to_list() == ["both"]
    assert out["schedule"]["yahoo_game_id"].null_count() == 1
    assert out["player"].width == PLAYER_COLUMNS
    assert (out["player"]["match_method"] == "exact_name").all()


def test_missing_season_raises() -> None:
    schedule, rosters = _espn("nba")
    with pytest.raises(FileNotFoundError, match="no NBA box scores for season 1990"):
        nba_crosswalks_from_archive(1990, schedule, rosters, F / "nba_stats" / "json")


def test_yahoo_ids_only_for_franchises_from_2014() -> None:
    frame = pl.DataFrame({"espn_team_id": [9, 31], "home_espn_team_id": [9, 31], "game_date": [date(2015, 6, 14)] * 2})
    got = frame.select(nba_crosswalk._yahoo_team_id(2015), nba_crosswalk._yahoo_game_id(2015))
    assert got.rows() == [("nba.t.9", "nba.g.2015061409"), (None, None)]
    early = frame.with_columns(nba_crosswalk._yahoo_team_id(2013), nba_crosswalk._yahoo_game_id(2013))
    assert early.select("yahoo_team_id", "yahoo_game_id").null_count().row(0) == (2, 2)


def test_season_vote_tie_is_left_unmatched() -> None:
    """LeBron carries a different Stats id in each Finals game: a 1-1 split must not pick one."""
    schedule, rosters = _espn("nba")
    stats = read_stats_archive(F / "nba_stats" / "json", "nba", 2015)
    lebron = pl.col("nba_player_id") == "2544"
    split = pl.when(lebron & (pl.col("nba_game_id") == "0041400406")).then(pl.lit("9999999"))
    stats["players"] = stats["players"].with_columns(split.otherwise(pl.col("nba_player_id")).alias("nba_player_id"))
    player = build_archive_crosswalks("nba", 2015, schedule, rosters, stats)["player"]
    row = player.filter(pl.col("espn_athlete_id") == "1966")
    assert row["nba_player_id"].to_list() == [None]
    assert row["match_method"].to_list() == ["unmatched"]
    ids = dict(zip(player["espn_athlete_id"], player["nba_player_id"]))
    assert ids["3975"] == "201939"  # Curry, matched in both games, is untouched
