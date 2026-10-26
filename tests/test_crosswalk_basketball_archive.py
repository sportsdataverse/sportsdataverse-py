"""Offline archive builders for the NBA / WNBA crosswalks, on real trimmed inputs.

Fixtures: ``tests/fixtures/crosswalk_basketball_archive/`` (provenance in its README):
2015 NBA Finals games 5-6 and 2015 WNBA Finals game 5, from the stats archives and
the ESPN schedule / game-roster parquet.
"""

from __future__ import annotations

import json
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


def test_wnba_archive_crosswalks_have_no_yahoo_ids_by_default() -> None:
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


# The real 2015 NBA All-Star game (ESPN 400606285, East 31 v West 32) and LeBron's roster
# row in it, copied from hoopR-nba-data schedules / game_rosters 2015. ESPN types this
# game "STD", so only the game graph can tell it apart from a franchise game.
_ALL_STAR_GAME = {
    "game_id": 400606285,
    "game_date": date(2015, 2, 15),
    "home_id": 31,
    "away_id": 32,
    "home_abbreviation": "EAST",
    "home_display_name": "Eastern Conf All-Stars",
    "home_short_display_name": "EAST",
    "home_location": "Eastern Conf",
    "home_name": "All-Stars",
    "away_abbreviation": "WEST",
    "away_display_name": "Western Conf All-Stars",
    "away_short_display_name": "WEST",
    "away_location": "Western Conf",
    "away_name": "All-Stars",
}
_ALL_STAR_ROSTER = {
    "game_id": "400606285",
    "team_id": 31,
    "team_abbreviation": "EAST",
    "athlete_id": 1966,
    "athlete_display_name": "LeBron James",
    "athlete_jersey": "23",
    "athlete_position": "F",
}


def test_exhibition_teams_leave_team_and_player_but_keep_their_games() -> None:
    schedule, rosters = _espn("nba")
    schedule = pl.concat([schedule, pl.DataFrame([_ALL_STAR_GAME], schema=schedule.schema)])
    rosters = pl.concat([rosters, pl.DataFrame([_ALL_STAR_ROSTER], schema=rosters.schema)])
    out = nba_crosswalks_from_archive(2015, schedule, rosters, F / "nba_stats" / "json")
    assert out["team"]["espn_team_id"].to_list() == [5, 9]
    assert set(out["player"]["espn_team_id"].to_list()) == {5, 9}
    assert "400606285" in out["schedule"]["espn_game_id"].to_list()


# Real-shaped Fox frames: the columns and the ids fox_nba_teams() / fox_nba_team_roster()
# returned on 2026-10-11 (Fox rosters are today's, so Curry is on Golden State, Cleveland's
# row is a player who was not on the 2015 Cavaliers).
_FOX_TEAMS = pl.DataFrame(
    {
        "fox_team_id": ["11", "21"],
        "fox_team_name": ["Cleveland Cavaliers", "Golden State Warriors"],
        "fox_section": ["2026-27 NBA PRESEASON STANDINGS"] * 2,
    }
)
_FOX_ROSTER_COLUMNS = ["team_id", "position_group", "player", "pos", "age", "ht", "wt", "school", "athlete_id"]
_FOX_ROSTERS = {
    "11": [("11", "GUARD", "James Harden", "PG", "37", "6'5\"", "220 lbs", "Arizona State", "1569")],
    "21": [("21", "GUARD", "Stephen Curry", "PG", "38", "6'2\"", "200 lbs", "Davidson", "1550")],
}


def _stub_fox(monkeypatch: pytest.MonkeyPatch, current: int, *, fail: bool = False) -> None:
    def roster(team_id: str, **_: object) -> pl.DataFrame:
        if fail:
            raise ConnectionError("fox down")
        return pl.DataFrame(_FOX_ROSTERS[str(team_id)], schema=_FOX_ROSTER_COLUMNS, orient="row")

    def segment(path: str, params: object = None, **_: object) -> dict:
        if fail:
            raise ConnectionError("fox down")
        return json.loads((F / f"fox_nba_scores_segment_{path.rsplit('/', 1)[-1]}.json").read_text())

    monkeypatch.setattr("sportsdataverse.nba.nba_schedule.most_recent_nba_season", lambda: current)
    monkeypatch.setattr("sportsdataverse.nba.nba_fox_ext.fox_nba_teams", lambda **_: _FOX_TEAMS)
    monkeypatch.setattr("sportsdataverse.nba.nba_fox_ext.fox_nba_team_roster", roster)
    monkeypatch.setattr("sportsdataverse._fox_layout.fox_get", segment)


def _fox_ids(out: dict) -> tuple[dict, dict]:
    team = dict(zip(out["team"]["espn_team_id"], out["team"]["fox_team_id"]))
    player = dict(zip(out["player"]["espn_athlete_id"], out["player"]["fox_athlete_id"]))
    return team, player


def test_fox_legs_fill_teams_and_current_season_players(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_fox(monkeypatch, current=2015)
    schedule, rosters = _espn("nba")
    out = nba_crosswalks_from_archive(2015, schedule, rosters, F / "nba_stats" / "json", fox=True)
    team, player = _fox_ids(out)
    assert team == {5: "11", 9: "21"}
    assert out["team"]["fox_team_name"].to_list() == ["Cleveland Cavaliers", "Golden State Warriors"]
    assert player["3975"] == "1550"  # Stephen Curry
    assert player["1966"] is None  # LeBron is not on today's Fox Cavaliers roster
    curry = out["player"].filter(pl.col("espn_athlete_id") == "3975")
    assert curry.select("fox_player", "fox_position_group").row(0) == ("Stephen Curry", "GUARD")
    assert curry["nba_player_id"].to_list() == ["201939"]  # the Stats match is untouched
    assert out["player"].width == PLAYER_COLUMNS
    assert out["player"].schema["fox_athlete_id"] == pl.Utf8


def test_fox_leg_fills_game_ids_for_a_past_season(monkeypatch: pytest.MonkeyPatch) -> None:
    # Fox serves NBA scoreboards back to 1947; the 2015 Finals segments are real captures.
    _stub_fox(monkeypatch, current=2027)
    schedule, rosters = _espn("nba")
    out = nba_crosswalks_from_archive(2015, schedule, rosters, F / "nba_stats" / "json", fox_games=True)
    games = out["schedule"].select("espn_game_id", "fox_game_id", "fox_home_team_id", "fox_away_team_id").rows()
    assert sorted(games) == [("400796849", "102609", "21", "11"), ("400796850", "102824", "11", "21")]


def test_fox_players_stay_null_for_a_past_season(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_fox(monkeypatch, current=2027)
    schedule, rosters = _espn("nba")
    out = nba_crosswalks_from_archive(2015, schedule, rosters, F / "nba_stats" / "json", fox=True)
    team, player = _fox_ids(out)
    assert team == {5: "11", 9: "21"}
    assert set(player.values()) == {None}


def test_fox_off_by_default_and_failures_leave_columns_null(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    schedule, rosters = _espn("nba")
    _stub_fox(monkeypatch, current=2015)
    off = nba_crosswalks_from_archive(2015, schedule, rosters, F / "nba_stats" / "json")
    assert off["team"]["fox_team_id"].null_count() == 2

    _stub_fox(monkeypatch, current=2015, fail=True)
    out = nba_crosswalks_from_archive(2015, schedule, rosters, F / "nba_stats" / "json", fox=True)
    team, player = _fox_ids(out)
    assert team == {5: "11", 9: "21"}
    assert set(player.values()) == {None}
    assert "Fox rosters unavailable" in caplog.text
    assert out["schedule"]["fox_game_id"].null_count() == 2  # fox alone fetches no games
    out = nba_crosswalks_from_archive(2015, schedule, rosters, F / "nba_stats" / "json", fox_games=True)
    assert out["schedule"]["fox_game_id"].null_count() == 2
    assert "Fox games unavailable" in caplog.text

    def down(**_: object) -> pl.DataFrame:
        raise ConnectionError("fox down")

    monkeypatch.setattr("sportsdataverse.nba.nba_fox_ext.fox_nba_teams", down)
    out = nba_crosswalks_from_archive(2015, schedule, rosters, F / "nba_stats" / "json", fox=True)
    assert out["team"]["fox_team_id"].null_count() == 2
    assert "Fox teams unavailable" in caplog.text


def _stub_yahoo(monkeypatch: pytest.MonkeyPatch, *, teams_down: bool = False, ids_down: bool = False) -> None:
    def teams(**_: object) -> dict:
        if teams_down:
            raise ConnectionError("yahoo down")
        return json.loads((F / "yahoo_wnba_league_teams.json").read_text())

    def game_ids(**kw: object) -> dict:
        if ids_down:
            raise ConnectionError("yahoo down")
        assert kw["season"] == "2015"
        return json.loads((F / "yahoo_wnba_game_ids_20151014.json").read_text())

    monkeypatch.setattr("sportsdataverse.providers.yahoo.yahoo_shangrila.yahoo_league_teams", teams)
    monkeypatch.setattr("sportsdataverse.providers.yahoo.yahoo_shangrila.yahoo_league_game_ids_by_date", game_ids)


def test_wnba_yahoo_leg_fills_team_and_listed_game_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_yahoo(monkeypatch)
    schedule, rosters = _espn("wnba")
    out = wnba_crosswalks_from_archive(2015, schedule, rosters, F / "wnba_stats" / "json", yahoo=True)
    team = out["team"].select("espn_team_id", "yahoo_team_id", "yahoo_team_abbreviation", "yahoo_team_name")
    assert team.sort("espn_team_id").rows() == [
        (5, "wnba.t.13", "IND", "Indiana Fever"),
        (8, "wnba.t.11", "MIN", "Minnesota Lynx"),
    ]
    assert out["schedule"]["yahoo_game_id"].to_list() == ["wnba.g.20151014011"]
    assert out["team"].columns == wnba_crosswalk.TEAM_COLUMNS
    assert out["schedule"].columns == wnba_crosswalk.SCHEDULE_COLUMNS


def test_wnba_yahoo_leg_fails_soft(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    schedule, rosters = _espn("wnba")
    _stub_yahoo(monkeypatch, ids_down=True)  # id list down: unverified home-side id kept
    out = wnba_crosswalks_from_archive(2015, schedule, rosters, F / "wnba_stats" / "json", yahoo=True)
    assert out["schedule"]["yahoo_game_id"].to_list() == ["wnba.g.20151014011"]
    _stub_yahoo(monkeypatch, teams_down=True)
    out = wnba_crosswalks_from_archive(2015, schedule, rosters, F / "wnba_stats" / "json", yahoo=True)
    assert out["team"]["yahoo_team_id"].null_count() == 2
    assert out["schedule"]["yahoo_game_id"].null_count() == 1
    assert "Yahoo teams unavailable" in caplog.text
