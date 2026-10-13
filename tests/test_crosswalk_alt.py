"""Offline tests for sportsdataverse._crosswalk_alt on real stored payloads (tests/fixtures/crosswalk_alt)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common_crosswalk_basketball import to_eastern
from sportsdataverse._crosswalk_alt import (
    BASKETBALL_PLAYER_LEGS,
    ESPN_GAMES_SCHEMA,
    ESPN_ROSTERS_SCHEMA,
    basketball_espn_side,
    fill_player_legs,
    main,
    match_season,
    parse_season,
    read_season,
)

FIX = Path(__file__).parent / "fixtures" / "crosswalk_alt"
RAW = FIX / "raw"


def _nba_espn(season: int, game_id: int) -> tuple:
    sched = pl.read_parquet(FIX / "espn" / f"nba_schedule_{season}_{game_id}.parquet")
    rosters = pl.read_parquet(FIX / "espn" / f"nba_game_rosters_{season}_{game_id}.parquet")
    return basketball_espn_side(sched, rosters)


def _espn_id(rosters: pl.DataFrame, name: str) -> str:
    return rosters.filter(pl.col("espn_full_name") == name)["espn_athlete_id"].item()


def test_parse_yahoo_uses_the_side_team_and_eastern_date():
    out = parse_season("yahoo", "nba", 2024, RAW)
    game = out["games"].row(0, named=True)
    assert game["game_id"] == "nba.g.2023110619"
    assert game["game_date"] == date(2023, 11, 6)  # 18:00 -06:00 = 19:00 ET
    assert (game["home_team_id"], game["away_team_id"]) == ("nba.t.19", "nba.t.6")
    assert game["home_team_name"] == "Orlando Magic"
    rosters = out["game_rosters"]
    assert rosters.height == 36  # 18 dressed per side, DNPs included
    luka = rosters.filter(pl.col("player_id") == "nba.p.6014").row(0, named=True)
    assert (luka["team_id"], luka["player_name"], luka["jersey"]) == ("nba.t.6", "Luka Dončić", "77")


def test_parse_fox_reads_left_as_away_and_full_names():
    out = parse_season("fox", "nba", 2002, RAW)
    game = out["games"].row(0, named=True)
    assert game["game_id"] == "85726"
    assert game["game_date"] == date(2001, 11, 6)  # 2001-11-07T01:00Z
    assert (game["home_team_id"], game["away_team_id"]) == ("19", "5")  # Spurs host the Magic
    rosters = out["game_rosters"]
    assert rosters.select("team_id", "player_id").is_duplicated().sum() == 0
    names = dict(rosters.select("player_id", "player_name").iter_rows())
    assert names["760"] == "Tim Duncan"  # headshot alt text
    assert names["766"] == "Tracy Mcgrady"  # default headshot: title-cased link title
    assert rosters.filter(pl.col("player_id") == "760")["team_id"].item() == "19"


def test_parse_cbs_takes_games_from_the_scoreboard_and_players_from_every_list():
    out = parse_season("cbs", "nfl", 2024, RAW)
    game = out["games"].filter(pl.col("game_id") == "29376790").row(0, named=True)
    assert game["game_date"] == date(2024, 12, 25)
    assert (game["home_team_id"], game["away_team_id"]) == ("426", "417")
    rosters = out["game_rosters"].filter(pl.col("game_id") == "29376790")
    # participants + inactives per side, one row per player
    assert rosters.group_by("team_id").len().sort("team_id").rows() == [("417", 54), ("426", 52)]
    mahomes = rosters.filter(pl.col("player_id") == "2142052").row(0, named=True)
    assert (mahomes["player_name"], mahomes["jersey"], mahomes["team_id"]) == ("Patrick Mahomes", "15", "417")


def test_match_yahoo_game_and_players_to_espn():
    games, rosters = _nba_espn(2024, 401584775)
    m = match_season(games, rosters, parse_season("yahoo", "nba", 2024, RAW))
    assert m["games"].rows() == [("401584775", "nba.g.2023110619")]
    assert dict(m["teams"].select("espn_team_id", "team_id").iter_rows()) == {6: "nba.t.6", 19: "nba.t.19"}
    players = m["players"]
    assert players.height == rosters.height  # every ESPN athlete found among Yahoo's 36
    luka = players.filter(pl.col("espn_athlete_id") == _espn_id(rosters, "Luka Doncic")).row(0, named=True)
    assert (luka["player_id"], luka["match_method"], luka["match_confidence"]) == ("nba.p.6014", "exact_name", 1.0)


def test_match_fox_without_jerseys():
    games, rosters = _nba_espn(2002, 211106024)
    m = match_season(games, rosters, parse_season("fox", "nba", 2002, RAW))
    assert m["games"].rows() == [("211106024", "85726")]
    by_espn = dict(m["players"].select("espn_athlete_id", "player_id").iter_rows())
    assert by_espn[_espn_id(rosters, "Tim Duncan")] == "760"
    assert by_espn[_espn_id(rosters, "Tracy McGrady")] == "766"


def test_match_cbs_against_espn_box_athletes():
    d = json.loads((FIX / "espn" / "nfl_2024_401671799.json").read_text())
    g = d["game"]
    games = pl.DataFrame(
        [
            {
                "espn_game_id": str(g["espn_event_id"]),
                "game_date": to_eastern(g["kickoff_utc"]),
                "home_espn_team_id": int(g["home_espn_team_id"]),
                "away_espn_team_id": int(g["away_espn_team_id"]),
            }
        ],
        schema=ESPN_GAMES_SCHEMA,
    )
    rosters = pl.DataFrame(
        [
            {
                "espn_game_id": str(g["espn_event_id"]),
                "espn_team_id": int(a["team_id"]),
                "espn_athlete_id": a["athlete_id"],
                "espn_full_name": a["athlete_display_name"],
                "espn_jersey": a["athlete_jersey"],
            }
            for a in d["athletes"]
        ],
        schema=ESPN_ROSTERS_SCHEMA,
    ).unique(["espn_team_id", "espn_athlete_id"])
    alt = parse_season("cbs", "nfl", 2024, RAW)
    # Two games share 2024-12-25 on the week page, and ESPN has only one: with nothing else to tell
    # them apart the team pairing is a tie, which stays unpaired rather than guessed.
    tied = match_season(games, rosters, alt)
    assert tied["teams"].height == 0 and tied["games"].height == 0 and tied["players"].height == 0
    alt["games"] = alt["games"].filter(pl.col("game_id") == "29376790")
    m = match_season(games, rosters, alt)
    assert m["games"].rows() == [("401671799", "29376790")]
    assert dict(m["teams"].select("espn_team_id", "team_id").iter_rows()) == {12: "417", 23: "426"}
    assert m["players"].filter(pl.col("espn_athlete_id") == "3139477")["player_id"].item() == "2142052"
    assert m["stats"]["players_matched"] >= 0.95 * m["stats"]["espn_athletes"]


def test_match_season_rejects_mistyped_espn_inputs():
    games, rosters = _nba_espn(2024, 401584775)
    with pytest.raises(ValueError, match="espn_rosters"):
        match_season(
            games, rosters.with_columns(pl.col("espn_team_id").cast(pl.Utf8)), parse_season("yahoo", "nba", 2024, RAW)
        )


def test_fill_player_legs_never_overwrites_an_existing_leg():
    xwalk = pl.DataFrame(
        {
            "espn_team_id": [6, 6, 19],
            "espn_athlete_id": ["a", "b", "c"],
            "yahoo_player_id": ["nba.p.1", None, None],
            "yahoo_player_name": ["Kept", None, None],
        },
        schema={
            "espn_team_id": pl.Int32,
            "espn_athlete_id": pl.Utf8,
            "yahoo_player_id": pl.Utf8,
            "yahoo_player_name": pl.Utf8,
        },
    )
    players = pl.DataFrame(
        {
            "espn_team_id": [6, 6],
            "espn_athlete_id": ["a", "b"],
            "player_id": ["nba.p.9", "nba.p.2"],
            "player_name": ["New", "Filled"],
        },
        schema={"espn_team_id": pl.Int32, "espn_athlete_id": pl.Utf8, "player_id": pl.Utf8, "player_name": pl.Utf8},
    )
    out = fill_player_legs(xwalk, players, BASKETBALL_PLAYER_LEGS["yahoo"])
    assert out.schema == xwalk.schema
    assert out.rows() == [
        (6, "a", "nba.p.1", "Kept"),
        (6, "b", "nba.p.2", "Filled"),
        (19, "c", None, None),
    ]


def test_cli_writes_a_season_that_read_season_reads_back(tmp_path):
    assert read_season(tmp_path, "yahoo", "nba", 2024) is None
    assert (
        main(
            [
                "--provider",
                "yahoo",
                "--league",
                "nba",
                "--season",
                "2024",
                "--raw-root",
                str(RAW),
                "--out-root",
                str(tmp_path),
            ]
        )
        == 0
    )
    back = read_season(tmp_path, "yahoo", "nba", 2024)
    assert back is not None
    assert back["games"].height == 1 and back["game_rosters"].height == 36
    assert (
        main(
            [
                "--provider",
                "yahoo",
                "--league",
                "nba",
                "--season",
                "2023",
                "--raw-root",
                str(RAW),
                "--out-root",
                str(tmp_path),
            ]
        )
        == 1
    )


def test_a_game_key_held_twice_is_left_unmatched():
    games, rosters = _nba_espn(2024, 401584775)
    alt = parse_season("yahoo", "nba", 2024, RAW)
    alt["games"] = pl.concat([alt["games"], alt["games"].with_columns(pl.lit("nba.g.duplicate").alias("game_id"))])
    m = match_season(games, rosters, alt)
    assert m["teams"].height == 2
    assert m["games"].height == 0 and m["players"].height == 0


def test_with_alt_player_legs_fills_yahoo_and_skips_missing_providers(tmp_path):
    from sportsdataverse._crosswalk_alt import with_alt_player_legs

    assert (
        main(
            [
                "--provider",
                "yahoo",
                "--league",
                "nba",
                "--season",
                "2024",
                "--raw-root",
                str(RAW),
                "--out-root",
                str(tmp_path),
            ]
        )
        == 0
    )
    sched = pl.read_parquet(FIX / "espn" / "nba_schedule_2024_401584775.parquet")
    rosters = pl.read_parquet(FIX / "espn" / "nba_game_rosters_2024_401584775.parquet")
    xwalk = rosters.select(
        pl.col("team_id").cast(pl.Int32).alias("espn_team_id"),
        pl.col("athlete_id").cast(pl.Int64).cast(pl.Utf8).alias("espn_athlete_id"),
        pl.col("athlete_display_name").alias("espn_full_name"),
        *[
            pl.lit(None, dtype=pl.Utf8).alias(c)
            for c in ("fox_athlete_id", "fox_player", "yahoo_player_id", "yahoo_player_name")
        ],
    )
    out = with_alt_player_legs("nba", 2024, xwalk, sched, rosters, tmp_path)
    assert out.columns == xwalk.columns
    assert out["yahoo_player_id"].null_count() == 0
    assert out.filter(pl.col("espn_full_name") == "Luka Doncic")["yahoo_player_name"].item() == "Luka Dončić"
    assert out["fox_athlete_id"].null_count() == out.height  # no parsed Fox 2024 season: untouched


def test_with_alt_game_legs_fills_fox_and_keeps_existing_legs(tmp_path):
    from sportsdataverse._crosswalk_alt import fill_game_legs, with_alt_game_legs

    argv = ["--provider", "fox", "--league", "nba", "--season", "2002", "--raw-root", str(RAW), "--out-root"]
    assert main([*argv, str(tmp_path)]) == 0
    sched = pl.read_parquet(FIX / "espn" / "nba_schedule_2002_211106024.parquet")
    rosters = pl.read_parquet(FIX / "espn" / "nba_game_rosters_2002_211106024.parquet")
    games, espn_rosters = basketball_espn_side(sched, rosters)
    legs = ("fox_game_id", "fox_home_team_id", "fox_away_team_id", "yahoo_game_id")
    xwalk = games.with_columns(*[pl.lit(None, dtype=pl.Utf8).alias(c) for c in legs])

    out = with_alt_game_legs("nba", 2002, xwalk, sched, rosters, tmp_path)
    assert out.columns == xwalk.columns and out.schema == xwalk.schema
    row = out.row(0, named=True)
    assert row["fox_game_id"] == "85726"
    assert row["fox_home_team_id"] is not None and row["fox_away_team_id"] is not None
    assert row["fox_home_team_id"] != row["fox_away_team_id"]
    assert row["yahoo_game_id"] is None  # no parsed Yahoo 2002 season: untouched

    kept = xwalk.with_columns(pl.lit("live").alias("fox_game_id"))
    m = match_season(games, espn_rosters, read_season(tmp_path, "fox", "nba", 2002))
    again = fill_game_legs(kept, m["games"], m["teams"], ("fox_game_id", "fox_home_team_id", "fox_away_team_id"))
    assert again.row(0, named=True)["fox_game_id"] == "live"
    assert again["fox_home_team_id"].null_count() == again.height  # a set game id keeps every leg as-is


def test_parse_cbs_skips_a_basketball_box_under_a_football_id():
    from sportsdataverse._crosswalk_alt import _cbs_players, _read

    payload = _read(RAW / "cbs" / "cfb" / "game_boxscore" / "2017" / "2843743.json.gz")
    assert any(isinstance(b.get("playerStats"), list) for b in payload["boxScores"])
    assert _cbs_players(payload, 2017, "2843743") == []


def test_known_teams_are_taken_as_given_and_contradictions_counted():
    games, rosters = _nba_espn(2024, 401584775)
    alt = parse_season("yahoo", "nba", 2024, RAW)
    schema = {"espn_team_id": pl.Int32, "team_id": pl.Utf8}

    agree = match_season(
        games, rosters, alt, known_teams=pl.DataFrame({"espn_team_id": [6], "team_id": ["nba.t.6"]}, schema=schema)
    )
    assert dict(agree["teams"].select("espn_team_id", "team_id").iter_rows()) == {6: "nba.t.6", 19: "nba.t.19"}
    assert agree["stats"]["teams_contradicting_known"] == 0

    swapped = pl.DataFrame({"espn_team_id": [6, 19], "team_id": ["nba.t.19", "nba.t.6"]}, schema=schema)
    m = match_season(games, rosters, alt, known_teams=swapped)
    assert dict(m["teams"].select("espn_team_id", "team_id").iter_rows()) == {6: "nba.t.19", 19: "nba.t.6"}
    assert m["teams"]["match_confidence"].to_list() == [1.0, 1.0]
    assert m["stats"]["teams_contradicting_known"] == 2


def test_known_teams_ships_the_cbs_cfb_map_only():
    from sportsdataverse._crosswalk_alt import known_teams

    cbs = known_teams("cbs", "cfb")
    assert cbs is not None and cbs.height >= 320
    assert cbs.schema == {"espn_team_id": pl.Int32, "team_id": pl.Utf8}
    assert cbs["espn_team_id"].is_unique().all() and cbs["team_id"].is_unique().all()
    assert cbs.filter(pl.col("espn_team_id") == 2405)["team_id"].item() == "709"  # Monmouth
    assert known_teams("fox", "nba") is None
