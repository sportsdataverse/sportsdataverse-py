import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb import cfb_yahoo_ext as y
from tests.conftest import skip_if_no_live

MODERN = {
    "data": {
        "leagues": [
            {
                "footballStats": [
                    {
                        "player": {
                            "playerId": "ncaaf.p.1",
                            "displayName": "QB One",
                            "team": {"displayName": "Team A", "abbreviation": "TA"},
                        },
                        "stats": [
                            {"statId": "PASSING_YARDS", "value": "4000"},
                            {"statId": "PASSING_TOUCHDOWNS", "value": "40"},
                        ],
                    },
                    {
                        "player": {
                            "playerId": "ncaaf.p.2",
                            "displayName": "QB Two",
                            "team": {"displayName": "Team B", "abbreviation": "TB"},
                        },
                        "stats": [{"statId": "PASSING_YARDS", "value": "3500"}],
                    },
                ]
            }
        ]
    },
    "extensions": {},
}


def test_flatten_modern_pivots_wide():
    rows = y._flatten_modern(MODERN, "footballStats")
    assert len(rows) == 2
    r0 = rows[0]
    assert r0["player_id"] == "ncaaf.p.1"
    assert r0["display_name"] == "QB One"
    assert r0["team"] == "Team A"
    assert r0["team_abbreviation"] == "TA"
    assert r0["passing_yards"] == "4000"
    assert r0["passing_touchdowns"] == "40"
    # missing stat is absent (or None) on row 2, but pivot keys exist on row 1
    assert "passing_yards" in rows[1]
    # polars path
    assert isinstance(y._frame([{"a": 1}], False), pl.DataFrame)


def test_player_season_stats_uses_modern_query(monkeypatch):
    captured = {}

    def fake_get(url, params=None, headers=None, **kw):
        captured["url"] = url
        captured["params"] = params
        return MODERN

    monkeypatch.setattr(y, "_get", fake_get)
    df = y.yahoo_cfb_player_season_stats(season=2024, return_as_pandas=True)
    assert captured["url"].endswith("/leagueStatsIndividual")
    assert captured["params"]["leagues"] == "ncaaf"
    assert captured["params"]["season"] == 2024
    assert "passing_yards" in df.columns
    assert len(df) == 2
    # raw passthrough
    raw = y.yahoo_cfb_player_season_stats(season=2024, return_parsed=False)
    assert "data" in raw


LEGACY = {
    "data": {
        "leagues": [
            {
                "leaders": [
                    {
                        "player": {
                            "playerId": "ncaaf.p.9",
                            "displayName": "RB Nine",
                            "team": {"displayName": "Team C", "abbreviation": "TC"},
                        },
                        "stats": [{"statId": "RUSHING_YARDS", "value": "1500"}],
                    }
                ]
            }
        ]
    },
    "extensions": {},
}


def test_team_and_legacy(monkeypatch):
    monkeypatch.setattr(
        y,
        "_get",
        lambda url, params=None, headers=None, **k: MODERN if "leagueStatsByTeam" in url else LEGACY,
    )
    tdf = y.yahoo_cfb_team_season_stats(season=2024, return_as_pandas=True)
    assert len(tdf) == 2
    pdf = y.yahoo_cfb_player_season_stats_legacy(
        season=2024, category="Rushing", sort_stat="RUSHING_YARDS", return_as_pandas=True
    )
    assert pdf.iloc[0]["rushing_yards"] == "1500"
    assert pdf.iloc[0]["season"] == 2024
    assert pdf.iloc[0]["category"] == "Rushing"


def test_legacy_rejects_bad_category():
    with pytest.raises(ValueError):
        y.yahoo_cfb_player_season_stats_legacy(season=2024, category="Bogus", sort_stat="X")


def test_team_legacy_and_extended_category(monkeypatch):
    monkeypatch.setattr(y, "_get", lambda url, params=None, headers=None, **k: LEGACY)
    df = y.yahoo_cfb_team_season_stats_legacy(
        season=2024, category="Offense", sort_stat="GAMES_OFFENSE", return_as_pandas=True
    )
    assert df.iloc[0]["category"] == "Offense"
    assert df.iloc[0]["season"] == 2024
    with pytest.raises(ValueError):
        y.yahoo_cfb_team_season_stats_legacy(season=2024, category="Bogus", sort_stat="X")


SCOREBOARD = {
    "service": {
        "scoreboard": {
            "games": {
                "ncaaf.g.1": {
                    "gameid": "ncaaf.g.1",
                    "home_team_id": "ncaaf.t.1",
                    "away_team_id": "ncaaf.t.2",
                    "total_home_points": "21",
                    "total_away_points": "17",
                    "week_number": "1",
                }
            }
        }
    }
}


def test_scoreboard_flattens_games_map(monkeypatch):
    monkeypatch.setattr(y, "_get", lambda url, params=None, headers=None, **k: SCOREBOARD)
    df = y.yahoo_cfb_scoreboard(season=2024, week=1, return_as_pandas=True)
    assert len(df) == 1
    assert df.iloc[0]["gameid"] == "ncaaf.g.1"
    assert df.iloc[0]["week"] == 1  # self-describing


BOX_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "yahoo" / "editorial_boxscore_ncaaf_ala_at_uk.json"


def test_boxscore_default_is_parsed_like_its_siblings(monkeypatch):
    """The module contract is polars-by-default; the box score used to be the lone
    ``return_parsed=False`` outlier. Pin both directions so it cannot drift back."""
    monkeypatch.setattr(y, "_get", lambda url, params=None, headers=None, **k: {"service": {"boxscore": {}}})
    df = y.yahoo_cfb_boxscore("ncaaf.g.202509200023")
    assert isinstance(df, pl.DataFrame) and df.columns == list(y._BOXSCORE_SCHEMA)
    out = y.yahoo_cfb_boxscore("ncaaf.g.202509200023", return_parsed=False)
    assert "service" in out  # return_parsed=False -> raw passthrough


def test_boxscore_parsed_is_one_row_per_entity_stat(monkeypatch):
    """Real editorial boxscore, Alabama (ncaaf.t.73) at Kentucky (ncaaf.t.69), 2026-09-12:
    both team_stats blocks plus four players (two per side), dictionaries untrimmed."""
    raw = json.loads(BOX_FIXTURE.read_text(encoding="utf-8"))
    seen = []

    def fake_get(url, params=None, headers=None, **k):
        seen.append(url)
        return raw

    monkeypatch.setattr(y, "_get", fake_get)
    df = y.yahoo_cfb_boxscore("ncaaf.g.202609120069", return_parsed=True)
    assert seen == [f"{y.EDITORIAL_BASE}/boxscore/ncaaf.g.202609120069"]
    assert df.columns == list(y._BOXSCORE_SCHEMA)
    assert all(dtype == pl.Utf8 for dtype in df.schema.values())
    assert df.height == 96  # 25 team stats x 2 teams + 15 + 15 + 6 + 10 player stats
    assert df["game_id"].unique().to_list() == ["ncaaf.g.202609120069"]

    teams = df.filter(pl.col("player_id").is_null())
    assert teams.height == 50 and set(teams["stat_category"]) == {"Team"}
    total = teams.filter(pl.col("stat_type_id") == "ncaaf.stat_type.945")
    assert dict(zip(total["team_id"], total["value"])) == {"ncaaf.t.69": "209", "ncaaf.t.73": "343"}
    third = teams.filter((pl.col("team_id") == "ncaaf.t.69") & (pl.col("stat_name") == "Third Down Efficiency"))
    assert third.select("home_away", "stat_abbreviation", "value").row(0) == ("home", "3DE", "1-14")

    qb = df.filter((pl.col("player_id") == "ncaaf.p.470424") & (pl.col("stat_type_id") == "ncaaf.stat_type.105"))
    assert qb.select("team_id", "home_away", "stat_category", "stat_name", "stat_variation", "value").row(0) == (
        "ncaaf.t.73",
        "away",
        "Passing",
        "Yards",
        "Game",
        "188",
    )
    kicker = df.filter(pl.col("player_id") == "ncaaf.p.404415")
    assert kicker.height == 6 and set(kicker["stat_category"]) == {"Kicking"}
    assert set(kicker["team_id"]) == {"ncaaf.t.73"}


@pytest.mark.parametrize("payload", [None, {}, {"service": {}}, {"service": {"boxscore": {}}}, "junk"])
def test_boxscore_parsed_empty_payload_keeps_schema(monkeypatch, payload):
    monkeypatch.setattr(y, "_get", lambda url, params=None, headers=None, **k: payload)
    df = y.yahoo_cfb_boxscore("ncaaf.g.1", return_parsed=True)
    assert df.height == 0 and df.columns == list(y._BOXSCORE_SCHEMA)


def test_boxscore_parsed_pandas(monkeypatch):
    raw = json.loads(BOX_FIXTURE.read_text(encoding="utf-8"))
    monkeypatch.setattr(y, "_get", lambda url, params=None, headers=None, **k: raw)
    df = y.yahoo_cfb_boxscore("ncaaf.g.202609120069", return_parsed=True, return_as_pandas=True)
    assert len(df) == 96 and "stat_name" in df.columns


@skip_if_no_live
def test_live_player_season_stats():
    df = y.yahoo_cfb_player_season_stats(season=2024, return_as_pandas=True)
    # subset-direction: Yahoo may add columns over time
    for col in ("player_id", "display_name", "team", "passing_yards", "season"):
        assert col in df.columns
    assert len(df) > 0
