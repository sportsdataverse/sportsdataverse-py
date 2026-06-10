from sportsdataverse.cfb import cfb_yahoo_ext as y

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


def test_legacy_rejects_bad_category():
    import pytest

    with pytest.raises(ValueError):
        y.yahoo_cfb_player_season_stats_legacy(season=2024, category="Bogus", sort_stat="X")


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


def test_boxscore_scaffold_returns_raw(monkeypatch):
    monkeypatch.setattr(y, "_get", lambda url, params=None, headers=None, **k: {"service": {"boxscore": {}}})
    out = y.yahoo_cfb_boxscore("ncaaf.g.202509200023")
    assert "service" in out  # scaffold passes raw through for now


import os

import pytest


@pytest.mark.skipif(
    os.environ.get("YAHOO_TESTS") != "1",
    reason="set YAHOO_TESTS=1 to run live Yahoo API tests",
)
def test_live_player_season_stats():
    df = y.yahoo_cfb_player_season_stats(season=2024, return_as_pandas=True)
    # subset-direction: Yahoo may add columns over time
    for col in ("player_id", "display_name", "team", "passing_yards", "season"):
        assert col in df.columns
    assert len(df) > 0
