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


# Yahoo retired the editorial feed on 2026-11-20 (every league answers 404); the
# scoreboard, team directory and box score read shangrila. The fixtures are real
# captures from 2026-11-20 (tests/fixtures/yahoo/README.md).
_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "yahoo"


def _fixture(name):
    return json.loads((_FIX / name).read_text(encoding="utf-8"))


def _route(monkeypatch, **by_query):
    """Answer each shangrila query name from its recorded payload; log every call."""
    calls = []

    def fake_get(url, params=None, headers=None, **k):
        query = url.rsplit("/", 1)[-1]
        calls.append((query, dict(params or {})))
        assert url == f"{y.SHANGRILA_BASE}/{query}" and headers == y._HEADERS
        return by_query[query]

    monkeypatch.setattr(y, "_get", fake_get)
    return calls


def test_scoreboard_on_a_real_capture(monkeypatch):
    """2025 week 5: leagueGameIdsByDate lists the ids, ONE scoreboardGame call fetches them."""
    calls = _route(
        monkeypatch,
        leagueGameIdsByDate=_fixture("shangrila_league_game_ids_by_date_ncaaf_2025_w5.json"),
        scoreboardGame=_fixture("shangrila_scoreboard_game_ncaaf_2025_w5.json"),
    )
    df = y.yahoo_cfb_scoreboard(season=2025, week=5)
    assert [q for q, _ in calls] == ["leagueGameIdsByDate", "scoreboardGame"]
    assert calls[0][1]["leagues"] == "ncaaf" and calls[0][1]["season"] == 2025 and calls[0][1]["week"] == 5
    assert calls[1][1]["gameId"] == "ncaaf.g.202509250099,ncaaf.g.202509260008,ncaaf.g.202509260058"
    assert df.schema == pl.Schema(y._SCOREBOARD_SCHEMA)
    row = df.filter(pl.col("game_id") == "ncaaf.g.202509260058").row(0, named=True)
    assert (
        row["home_team_id"],
        row["home_team"],
        row["away_team_id"],
        row["away_team"],
    ) == (
        "ncaaf.t.58",
        "Arizona State Sun Devils",
        "ncaaf.t.85",
        "TCU Horned Frogs",
    )
    assert (row["start_time"], row["season"], row["week"], row["season_phase"]) == (
        "2025-09-26T20:00:00-05:00",
        2025,
        5,
        "REGULAR_SEASON",
    )
    raw = y.yahoo_cfb_scoreboard(season=2025, week=5, return_parsed=False)
    assert [g["gameId"] for g in raw["data"]["games"]] == df["game_id"].to_list()


def test_scoreboard_chunks_a_big_week_and_keeps_schema_on_an_empty_one(monkeypatch):
    ids = {"data": {"leagues": [{"games": [{"gameId": f"ncaaf.g.{i}"} for i in range(y._SCOREBOARD_CHUNK + 2)]}]}}
    calls = _route(monkeypatch, leagueGameIdsByDate=ids, scoreboardGame={"data": {"games": []}})
    y.yahoo_cfb_scoreboard(season=2025, week=1)
    assert [len(p["gameId"].split(",")) for q, p in calls if q == "scoreboardGame"] == [
        y._SCOREBOARD_CHUNK,
        2,
    ]

    calls = _route(monkeypatch, leagueGameIdsByDate={"data": {"leagues": [{"games": []}]}})
    df = y.yahoo_cfb_scoreboard(season=2025, week=22, return_as_pandas=True)
    assert len(df) == 0 and list(df.columns) == list(y._SCOREBOARD_SCHEMA)
    assert [q for q, _ in calls] == ["leagueGameIdsByDate"]  # no empty scoreboardGame call


def test_scoreboard_404_on_listed_ids_is_a_failed_fetch(monkeypatch):
    # NoDataError here would let the crosswalk count the week as empty and drop its ids
    from sportsdataverse.errors import AssetFetchError, NoDataError

    ids = {"data": {"leagues": [{"games": [{"gameId": "ncaaf.g.1"}]}]}}

    def fake_get(url, params=None, headers=None, **k):
        if url.endswith("/scoreboardGame"):
            raise NoDataError("404")
        return ids

    monkeypatch.setattr(y, "_get", fake_get)
    with pytest.raises(AssetFetchError, match="listed ids"):
        y.yahoo_cfb_scoreboard(season=2025, week=1)


def test_teams_on_a_real_capture(monkeypatch):
    calls = _route(monkeypatch, leagueTeams=_fixture("shangrila_league_teams_ncaaf.json"))
    df = y.yahoo_cfb_teams(season=2025)
    assert calls == [("leagueTeams", {**calls[0][1], "league": "ncaaf", "season": 2025})]
    assert df.schema == pl.Schema(y._TEAMS_SCHEMA)  # every column Utf8, editorial column set kept
    assert df["team_id"].to_list() == ["ncaaf.t.1", "ncaaf.t.116", "ncaaf.t.226"]
    clemson = df.row(0, named=True)
    assert clemson == {
        "team_id": "ncaaf.t.1",
        "abbreviation": "CLEM",
        "display_name": "Clemson",
        "full_name": "Clemson Tigers",
        "location": "Clemson",
        "nickname": "Tigers",
        "conference": "Atlantic Coast",
        "conference_abbreviation": None,
        "conference_id": "1",
        "division": "FBS",
        "division_id": "1",
        "seatgeek_id": None,
    }
    assert df["division"].to_list() == ["FBS", "FCS", "D2"]
    assert df["seatgeek_id"].null_count() == df.height == df["conference_abbreviation"].null_count()


def test_teams_empty_payload_keeps_schema(monkeypatch):
    _route(monkeypatch, leagueTeams={"data": {"leagues": []}})
    df = y.yahoo_cfb_teams(season=2025)
    assert df.height == 0 and df.columns == list(y._TEAMS_SCHEMA)


def test_boxscore_default_is_parsed_like_its_siblings(monkeypatch):
    """The module contract is polars-by-default; pin both directions so it cannot drift back."""
    _route(monkeypatch, playbookBoxscore={"data": {"games": []}})
    df = y.yahoo_cfb_boxscore("ncaaf.g.202509270116")
    assert isinstance(df, pl.DataFrame) and df.columns == list(y._BOXSCORE_SCHEMA)
    out = y.yahoo_cfb_boxscore("ncaaf.g.202509270116", return_parsed=False)
    assert "data" in out  # return_parsed=False -> raw passthrough


def test_boxscore_on_a_real_capture(monkeypatch):
    """playbookBoxscore, ncaaf.t.114 at Montana (ncaaf.t.116), 2025-09-27: two players
    with stats and one without per side; null stats are dropped."""
    calls = _route(monkeypatch, playbookBoxscore=_fixture("shangrila_playbook_boxscore_ncaaf.json"))
    df = y.yahoo_cfb_boxscore("ncaaf.g.202509270116")
    assert calls[0][1]["gameId"] == "ncaaf.g.202509270116" and calls[0][1]["isFootball"] == "true"
    assert df.columns == list(y._BOXSCORE_SCHEMA)
    assert all(dtype == pl.Utf8 for dtype in df.schema.values())
    assert df.height == 14  # home 3 + 2 stats, away 5 + 4; stat-less players add no rows
    assert df["game_id"].unique().to_list() == ["ncaaf.g.202509270116"]
    assert df["player_id"].null_count() == 0 and set(df["stat_variation"]) == {"Game"}
    qb = df.filter((pl.col("player_id") == "ncaaf.p.336870") & (pl.col("stat_type_id") == "PASSING_YARDS"))
    assert qb.select("team_id", "home_away", "stat_category", "stat_name", "value").row(0) == (
        "ncaaf.t.114",
        "away",
        "Passing",
        "Passing Yards",
        "262",
    )
    receiver = df.filter(pl.col("player_id") == "ncaaf.p.312809")
    assert set(receiver["stat_category"]) == {"Receiving"} and set(receiver["home_away"]) == {"home"}
    assert df["stat_category"].null_count() == 0


@pytest.mark.parametrize("payload", [None, {}, {"data": {}}, {"data": {"games": []}}, "junk"])
def test_boxscore_parsed_empty_payload_keeps_schema(monkeypatch, payload):
    _route(monkeypatch, playbookBoxscore=payload)
    df = y.yahoo_cfb_boxscore("ncaaf.g.1", return_parsed=True)
    assert df.height == 0 and df.columns == list(y._BOXSCORE_SCHEMA)


def test_boxscore_parsed_pandas(monkeypatch):
    _route(monkeypatch, playbookBoxscore=_fixture("shangrila_playbook_boxscore_ncaaf.json"))
    df = y.yahoo_cfb_boxscore("ncaaf.g.202509270116", return_as_pandas=True)
    assert len(df) == 14 and "stat_name" in df.columns


@skip_if_no_live
def test_live_player_season_stats():
    df = y.yahoo_cfb_player_season_stats(season=2024, return_as_pandas=True)
    # subset-direction: Yahoo may add columns over time
    for col in ("player_id", "display_name", "team", "passing_yards", "season"):
        assert col in df.columns
    assert len(df) > 0


# --------------------------------------------------------------------------------------
# B-L50: the season-stat tables on real Yahoo captures (tests/fixtures/yahoo/README.md):
# the modern team table (leagueStatsByTeam, 2024 FBS, trimmed to its first 3 teams) and the
# legacy player table (season-stats passing leaders). Every expected value is read from the
# payload: one row per entity, one column per statId holding that stat's value.
# --------------------------------------------------------------------------------------

_YAHOO = Path(__file__).resolve().parents[1] / "fixtures" / "yahoo"


def _yahoo(name):
    return json.loads((_YAHOO / f"{name}.json").read_text(encoding="utf-8"))


def test_team_season_stats_on_a_real_capture(monkeypatch):
    raw = _yahoo("shangrila_team_season_stats_ncaaf_2024")
    monkeypatch.setattr(y, "_get", lambda url, params=None, headers=None, **k: raw)
    df = y.yahoo_cfb_team_season_stats(season=2024)
    teams = raw["data"]["leagues"][0]["footballStats"]
    assert df["team"].to_list() == ["Clemson", "Duke", "Florida St."] == [t["team"]["displayName"] for t in teams]
    assert df["team_abbreviation"].to_list() == [t["team"]["abbreviation"] for t in teams]
    assert df["season"].unique().to_list() == [2024]
    for i, team in enumerate(teams):
        for stat in team["stats"]:
            assert df[y._clean(stat["statId"])][i] == stat["value"], (team["team"]["displayName"], stat["statId"])


def test_player_season_stats_legacy_on_a_real_capture(monkeypatch):
    raw = _yahoo("shangrila_season_stats_football_passing_ncaaf")
    monkeypatch.setattr(y, "_get", lambda url, params=None, headers=None, **k: raw)
    df = y.yahoo_cfb_player_season_stats_legacy(season=2024, category="Passing", sort_stat="PASSING_YARDS")
    leaders = raw["data"]["leagues"][0]["leaders"]
    assert df.height == len(leaders) == 3
    assert df["player_id"].to_list() == [r["player"]["playerId"] for r in leaders]
    assert df["display_name"].to_list() == [r["player"]["displayName"] for r in leaders]
    for i, row in enumerate(leaders):
        for stat in row["stats"]:
            assert df[y._clean(stat["statId"])][i] == stat["value"], (row["player"]["displayName"], stat["statId"])
