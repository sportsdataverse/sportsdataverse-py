"""Offline tests for the SIHF (``sihf_api_web``) flat-API family.

Asserts the parsers against the real captures in ``tests/fixtures/sihf_api_web/`` (never
synthetic payloads), the committed returns-schemas against what the parsers emit, the
wrappers' request construction through a fake getter, and the empty / malformed contract.
No network, except the gated live smokes at the bottom (``SDV_PY_LIVE_TESTS=1``).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse.hockey.sihf import sihf_api_web
from sportsdataverse.hockey.sihf.sihf_api_web_parsers import (
    parse_sihf_game_goalie_stats,
    parse_sihf_game_info,
    parse_sihf_game_lineups,
    parse_sihf_game_player_stats,
    parse_sihf_game_team_stats,
    parse_sihf_pbp,
    parse_sihf_table,
)
from tests.conftest import skip_if_no_live

FIXTURES = Path(__file__).parents[2] / "fixtures" / "sihf_api_web"
SCHEMA_DIR = Path(__file__).parents[3] / "tools" / "codegen" / "schemas" / "native" / "sihf_api_web"
GAME = "gameoverview__20261105000325.json"  # goaltender change + revoked goal
SHOOTOUT = "gameoverview__20261105000009.json"

GAME_PARSERS = {
    "pbp": parse_sihf_pbp,
    "game_info": parse_sihf_game_info,
    "game_player_stats": parse_sihf_game_player_stats,
    "game_goalie_stats": parse_sihf_game_goalie_stats,
    "game_team_stats": parse_sihf_game_team_stats,
    "game_lineups": parse_sihf_game_lineups,
}
TABLES = {
    "results": "table__results__2026__1.json",
    "standings": "table__standing__2026__1.json",
    "player_stats": "table__player__2026__1.json",
    "goalie_stats": "table__goalkeeper__2026__1.json",
}


def _load(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


# --- tables -------------------------------------------------------------------


def test_results_expand_typed_cells():
    df = parse_sihf_table(_load(TABLES["results"]))
    assert df.height == 3
    first = df.row(0, named=True)
    assert (first["season"], first["phase_id"], first["row_type"]) == (2026, None, "data")
    assert "league_id" not in df.columns  # the league travels in searchQuery, which is not echoed
    # season order: the opener first
    assert (first["home_team_name"], first["away_team_name"]) == ("HC Ambri-Piotta", "EHC Kloten")
    assert first["date"].isoformat() == "2025-09-09"
    assert (first["score_home"], first["score_away"], first["decision"]) == (2, 1, None)  # "" = regulation
    assert (first["score_per_period_home"], first["score_per_period_away"]) == ([0, 1, 1], [0, 1, 0])
    assert first["game_id"] == 20261105000001
    assert df.schema["date"] == pl.Date and df.schema["game_id"] == pl.Int64
    assert df.schema["home_team_id"] == pl.Int64
    assert df.schema["phase_id"] == pl.Int64  # Int64 even when every row is null


def test_current_season_results_cover_the_whole_schedule():
    # captured with the season-wide Date range: without it the current season answers one game day
    df = parse_sihf_table(_load("table__results__2027__1.json"))
    assert set(df["season"]) == {2027}
    assert df["date"].min().isoformat() >= "2026-07-01"


def test_standings_types_and_order():
    df = parse_sihf_table(_load(TABLES["standings"]))
    assert df.height == 14
    assert df["rank"].to_list() == list(range(1, 15))
    assert df.schema["points_quotient"] == pl.Float64
    assert df.schema["goals_difference"] == pl.Int64
    assert set(df["row_type"]) == {"data"}  # the "line" highlights are separators, not summary rows


def test_standings_are_the_full_regular_season_table():
    # Captured with ContentType=All. Without it SIHF serves the away-games table: 26 GP each, and
    # HC Davos on 50 points instead of 117 (measured 2026-10-12 against the season's results).
    df = parse_sihf_table(_load(TABLES["standings"]))
    assert set(df["games_played"]) == {52}  # 14 teams x 4 meetings
    assert df["games_played"].sum() == 2 * 364  # every one of the 364 regular-season games, twice
    top = df.row(0, named=True)
    assert (top["team_acronym"], top["points"]) == ("HCD", 117)
    wins = ["games_won_in_time", "games_won_extra_time", "games_won_penalty"]
    losses = ["games_lost_in_time", "games_lost_extra_time", "games_lost_penalty"]
    assert (df.select(pl.sum_horizontal(wins + losses)).to_series() == df["games_played"]).all()
    assert set(parse_sihf_table(_load("table__standing__2025__1.json"))["games_played"]) == {52}


def test_goalie_table_tags_the_league_average_row():
    df = parse_sihf_table(_load(TABLES["goalie_stats"]))
    avg = df.filter(pl.col("row_type") == "average")
    assert avg.height == 1
    assert avg["team_name"].item() == "Durchschnitt"
    assert avg["rank"].item() is None  # "" upstream
    assert df.schema["seconds_played"] == pl.String  # "mm:ss", despite the name


def test_second_season_fixture_echoes_its_season():
    df = parse_sihf_table(_load("table__standing__2025__1.json"))
    assert set(df["season"]) == {2025}
    assert "Fribourg-Gottéron" in parse_sihf_table(_load(TABLES["standings"]))["team_name"].to_list()  # UTF-8 intact


def test_table_pandas():
    assert isinstance(parse_sihf_table(_load(TABLES["standings"]), return_as_pandas=True), pd.DataFrame)


# --- the game page ------------------------------------------------------------


def test_pbp_events_in_game_order():
    df = parse_sihf_pbp(_load(GAME))
    assert df["event_index"].to_list() == list(range(1, df.height + 1))
    seconds = df["game_seconds"].to_list()
    assert seconds == sorted(seconds)
    assert set(df["event_type"]) == {"goal", "penalty", "goalie", "revoked_goal"}
    goals = df.filter(pl.col("event_type") == "goal")
    last = goals.row(-1, named=True)
    assert (last["score_home"], last["score_away"]) == (4, 3)
    assert goals["player_name"].null_count() == 0
    # plus / minus are listed for even-strength goals only
    pp = goals.filter(pl.col("strength") == "PP1")
    assert pp.height and all(v == [] for v in pp["plus_player_ids"].to_list())
    eq = goals.filter(pl.col("strength") == "EQ")
    assert all(len(v) == 5 for v in eq["plus_player_ids"].to_list())


def test_pbp_goalie_change_and_revoked_goal():
    df = parse_sihf_pbp(_load(GAME))
    changes = df.filter(pl.col("event_type") == "goalie").select("time", "player_name", "goalie_change").rows()
    assert ("36:24", "Boltshauser Luca", "out") in changes
    assert ("36:24", "Meyer Robin", "in") in changes
    revoked = df.filter(pl.col("event_type") == "revoked_goal")
    assert revoked["revoke_reason"].to_list() == ["CC-Offiside"]


def test_pbp_shootout_rows_follow_the_periods():
    df = parse_sihf_pbp(_load(SHOOTOUT))
    so = df.filter(pl.col("event_type") == "shootout")
    assert so["shootout_number"].to_list() == list(range(1, 11))
    assert so["shootout_scored"].sum() == 3
    assert so["goalie_name"].null_count() == 0
    assert set(so["team_side"]) == {"home", "away"}
    assert df["event_type"].to_list()[-so.height :] == ["shootout"] * so.height


def test_game_info_one_row():
    row = parse_sihf_game_info(_load(GAME)).row(0, named=True)
    assert row["game_id"] == 20261105000325
    assert (row["home_team_name"], row["away_team_name"]) == ("HC Davos", "SCL Tigers")
    assert (row["home_score"], row["away_score"]) == (4, 3)
    assert sum(row["home_score_by_period"]) == row["home_score"]
    assert len(row["period_indicators"]) == len(row["home_shots_by_period"])
    assert row["spectators"] and row["referees"]


def test_game_player_stats_resolve_player_ids():
    df = parse_sihf_game_player_stats(_load(GAME))
    players = df.filter(pl.col("row_type") == "data")
    assert players["player_id"].null_count() == 0
    assert set(df["team_side"]) == {"home", "away"}
    totals = df.filter(pl.col("row_type") == "total")
    assert totals.height == 2
    for side in ("home", "away"):
        side_players = players.filter(pl.col("team_side") == side)
        side_total = totals.filter(pl.col("team_side") == side)
        assert side_players["goals"].sum() == side_total["goals"].item()


def test_game_goalie_and_team_stats():
    goalies = parse_sihf_game_goalie_stats(_load(GAME))
    assert goalies.height == 4 and goalies["player_id"].null_count() == 0
    stats = parse_sihf_game_team_stats(_load(GAME))
    sog = stats.filter(pl.col("statistic") == "SOG Total").row(0, named=True)
    info = parse_sihf_game_info(_load(GAME)).row(0, named=True)
    assert int(sog["home_value"]) == sum(info["home_shots_by_period"])


def test_game_lineups_units_and_absentees():
    df = parse_sihf_game_lineups(_load(GAME))
    dressed = df.filter(pl.col("unit").is_not_null())
    assert set(dressed["unit"]) >= {"goalkeeper", "defense_left", "forward_center"}
    assert dressed.filter(pl.col("is_captain")).height == 2  # one per side
    absent = df.filter(pl.col("unit").is_null())
    assert absent.height and absent["absence_reason"].null_count() == 0
    assert df["player_name"].null_count() == 0


# --- contract -----------------------------------------------------------------


@pytest.mark.parametrize("payload", [None, {}, {"gameId": None}, [], "x"])
@pytest.mark.parametrize("short", sorted(GAME_PARSERS))
def test_game_parsers_empty_keep_their_columns(short, payload):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    df = GAME_PARSERS[short](payload)
    assert df.height == 0
    assert df.columns == [c["name"] for c in schema["columns"]]


_EMPTY_TABLES = [None, {}, [], "x", {"header": [], "data": []}, {"header": [{"alias": "a"}], "data": "x"}]


@pytest.mark.parametrize("payload", _EMPTY_TABLES)
@pytest.mark.parametrize("short", sorted(TABLES))
def test_table_parser_empty_keeps_the_documented_schema(short, payload):
    # The columns AND dtypes of an empty table are the ones the real capture parses to.
    names = [c["name"] for c in yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))["columns"]]
    df = parse_sihf_table(payload, columns=names)
    assert df.height == 0
    assert df.columns == names
    assert df.schema == parse_sihf_table(_load(TABLES[short])).schema


@pytest.mark.parametrize("payload", _EMPTY_TABLES)
def test_table_parser_empty_without_columns(payload):
    assert parse_sihf_table(payload).height == 0


@pytest.mark.parametrize(
    ("fn", "short"),
    [
        (sihf_api_web.sihf_results, "results"),
        (sihf_api_web.sihf_standings, "standings"),
        (sihf_api_web.sihf_player_stats, "player_stats"),
        (sihf_api_web.sihf_goalie_stats, "goalie_stats"),
    ],
)
def test_table_wrappers_return_the_documented_schema_when_empty(monkeypatch, fn, short):
    body = _load(TABLES[short])
    body["data"] = []  # a season / phase with no rows yet
    _capture(monkeypatch, body)
    df = fn(season=2026)
    assert df.height == 0
    assert df.schema == parse_sihf_table(_load(TABLES[short])).schema


@pytest.mark.parametrize(
    ("short", "fname"), [*TABLES.items(), *((s, GAME) for s in GAME_PARSERS)], ids=lambda v: str(v)
)
def test_committed_schema_matches_the_parser(short, fname):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    parser = GAME_PARSERS.get(short, parse_sihf_table)
    df = parser(_load(fname))
    assert df.columns == [c["name"] for c in schema["columns"]]


# --- wrappers -----------------------------------------------------------------


def _capture(monkeypatch: pytest.MonkeyPatch, body: Any) -> List[Dict[str, Any]]:
    calls: List[Dict[str, Any]] = []

    def fake(url: str, params: Any = None, **kw: Any) -> Any:
        calls.append({"url": url, "params": dict(params or {})})
        return body

    monkeypatch.setattr(sihf_api_web, "_get", fake)
    return calls


@pytest.mark.parametrize(
    ("fn", "alias"),
    [
        (sihf_api_web.sihf_results, "results"),
        (sihf_api_web.sihf_standings, "standing"),
        (sihf_api_web.sihf_player_stats, "player"),
        (sihf_api_web.sihf_goalie_stats, "goalkeeper"),
    ],
)
def test_table_wrappers_send_alias_and_filters(monkeypatch, fn, alias):
    calls = _capture(monkeypatch, _load(TABLES["standings"]))
    fn(season=2026, league_id=2)
    assert calls[0]["url"] == "https://data.sihf.ch/statistic/api/cms/table"
    p = calls[0]["params"]
    # the runtime moves League into searchQuery; the wrapper hands it over as a parameter
    assert (p["alias"], p["searchQuery"], p["Season"], p["League"], p["language"]) == (alias, "1//", 2026, 2, "en")
    # only standings take ContentType, and always send it (without it SIHF answers the away table)
    assert p.get("ContentType") == ("All" if alias == "standing" else None)


def test_game_wrapper_sends_game_id(monkeypatch):
    calls = _capture(monkeypatch, _load(GAME))
    df = sihf_api_web.sihf_pbp(game_id=20261105000325)
    assert calls[0]["params"]["alias"] == "gameDetail"
    assert calls[0]["params"]["searchQuery"] == 20261105000325
    assert df.height > 0
    raw = sihf_api_web.sihf_pbp(game_id=20261105000325, return_parsed=False)
    assert raw["gameId"] == "20261105000325"


def test_language_is_a_closed_set():
    with pytest.raises(ValueError, match="language"):
        sihf_api_web.sihf_results(season=2026, language="xx")


# --- live (gated) ---------------------------------------------------------------


@skip_if_no_live
def test_live_standings_and_game():
    from sportsdataverse.errors import NoDataError

    df = sihf_api_web.sihf_standings(season=2026, league_id=1)
    assert df.height == 14 and set(df["season"]) == {2026}
    assert set(df["games_played"]) == {52}  # the full table, not the away table SIHF serves by default
    pbp = sihf_api_web.sihf_pbp(game_id=20261105000421)
    assert pbp.filter(pl.col("event_type") == "goal").height == 5
    with pytest.raises(NoDataError):
        sihf_api_web.sihf_results(season=1890)
    with pytest.raises(NoDataError):
        sihf_api_web.sihf_standings(season=2026, league_id=999)


@skip_if_no_live
def test_live_current_season():
    # 2026-27: the season that answered 400 MissingLeagueId before the league moved into searchQuery
    results = sihf_api_web.sihf_results(season=2027)
    assert results.height > 300 and set(results["season"]) == {2027}
    assert sihf_api_web.sihf_standings(season=2027).height == 14
    assert sihf_api_web.sihf_player_stats(season=2027).height > 100
