"""Offline tests for the HockeyAllsvenskan (``ha``) flat-API family: parsers, runtime and YAML.

Parsers are asserted against the real captures in ``tests/fixtures/ha/`` (refs captures,
truncated) and the full games body in ``tests/fixtures/ha/full/`` (dtypes come from that one).
The runtime is driven through fake transports (a fake POST session and ``_codegen_runtime.download``),
so the error vocabulary is exercised end to end. No network.
"""

from __future__ import annotations

import functools
import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse import _codegen_runtime
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime
from sportsdataverse.hockey.ha import ha_parsers as P
from sportsdataverse.hockey.ha import ha_runtime as R

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "ha"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "ha.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "ha"
MANUAL = ROOT / "tools" / "codegen" / "manual_column_descriptions.yaml"
#: The spec's routes, request examples and category enums, written by gen_ha.py (CI has no refs checkout).
SNAPSHOT = json.loads((FIXTURES / "spec_snapshot.json").read_text(encoding="utf-8"))
HOST = R.HOST

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_ha  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
ALL_PARSERS = [getattr(P, n) for n in P.__all__ if n.startswith("parse_")]
#: Every refs capture -> the parser that reads it (the @2005 / @2026-unplayed variants included).
CAPTURE_PARSERS = {
    "pages__matcher": P.parse_ha_schedule,
    "api__game": P.parse_ha_schedule,
    "api__play-by-play": P.parse_ha_pbp,
    "api__all-time-games-stats": P.parse_ha_games,
    "api__league-standings-all-time": P.parse_ha_standings,
    "api__all-time-skaters-stats": P.parse_ha,
    "api__all-time-goalkeepers-stats": P.parse_ha,
    "api__all-time-teams-stats": P.parse_ha_team_stats,
    "api__player-leaderboard": P.parse_ha_leaders,
    "api__all-players": P.parse_ha_players,
}


@functools.lru_cache(maxsize=1)
def _manual() -> dict:
    loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
    return yaml.load(MANUAL.read_text(encoding="utf-8"), Loader=loader)


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _parsed(short: str) -> pl.DataFrame:
    return gen_ha._frame(gen_ha.ENDPOINTS[short])


def _id_dtypes_ok(df: pl.DataFrame) -> None:
    for col, dtype in df.schema.items():
        if col.endswith("statnet_id") and col != "player_statnet_id" and not col.endswith("_player_statnet_id"):
            assert dtype == pl.String, (col, dtype)  # team StatNet ids are text: AIK, MODO
        elif col in ("player_statnet_id", "statnet_game_number") or col.endswith("cms_id"):
            assert dtype == pl.Int64, (col, dtype)
        elif col.endswith("document_id") or col.endswith("team_code"):
            assert dtype == pl.String, (col, dtype)


# --- parsers over every committed fixture -------------------------------------------------------


@pytest.mark.parametrize("short", sorted(gen_ha.ENDPOINTS))
def test_every_wrapper_parses_its_fixture(short):
    df = _parsed(short)
    assert isinstance(df, pl.DataFrame) and df.height > 0, short
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    _id_dtypes_ok(df)
    if "season" in df.columns:
        assert df.schema["season"] == pl.Int64 and df["season"].null_count() == 0


@pytest.mark.parametrize("fixture", sorted(p.name for p in FIXTURES.glob("*.json") if p.name != "spec_snapshot.json"))
def test_every_refs_capture_parses(fixture):
    df = CAPTURE_PARSERS[fixture.split("@")[0].removesuffix(".json")](_load(fixture))
    assert df.height > 0 and all(SNAKE.match(c) for c in df.columns)
    _id_dtypes_ok(df)


@pytest.mark.parametrize("short", sorted(gen_ha.ENDPOINTS))
def test_committed_schema_matches_the_parser(short):
    committed = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert committed["schema"] == f"ha_{short}"
    df = _parsed(short)
    assert [c["name"] for c in committed["columns"]] == df.columns
    typed = {"integer": pl.Int64, "numeric": pl.Float64, "logical": pl.Boolean, "character": pl.String}
    texts = _manual()[f"ha_{short}"]
    for col in committed["columns"]:
        assert df.schema[col["name"]] == typed[col["type"]], col
        assert col["description"] == "", col["name"]  # descriptions never live in schemas/**
        assert len(texts[col["name"]]) >= 15, col["name"]
    assert set(texts) == {c["name"] for c in committed["columns"]}  # no orphan, no gap


@pytest.mark.parametrize("short", sorted(gen_ha.ENDPOINTS))
def test_empty_frame_concats_with_a_populated_one(short):
    """An empty body parses to the populated frame's columns AND dtypes (the wrapper passes ``columns``)."""
    parser = gen_ha.ENDPOINTS[short][1]
    cols = [c["name"] for c in yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))["columns"]]
    full = gen_ha._frame(gen_ha.ENDPOINTS[short])
    populated = parser(_load(gen_ha.ENDPOINTS[short][2]), columns=cols)
    for empty in (parser({}, columns=cols), parser({"results": "x"}, columns=cols)):
        assert empty.schema == populated.select(cols).schema
        assert pl.concat([empty, populated], how="vertical").height == populated.height
    assert pl.concat([parser({}), full], how="vertical").height == full.height


def test_documented_columns_lead_in_schema_order():
    df = P.parse_ha_leaders(_load("api__player-leaderboard.json"), columns=["team_code", "rank", "never_sent"])
    assert df.columns[:3] == ["team_code", "rank", "never_sent"] and df.width > 3


@pytest.mark.parametrize(
    ("sent", "want"), [("13950", 13950), ("13 950", 13950), ("1,386", 1386), ("", None), (987, 987)]
)
def test_attendance_is_an_integer_count(sent, want):
    df = P.parse_ha_schedule({"data": [{"id": 1, "attendance": sent}]})
    assert df.schema["attendance"] == pl.Int64 and df["attendance"].to_list() == [want]


@pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
@pytest.mark.parametrize("payload", [{}, [], None, "x", 3, {"_request": {"season": 2026}}, {"results": "x"}])
def test_empty_or_malformed_payload_is_a_zero_row_frame(parser, payload):
    df = parser(payload)
    assert isinstance(df, pl.DataFrame) and df.height == 0


@pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
def test_empty_payload_keeps_the_documented_columns(parser):
    cols = ["season", "team_statnet_id", "player_statnet_id", "team_cms_id", "team_code", "svs_perc", "is_played"]
    df = parser({}, columns=cols)
    assert df.height == 0
    assert dict(df.schema) == {
        "season": pl.Int64,
        "team_statnet_id": pl.String,
        "player_statnet_id": pl.Int64,
        "team_cms_id": pl.Int64,
        "team_code": pl.String,
        "svs_perc": pl.Float64,
        "is_played": pl.Boolean,
    }


def test_missing_documented_column_is_added_as_null():
    df = P.parse_ha_leaders(_load("api__player-leaderboard.json"), columns=["rank", "never_sent"])
    assert df["never_sent"].null_count() == df.height


@pytest.mark.parametrize("short", ["games", "pbp", "standings", "players"])
def test_return_as_pandas(short):
    path, parser, fixture, request, *_ = gen_ha.ENDPOINTS[short]
    body = _load(fixture)
    if request is not None:
        body["_request"] = R.build_body(path, request)[1]
    out = parser(body, return_as_pandas=True)
    assert isinstance(out, pd.DataFrame) and len(out) > 0


# --- dtypes from the full body ------------------------------------------------------------------


def test_full_games_body_rates_are_float_whatever_the_wire_type():
    raw = _load("full/all-time-games-stats__2026__played.json")
    wire = {
        type(g["GameStatistics"].get("home_team", {}).get("powerplay", {}).get("efficiency")) for g in raw["results"]
    }
    assert {str, int, type(None)} <= wire  # "14.29", 0 and null in one column
    df = _parsed("games")
    assert df.height == raw["count"] == 68
    for side in ("home", "away"):
        assert df.schema[f"{side}_powerplay_efficiency"] == pl.Float64
        assert df.schema[f"{side}_shots_efficiency"] == pl.Float64
        assert df.schema[f"{side}_shots_total"] == pl.Int64
    assert df["home_powerplay_efficiency"].null_count() < df.height


# --- unplayed games -----------------------------------------------------------------------------


def test_unplayed_games_are_returned_and_flagged():
    body = _load("api__all-time-games-stats@2026-unplayed.json")
    body["_request"] = {"season": 2027}
    df = P.parse_ha_games(body)
    assert df.height == 3  # never dropped
    assert df["is_played"].to_list() == [False] * 3
    assert df["decided_in"].to_list() == [""] * 3
    assert df["attendance"].null_count() == 3 and df["period_scores"].null_count() == 3
    assert df.select(pl.col("home_score") + pl.col("away_score")).to_series().to_list() == [0, 0, 0]
    played = _parsed("games")
    assert (played["is_played"] == True).all()  # noqa: E712


# --- the id traps -------------------------------------------------------------------------------


def test_cms_games_keep_three_team_key_spaces():
    g = _parsed("game").row(0, named=True)
    assert (g["home_statnet_id"], g["away_statnet_id"]) == ("AIK", "MODO")  # StatNet
    assert (g["home_team_code"], g["away_team_code"]) == ("AIK", "MoDo")  # display
    assert g["home_team_cms_id"] == 81  # CMS
    assert (g["statnet_game_number"], g["game_cms_id"]) == (23401, 6566)
    assert (g["season"], g["cms_season"]) == (2027, "season_2026")  # CMS keys a START year


def test_teams_list_is_the_statnet_to_display_code_crosswalk():
    df = _parsed("teams")
    pairs = dict(zip(df["team_code"], df["team_statnet_id"]))
    assert pairs["MoDo"] == "MODO" and pairs["MORA"] == "MIK" and pairs["NVIF"] == "NYB" and pairs["ÖIK"] == "OSIK"


def test_standings_mixed_team_code_is_kept_apart():
    df = _parsed("standings")
    rows = {r["team_code"]: r["team_code_mixed"] for r in df.iter_rows(named=True)}
    assert rows["MoDo"] == "MODO"  # the StatNet id
    assert rows["MORA"] == "MORA" and rows["NVIF"] == "NVIF" and rows["ÖIK"] == "ÖIK"  # display codes


def test_standings_wins_are_regulation_and_team_table_wins_are_totals():
    standings, teams = _parsed("standings"), _parsed("team_stats")
    key = "team_code"
    assert standings.schema[key] == teams.schema[key]  # one key space, one dtype, before the join
    ifb = standings.join(teams, on=["season", key], how="inner").filter(pl.col(key) == "IFB").row(0, named=True)
    assert (ifb["regulation_wins"], ifb["overtime_and_shootout_wins"], ifb["wins_total"]) == (34, 7, 41)
    assert ifb["regulation_wins"] + ifb["otw"] + ifb["sow"] == ifb["wins_total"]
    assert "wins" not in standings.columns and "w" not in teams.columns


def test_player_ids_only_on_pbp_and_leaderboard():
    pbp, leaders = _parsed("pbp"), _parsed("leaders")
    assert pbp.schema["player_statnet_id"] == leaders.schema["player_statnet_id"] == pl.Int64
    assert 920 in pbp["player_statnet_id"].to_list()  # Jhonas Enroth (= SHL isa 920)
    for short in ("skater_stats", "goalie_stats", "games"):
        df = _parsed(short)
        assert not [c for c in df.columns if c.startswith("player_") and c.endswith("id")], short
    assert _parsed("skater_stats").schema["team_statnet_id"] == pl.String


def test_pbp_rows_carry_the_request_and_no_coordinates():
    df = _parsed("pbp")
    assert set(df["statnet_game_number"]) == {23401}
    assert set(df["team_statnet_id"]) <= {"AIK", "MODO"}
    assert set(df["event_type"]) == {"GoalkeeperEvent", "Shot", "Goal", "Penalty", "Timeout"}
    assert df.schema["time"] == pl.Int64 and df.schema["period"] == pl.Int64
    assert df.schema["is_penalty_shot"] == pl.Boolean
    assert not [c for c in df.columns if re.search(r"(^|_)(x|y|coord|on_ice)", c)]


def test_player_directory_ids_are_cms_ids():
    df = _parsed("players")
    assert df.schema["player_cms_id"] == pl.Int64 and set(df["team_cms_id"]) == {81}
    assert set(df["team_code"]) == {"AIK"} and "headshots" not in df.columns


# --- seasons ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("today", "end"), [(date(2026, 10, 12), 2027), (date(2027, 6, 30), 2027), (date(2027, 7, 1), 2028)]
)
def test_current_season_rolls_over_on_july_1(today, end):
    assert R.current_season(today) == end


def test_season_end_year_maps_to_the_start_year_body_key():
    assert R.body_season(2026) == (2026, "2025")
    assert R.body_season(None, today=date(2026, 10, 12)) == (2027, "2026")
    body, stamp = R.build_body("/api/all-time-skaters-stats", {"season": 2006, "categories": "goals"})
    assert body["season"] == "2005" and stamp == {"season": 2006} and body["categories"] == "goals"


@pytest.mark.parametrize("season", [2005, 1999])
def test_season_before_the_floor_is_no_data(season):
    with pytest.raises(NoDataError, match="2005/06"):
        R.body_season(season)


@pytest.mark.parametrize("season", [True, 2026.0, "x"])
def test_season_must_be_a_year(season):
    with pytest.raises(ValueError, match="END year"):
        R.body_season(season)


# --- YAML ---------------------------------------------------------------------------------------


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def test_yaml_lists_every_route():
    doc = _yaml()
    assert {e[0] for e in gen_ha.ENDPOINTS.values()} == set(SNAPSHOT["routes"])
    assert [e["short"] for e in doc["endpoints"]] == list(gen_ha.ENDPOINTS)
    assert len(doc["endpoints"]) == 11
    assert doc["getter_module"] == "sportsdataverse.hockey.ha.ha_runtime"
    assert doc["schema_prefix"] == "ha_" and doc["parser_columns"] is True


def test_yaml_categories_are_the_probed_sets():
    by = {e["short"]: e for e in _yaml()["endpoints"]}
    for short, route, n in (
        ("skater_stats", "/api/all-time-skaters-stats", 14),
        ("goalie_stats", "/api/all-time-goalkeepers-stats", 5),
        ("team_stats", "/api/all-time-teams-stats", 12),
    ):
        cat = {p["name"]: p for p in by[short]["body"]}["category"]
        assert cat["key"] == "categories" and tuple(cat["choices"]) == R.CATEGORIES[route]
        assert len(cat["choices"]) == n and cat["default"] == R.BODIES[route]["categories"]
    assert {route: tuple(enum) for route, enum in SNAPSHOT["categories"].items()} == R.CATEGORIES


def test_runtime_default_bodies_are_the_spec_examples():
    assert SNAPSHOT["request_examples"]
    gen_ha._check_bodies(SNAPSHOT["request_examples"])  # raises on any drift


def test_players_wrapper_requires_a_team():
    by = {e["short"]: e for e in _yaml()["endpoints"]}
    team = {p["name"]: p for p in by["players"]["body"]}["team"]
    assert team["key"] == "teamShortName" and team["required"] is True


def test_no_path_token_named_league_or_sport():
    for e in _yaml()["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"])), e["path"]
        fields = [*e.get("extra_params", []), *e.get("body", [])]
        assert not {"league", "sport"} & {p["name"] for p in fields}, e["short"]


# --- runtime ------------------------------------------------------------------------------------


class _Resp:
    def __init__(self, status: int, body: Any = None, text: str = None, ctype: str = "application/json"):
        self.status_code = status
        self.text = text if text is not None else json.dumps(body)
        self.headers = {"content-type": ctype}

    def json(self) -> Any:
        return json.loads(self.text)


@pytest.fixture
def transport(monkeypatch):
    """Fake POST session + fake GET ``download``: queued responses, every call recorded, no sleeps."""
    state: Dict[str, Any] = {"posts": [], "gets": [], "queue": [], "waited": []}

    def answer() -> Any:
        nxt = state["queue"].pop(0) if len(state["queue"]) > 1 else state["queue"][0]
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    class _Session:
        def post(self, url: str, json: Any = None, **kw: Any) -> Any:
            state["posts"].append({"url": url, "json": json, **kw})
            return answer()

    def download(url: str, params: Any = None, **kw: Any) -> Any:
        state["gets"].append({"url": url, "params": dict(params or {}), "session": kw.get("session")})
        return answer()

    monkeypatch.setattr(R, "_SESSION", _Session())
    monkeypatch.setattr(_codegen_runtime, "download", download)
    monkeypatch.setattr(_codegen_runtime.time, "sleep", lambda s: None)
    monkeypatch.setattr(R.time, "sleep", lambda s: None)
    monkeypatch.setattr(euro_hockey_runtime, "_polite_wait", lambda url, *a: state["waited"].append(url))
    return state


def _ok(fixture: str) -> _Resp:
    return _Resp(200, _load(fixture))


def test_runtime_posts_the_default_body_with_the_mapped_season_and_throttles(transport):
    transport["queue"] = [_Resp(200, {**_load("api__all-time-skaters-stats.json"), "count": 3})]
    body = R._get(f"{HOST}/api/all-time-skaters-stats", {"season": 2026, "categories": None, "team": None})
    sent = transport["posts"][0]
    assert sent["json"] == {**R.BODIES["/api/all-time-skaters-stats"], "season": "2025"}
    assert transport["waited"] == [f"{HOST}/api/all-time-skaters-stats"]
    assert body["_request"] == {"season": 2026}


def test_runtime_stamps_the_pbp_request(transport):
    transport["queue"] = [_ok("api__play-by-play.json")]
    req = {
        "statNetGameNumber": 23401,
        "homeStatNetId": "AIK",
        "awayStatNetId": "MODO",
        "scheduledDateTime": "2026-09-18T17:00:00.000Z",
        "playedDateTime": None,
    }
    body = R._get(f"{HOST}/api/play-by-play", req)
    assert transport["posts"][0]["json"]["statNetGameNumber"] == "23401"  # the site's string form
    assert "playedDateTime" not in transport["posts"][0]["json"]
    assert body["_request"] == {"statnet_game_number": "23401", "home_statnet_id": "AIK", "away_statnet_id": "MODO"}
    assert P.parse_ha_pbp(body)["statnet_game_number"].unique().to_list() == [23401]


def test_runtime_gets_a_game_record_by_slug(transport):
    transport["queue"] = [_ok("api__game.json")]
    body = R._get(f"{HOST}/api/game", {"slug": "20260918-aik-modo"})
    assert transport["gets"][0]["params"] == {"slug": "20260918-aik-modo"} and transport["posts"] == []
    assert "_request" not in body


def test_runtime_retries_a_502_post_then_succeeds(transport):
    transport["queue"] = [_Resp(502, {"x": 1}), _ok("api__league-standings-all-time.json")]
    R._get(f"{HOST}/api/league-standings-all-time", {"season": 2026})
    assert len(transport["posts"]) == 2 and len(transport["waited"]) == 2  # each attempt throttled


@pytest.mark.parametrize(("status", "error"), [(404, NoDataError), (400, ValueError), (502, AssetFetchError)])
def test_runtime_non_200_follows_the_vocabulary(transport, status, error):
    transport["queue"] = [_Resp(status, {"error": "Missing league or season"})]
    with pytest.raises(error):
        R._get(f"{HOST}/api/league-standings-all-time", {"season": 2026})


@pytest.mark.parametrize("status", [429, 503])
def test_runtime_honours_retry_after(transport, monkeypatch, status):
    slept: List[float] = []
    monkeypatch.setattr(R.time, "sleep", slept.append)
    busy = _Resp(status, {"x": 1})
    busy.headers["Retry-After"] = "7"
    transport["queue"] = [busy, _Resp(status, {"x": 1}), _ok("api__league-standings-all-time.json")]
    R._get(f"{HOST}/api/league-standings-all-time", {"season": 2026})
    assert slept == [7.0, 4.0]  # the server's wait, then the plain backoff
    assert len(transport["waited"]) == 3


def test_runtime_transport_failure_is_asset_fetch_error(transport):
    transport["queue"] = [ConnectionError("reset")]
    with pytest.raises(AssetFetchError, match="fetch failed"):
        R._get(f"{HOST}/api/league-standings-all-time", {"season": 2026})
    assert len(transport["posts"]) == 3


@pytest.mark.parametrize(
    ("route", "params", "match"),
    [
        ("/api/all-time-skaters-stats", {"categories": "all"}, "categories must be one of"),
        ("/api/all-time-teams-stats", {"categories": "summary"}, "categories must be one of"),
        ("/api/all-time-goalkeepers-stats", {"categories": "points"}, "categories must be one of"),
        ("/api/league-standings-all-time", {"phase": "SHL"}, "phase must be one of"),
        ("/api/player-leaderboard", {"metric": ""}, "metric must be"),
        ("/api/all-players", {"teamShortName": None}, "team is required"),
    ],
)
def test_runtime_refuses_bad_fields_before_sending(transport, route, params, match):
    transport["queue"] = [_Resp(200, {})]
    with pytest.raises(ValueError, match=match):
        R._get(HOST + route, params)
    assert transport["posts"] == []


def test_runtime_refuses_a_season_before_the_floor_without_a_request(transport):
    transport["queue"] = [_Resp(200, {})]
    with pytest.raises(NoDataError):
        R._get(f"{HOST}/api/all-time-teams-stats", {"season": 2005})
    assert transport["posts"] == []


@pytest.mark.parametrize(
    ("route", "params", "body", "error", "match"),
    [
        # a wrong category / empty season answers 200 with zero rows
        ("/api/all-time-skaters-stats", {}, {"count": 0, "total_pages": 0, "results": []}, NoDataError, "no rows"),
        ("/api/all-time-teams-stats", {}, {"data": [], "sort_by_options": []}, NoDataError, "no rows"),
        ("/api/league-standings-all-time", {}, {"standings": []}, NoDataError, "no rows"),
        # error envelopes and wrong shapes in a 200
        ("/api/league-standings-all-time", {}, {"error": "StatNet API error"}, AssetFetchError, "error envelope"),
        ("/api/league-standings-all-time", {}, ["x"], AssetFetchError, "not an object"),
        ("/api/league-standings-all-time", {}, {"standings": ["x"]}, AssetFetchError, "list of objects"),
    ],
)
def test_runtime_refuses_empty_and_error_bodies(transport, route, params, body, error, match):
    transport["queue"] = [_Resp(200, body)]
    with pytest.raises(error, match=match):
        R._get(HOST + route, {"season": 2026, **params})


def test_runtime_refuses_an_empty_200(transport):
    transport["queue"] = [_Resp(200, text="")]
    with pytest.raises(AssetFetchError, match="empty body"):
        R._get(f"{HOST}/api/league-standings-all-time", {"season": 2026})


@pytest.mark.parametrize(
    "patch",
    [{"total_pages": 2}, {"count": 999}, {"total_pages": True}, {"count": None}],
)
def test_runtime_refuses_a_truncated_page(transport, patch):
    body = {**_load("api__all-time-goalkeepers-stats.json"), "count": 3, "total_pages": 1, **patch}
    transport["queue"] = [_Resp(200, body)]
    with pytest.raises(AssetFetchError, match="truncated"):
        R._get(f"{HOST}/api/all-time-goalkeepers-stats", {"season": 2026})


def test_runtime_returns_unplayed_games_and_refuses_out_of_season_rows(transport):
    body = {**_load("api__all-time-games-stats@2026-unplayed.json"), "count": 3, "total_pages": 1}
    transport["queue"] = [_Resp(200, body)]
    raw = R._get(f"{HOST}/api/all-time-games-stats", {"season": 2027})
    assert P.parse_ha_games(raw)["is_played"].to_list() == [False] * 3
    transport["queue"] = [_Resp(200, body)]
    with pytest.raises(AssetFetchError, match="outside season 2026"):
        R._get(f"{HOST}/api/all-time-games-stats", {"season": 2026})
    bad = {**body, "results": [{**body["results"][0], "decided_in": None}]}
    bad["count"] = 1
    transport["queue"] = [_Resp(200, bad)]
    with pytest.raises(AssetFetchError, match="decided_in"):
        R._get(f"{HOST}/api/all-time-games-stats", {"season": 2027})


@pytest.mark.parametrize(
    "patch",
    [
        {"totalPages": 2},
        {"totalCount": 25},  # 3 rows in the capture: the board was cut
        {"totalPages": True},
    ],
)
def test_runtime_refuses_a_truncated_leaderboard(transport, patch):
    body = {**_load("api__player-leaderboard.json"), "totalCount": 3, "totalPages": 1, **patch}
    transport["queue"] = [_Resp(200, body)]
    with pytest.raises(AssetFetchError, match="truncated board"):
        R._get(f"{HOST}/api/player-leaderboard", {"metric": "TP"})


@pytest.mark.parametrize("row_metric", ["G", None, ["TP"], 1])
def test_runtime_checks_the_leaderboard_metric_type_strictly(transport, row_metric):
    body = {**_load("api__player-leaderboard.json"), "totalCount": 3, "totalPages": 1}
    body["players"] = [{**r, "metric": row_metric} if i == 1 else r for i, r in enumerate(body["players"])]
    transport["queue"] = [_Resp(200, body)]
    with pytest.raises(AssetFetchError, match="outside metric"):
        R._get(f"{HOST}/api/player-leaderboard", {"metric": "TP"})


def test_runtime_accepts_a_complete_leaderboard(transport):
    body = {**_load("api__player-leaderboard.json"), "totalCount": 3, "totalPages": 1}
    transport["queue"] = [_Resp(200, body)]
    R._get(f"{HOST}/api/player-leaderboard", {"metric": "TP"})
    assert transport["posts"][0]["json"]["pageSize"] == 100 and transport["posts"][0]["json"]["team"] is None


@pytest.mark.parametrize(
    ("patch", "match"),
    [
        ({"pagination": {"page": 1, "pageSize": 100, "pageCount": 2, "total": 130}}, "multi-page"),
        ({"pagination": {"page": 1, "pageSize": 100, "pageCount": 1, "total": 29}}, "multi-page"),
    ],
)
def test_runtime_refuses_a_multi_page_directory(transport, patch, match):
    transport["queue"] = [_Resp(200, {**_load("api__all-players.json"), **patch})]
    with pytest.raises(AssetFetchError, match=match):
        R._get(f"{HOST}/api/all-players", {"teamShortName": "AIK"})


def test_runtime_directory_rows_must_be_the_requested_team(transport):
    body = _load("api__all-players.json")
    body["pagination"] = {"page": 1, "pageSize": 100, "pageCount": 1, "total": 3}
    transport["queue"] = [_Resp(200, body)]
    R._get(f"{HOST}/api/all-players", {"teamShortName": "AIK"})
    assert transport["posts"][0]["json"]["teamShortName"] == "AIK"
    with pytest.raises(AssetFetchError, match="outside teamShortName"):
        R._get(f"{HOST}/api/all-players", {"teamShortName": "MORA"})


def test_runtime_refuses_pbp_events_of_another_team(transport):
    req = {"statNetGameNumber": "1", "homeStatNetId": "AIK", "awayStatNetId": "MIK", "scheduledDateTime": "x"}
    transport["queue"] = [_ok("api__play-by-play.json")]  # AIK v MODO
    with pytest.raises(AssetFetchError, match="outside"):
        R._get(f"{HOST}/api/play-by-play", req)
    transport["queue"] = [_Resp(200, {"game_info": {}, "game_events": [{"Period": "1", "Events": []}]})]
    with pytest.raises(NoDataError, match="no play-by-play"):
        R._get(f"{HOST}/api/play-by-play", req)
    transport["queue"] = [_Resp(404, {"error": "StatNet API error: 404"})]
    with pytest.raises(NoDataError):
        R._get(f"{HOST}/api/play-by-play", req)


def test_runtime_game_record_refusals(transport):
    transport["queue"] = [_Resp(200, {"data": [], "meta": {}})]
    with pytest.raises(NoDataError, match="2025/26"):
        R._get(f"{HOST}/api/game", {"slug": "20250101-aik-modo"})
    transport["queue"] = [_ok("api__game.json")]
    with pytest.raises(AssetFetchError, match="outside slug"):
        R._get(f"{HOST}/api/game", {"slug": "20260919-mik-aik"})


# --- the schedule page (HTML) -------------------------------------------------------------------


def _page(props: Dict[str, Any], split: int = 40) -> str:
    """A Next.js page whose game-calendar element is split across two flight chunks."""
    text = '0:["$","$L4c","games.game-calendar-265-0",' + json.dumps(props) + "]\n"
    chunks: List[str] = [text[:split], text[split:]]
    pushes = "".join(f"<script>self.__next_f.push([1,{json.dumps(c)}])</script>" for c in chunks)
    return f"<html><body>{pushes}</body></html>"


def test_schedule_page_decodes_the_split_flight_element(transport):
    props = _load("pages__matcher.json")
    transport["queue"] = [_Resp(200, text=_page(props), ctype="text/html; charset=utf-8")]
    body = R._get(f"{HOST}/pages/matcher")
    assert body == props and transport["gets"][0]["session"] is euro_hockey_runtime.SESSION  # waits per attempt
    assert P.parse_ha_schedule(body).height == 3


@pytest.mark.parametrize(
    ("text", "ctype", "error", "match"),
    [
        ("<html>no flight</html>", "text/html", AssetFetchError, "no self.__next_f"),
        (_page({"x": 1}).replace("games.game-calendar", "games.other"), "text/html", AssetFetchError, "no games"),
        (_page({"season": "current", "games": "x"}), "text/html", AssetFetchError, "no games list"),
        (_page({"season": "current", "games": []}), "text/html", NoDataError, "no games"),
        ('{"games": []}', "application/json", AssetFetchError, "not text/html"),
    ],
)
def test_schedule_page_refusals(transport, text, ctype, error, match):
    transport["queue"] = [_Resp(200, text=text, ctype=ctype)]
    with pytest.raises(error, match=match):
        R._get(f"{HOST}/pages/matcher")


def test_schedule_page_refuses_mixed_seasons(transport):
    props = _load("pages__matcher.json")
    props["games"][1] = {**props["games"][1], "season": "season_2025"}
    transport["queue"] = [_Resp(200, text=_page(props), ctype="text/html")]
    with pytest.raises(AssetFetchError, match="mixes seasons"):
        R._get(f"{HOST}/pages/matcher")
