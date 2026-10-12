"""eSports hokejovyzapis families (del_esports, icehl_esports, eihl_esports, metal_esports): parsers,
endpoint YAMLs, returns-schemas and the runtime's season map and refusals. Offline, on committed fixtures."""

from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse import _codegen_runtime
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import esports_parsers as P
from sportsdataverse.hockey import esports_runtime as rt
from sportsdataverse.hockey import euro_hockey_runtime

ROOT = Path(__file__).resolve().parents[2]
FIX = ROOT / "tests" / "fixtures"
CODEGEN = ROOT / "tools" / "codegen"
STEMS = ("del_esports", "icehl_esports", "eihl_esports", "metal_esports")
PARSERS = [getattr(P, name) for name in P.__all__]
_SNAKE = re.compile(r"^[a-z0-9_]+$")
_ID = re.compile(r"(^|_)id$")


def _slug(path: str) -> str:
    return path.strip("/").replace("/", "__").replace("{", "").replace("}", "").removesuffix(".json")


def _yaml(stem):
    return yaml.safe_load((CODEGEN / "endpoints" / f"{stem}.yaml").read_text(encoding="utf-8"))


def _load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


ROUTES = [(stem, ep) for stem in STEMS for ep in _yaml(stem)["endpoints"]]


def _id(param):
    stem, ep = param
    return f"{stem}-{ep['short']}"


def _parser(ep):
    return getattr(P, ep["parser"])


@pytest.mark.parametrize("route", ROUTES, ids=_id)
def test_every_capture_parses_to_rows_with_snake_case_columns_and_int64_ids(route):
    stem, ep = route
    df = _parser(ep)(_load(FIX / stem / f"{_slug(ep['path'])}.json"))
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert all(_SNAKE.match(c) for c in df.columns), df.columns
    ids = {c: df.schema[c] for c in df.columns if _ID.search(c)}
    assert ids and all(t == pl.Int64 for t in ids.values()), ids


def _rtype(dtype) -> str:
    if dtype == pl.Boolean:
        return "logical"
    if dtype.is_integer():
        return "integer"
    return "numeric" if dtype.is_float() else "character"


FULL = sorted(FIX.glob("*_esports/full/*.json"))


@pytest.mark.parametrize("body", FULL, ids=lambda p: f"{p.parts[-3]}-{p.stem}")
def test_returns_schema_names_and_dtypes_come_from_the_full_body(body):
    stem = body.parts[-3]
    ep = next(e for e in _yaml(stem)["endpoints"] if _slug(e["path"]) == body.stem)
    df = _parser(ep)(_load(body))
    schema = yaml.safe_load((CODEGEN / "schemas" / f"{ep['returns_schema']}.yaml").read_text(encoding="utf-8"))
    assert [(c["name"], c["type"]) for c in schema["columns"]] == [(n, _rtype(t)) for n, t in df.schema.items()]


def test_every_family_has_one_full_body():
    assert {p.parts[-3] for p in FULL} == set(STEMS)


@pytest.mark.parametrize("stem", STEMS)
def test_yaml_lists_every_captured_route_and_only_those(stem):
    paths = {_slug(e["path"]) for e in _yaml(stem)["endpoints"]}
    assert paths == {p.stem for p in (FIX / stem).glob("*.json")}


def test_per_bucket_path_availability():
    paths = {stem: {e["path"] for e in _yaml(stem)["endpoints"]} for stem in STEMS}
    assert "/visualization/shots/{match_id}.json" not in paths["metal_esports"]  # Denmark: empty shells only
    for path in ("/matches/{match_id}/roster.json", "/matches/{match_id}/shiftsSC.json"):
        assert {s for s in STEMS if path in paths[s]} == {"del_esports"}
    assert "/matches/{match_id}/game-info.json" in paths["eihl_esports"]
    assert "/matches/{match_id}/game-header.json" not in paths["eihl_esports"]
    assert "/tables/{season}/{league_id}.json" in paths["del_esports"]
    assert "/table/{season}/{league_id}.json" in paths["metal_esports"]
    # every family names its game file and standings the same, whatever the bucket's path
    for stem in STEMS:
        assert {"game_info", "standings", "pbp", "schedule"} <= {e["short"] for e in _yaml(stem)["endpoints"]}


@pytest.mark.parametrize("stem", STEMS)
def test_family_yaml_contract(stem):
    doc = _yaml(stem)
    assert doc["getter_module"] == "sportsdataverse.hockey.esports_runtime"
    assert doc["host"].startswith("https://s3.eu-west-1.amazonaws.com/") and doc["host"].endswith(".hokejovyzapis.cz")
    assert doc["name_pattern"] == f"{doc['schema_prefix']}{{short}}"
    for ep in doc["endpoints"]:
        names = {p["name"] for p in ep.get("path_params", [])}
        assert not {"league", "sport"} & names
        assert ep["summary"].endswith(".") and "{" not in ep["summary"]
        for p in ep.get("path_params", []):
            if p["name"] == "season":
                assert p["transform"] == "season_start_year"
                assert ep["example_args"]["season"] == 2027  # END year -> bucket key 2026


def test_league_id_defaults_to_each_buckets_main_competition():
    want = {"del_esports": 1, "icehl_esports": 1, "eihl_esports": 3, "metal_esports": 1}
    for stem, league in want.items():
        defaults = {
            p["default"] for e in _yaml(stem)["endpoints"] for p in e.get("path_params", []) if p["name"] == "league_id"
        }
        assert defaults == {league}


# ------------------------------------------------------------------ parser contract


@pytest.mark.parametrize("parser", PARSERS, ids=lambda f: f.__name__)
@pytest.mark.parametrize("payload", [None, {}, [], "", "x", 7, [1, 2, 3]], ids=repr)
def test_unusable_payload_is_a_zero_row_frame(parser, payload):
    df = parser(payload)
    assert isinstance(df, pl.DataFrame) and df.height == 0


@pytest.mark.parametrize("parser", PARSERS, ids=lambda f: f.__name__)
def test_zero_row_frame_carries_the_documented_columns(parser):
    df = parser(None, columns=["match_id", "xg", "name", "overtime_count"])
    assert df.schema == {"match_id": pl.Int64, "xg": pl.Float64, "name": pl.String, "overtime_count": pl.Int64}


@pytest.mark.parametrize(
    "payload",
    [
        [{"id": 7}, {"id": "8"}],
        [{"x": [1, 2]}, {"x": 5}],
        [{"x": {"a": 1}}, {"x": 5}],
        [{"HomeTeam": 1, "homeTeam": 2}],
    ],
    ids=["one-key-two-types", "list-and-scalar", "dict-and-scalar", "names-collide"],
)
@pytest.mark.parametrize("parser", PARSERS, ids=lambda f: f.__name__)
def test_heterogeneous_body_is_a_frame_not_an_exception(parser, payload):
    df = parser(payload)
    assert isinstance(df, pl.DataFrame)
    if parser is P.parse_esports:
        assert df.height == len(payload)


def test_return_as_pandas():
    raw = _load(FIX / "icehl_esports" / "full" / "visualization__shots__match_id.json")
    df = P.parse_esports_shots(raw, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame) and len(df) == 78


# ------------------------------------------------------------------ shapers


def _full(stem, name):
    return _load(FIX / stem / "full" / f"{name}.json")


def test_shots_ids_codes_and_xg():
    for stem, xg in (("del_esports", False), ("icehl_esports", True), ("eihl_esports", True)):
        df = P.parse_esports_shots(_full(stem, "visualization__shots__match_id"))
        assert df.schema["match_id"] == pl.Int64 and df["match_id"].n_unique() == 1  # "4444" on the wire
        assert df["shot_result_id"].to_list() == df["match_shot_resutl_id"].to_list()
        assert df["is_goal"].to_list() == [c == 4 for c in df["shot_result_id"].to_list()]
        assert df.schema["xg"] == pl.Float64 and df.schema["coordinate_x"] == pl.Float64
        assert (df["xg"].null_count() == 0) is xg and (df["xg"].null_count() == df.height) is (not xg)
        # x runs along the rink length, home positive: each side keeps one sign all game
        home, away = df.filter(pl.col("team_side") == "home"), df.filter(pl.col("team_side") == "visitor")
        assert home.height + away.height == df.height
        wrong = home.filter(pl.col("coordinate_x") < 0).vstack(away.filter(pl.col("coordinate_x") > 0))
        # one known exception: an EIHL 4440 visitor goal recorded at x = +70 (shot 786469)
        assert wrong["shot_id"].to_list() == ([786469] if stem == "eihl_esports" else [])


def test_shot_player_id_is_playerid_space_not_matchplayerid():
    shots = P.parse_esports_shots(_full("del_esports", "visualization__shots__match_id"))
    pbp = P.parse_esports_pbp(_full("del_esports", "matches__match_id__period-events"))
    goals = pbp.filter(pl.col("event_type") == "goal").select(pl.col("scorer_player_id").alias("player_id"))
    scored = shots.filter(pl.col("is_goal") == True).select("player_id")  # noqa: E712
    assert goals.schema["player_id"] == scored.schema["player_id"] == pl.Int64
    assert sorted(goals["player_id"].to_list()) == sorted(scored["player_id"].to_list())


def test_pbp_flattens_periods_and_penalty_windows():
    df = P.parse_esports_pbp(_full("del_esports", "matches__match_id__period-events"))
    assert df["event_index"].to_list() == list(range(df.height))
    assert df["period_number"].to_list() == sorted(df["period_number"].to_list())
    assert df.schema["time"] == pl.Int64  # DEL writes periodEnd times as 1200.0
    assert df.filter(pl.col("event_type") == "goal").height == 7
    pen = df.filter(pl.col("event_type") == "penalty")
    assert pen.height == 8 and pen["penalty_from_scoreboard_time"].null_count() == 0
    assert pen["penalty_to_real_time"].null_count() == 0


def test_pbp_overtime_and_shootout_keys():
    raw = {
        "1": [{"time": 5, "type": "goal"}],
        "overtime": [{"time": 3700, "type": "goal"}],
        "shootout": [{"time": 3900, "type": "goal"}],
    }
    df = P.parse_esports_pbp(raw)
    assert df.select("period", "period_number").rows() == [("1", 1), ("OT", 4), ("SO", 5)]


def test_schedule_season_is_the_end_year_and_round_has_no_bom():
    df = P.parse_esports_schedule(_full("del_esports", "league-matches__season__league_id"))
    assert df["season"].unique().to_list() == [2027]  # body "2026" = 2026/27
    assert df.height == 364 and df["match_id"].n_unique() == 364
    assert not df["round"].str.contains("﻿").any()
    assert df.schema["overtime_count"] == pl.Int64


def test_standings_both_shapes():
    de = P.parse_esports_standings(_load(FIX / "del_esports" / "tables__season__league_id.json"))
    dk = P.parse_esports_standings(_load(FIX / "metal_esports" / "table__season__league_id.json"))
    assert {"league_part_id", "team_id", "points"} <= set(de.columns)
    assert {"group_name", "team_id", "points"} <= set(dk.columns)


def test_player_box_sums_the_venue_split():
    df = P.parse_esports_player_stats(_load(FIX / "del_esports" / "matches__match_id__team-stats__team_id.json"))
    assert df.columns[0] == "player_id"
    assert df["goals"].to_list() == [h + a for h, a in zip(df["goals_home"].to_list(), df["goals_away"].to_list())]


def test_lineup_reads_roster_json_and_the_game_header_roster():
    de = P.parse_esports_lineup(_load(FIX / "del_esports" / "matches__match_id__roster.json"))
    ice = P.parse_esports_lineup(_load(FIX / "icehl_esports" / "matches__match_id__game-header.json"))
    for df in (de, ice):
        assert {"team_side", "slot", "player_id", "match_player_id"} <= set(df.columns)
        assert df.schema["player_id"] == df.schema["match_player_id"] == pl.Int64
    info = P.parse_esports_game_info(_load(FIX / "icehl_esports" / "matches__match_id__game-header.json"))
    assert info.height == 1 and not any(c.startswith("roster") for c in info.columns)


# ------------------------------------------------------------------ runtime


def test_season_start_year():
    assert rt.season_start_year(2027) == 2026
    assert rt.season_start_year("2017") == 2016
    with pytest.raises(ValueError):
        rt.season_start_year(27)


@pytest.fixture
def serve(monkeypatch):
    """Answer every request with ``(status, text)``; record each attempt sent over the throttled session."""
    waited = []

    def install(status, text):
        if not isinstance(text, str):
            text = json.dumps(text)

        def download(url, **kw):
            if kw.get("session") is euro_hockey_runtime.SESSION:  # SESSION waits for the host before every attempt
                waited.append(url)
            return SimpleNamespace(status_code=status, text=text, headers={}, json=lambda: json.loads(text))

        monkeypatch.setattr(_codegen_runtime, "download", download)
        return waited

    return install


B = "https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz"
_XML = '<?xml version="1.0" encoding="UTF-8"?><Error><Code>{}</Code><Message>m</Message></Error>'


def test_runtime_returns_a_good_body_after_the_throttle(serve):
    body = _load(FIX / "metal_esports" / "league-matches__season__league_id.json")
    waited = serve(200, body)
    assert rt._get(f"{B}/league-matches/2026/1.json") == body
    assert waited == [f"{B}/league-matches/2026/1.json"]


@pytest.mark.parametrize(
    "url,status,body,error",
    [
        (f"{B}/matches/1/period-events.json", 404, _XML.format("NoSuchKey"), NoDataError),
        (f"{B}/matches/1/period-events.json", 403, _XML.format("AccessDenied"), AssetFetchError),
        (f"{B}/matches/1/period-events.json", 200, _XML.format("NoSuchKey"), AssetFetchError),
        (f"{B}/matches/1/top-goalies.json", 200, {"error": "x", "message": "y"}, AssetFetchError),
        (f"{B}/matches/1/top-goalies.json", 200, [], NoDataError),
        (f"{B}/matches/1/top-goalies.json", 200, {}, NoDataError),
        (f"{B}/league-matches/2026/1.json", 200, {"matches": []}, NoDataError),
        (f"{B}/league-matches/2026/1.json", 200, {"matches": [{"id": 1, "season": "2025"}]}, AssetFetchError),
        (
            f"{B}/visualization/shots/575.json",
            200,
            {"match": {"id": "575", "shots": [], "status": "LIVE"}},
            NoDataError,
        ),
        (f"{B}/visualization/shots/575.json", 200, {"match": {"id": "576", "shots": [{"id": 1}]}}, AssetFetchError),
        (f"{B}/widget/esports/match/575.json", 200, {"data": {"gameData": {"id": 576}}}, AssetFetchError),
        (
            f"{B}/matches/575/period-events.json",
            200,
            {"periods": {"1": {"start": None}}, "1": [], "2": [], "3": [], "overtime": [], "shootout": []},
            NoDataError,
        ),
    ],
    ids=[
        "404-NoSuchKey",
        "403-AccessDenied",
        "200-xml",
        "error-envelope",
        "empty-list",
        "empty-object",
        "schedule-no-matches",
        "schedule-other-season",
        "denmark-shot-shell",
        "shots-other-match",
        "widget-other-match",
        "pbp-no-events",
    ],
)
def test_runtime_refusals(serve, url, status, body, error):
    serve(status, body)
    with pytest.raises(error):
        rt._get(url)
