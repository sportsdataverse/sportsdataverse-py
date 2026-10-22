"""Offline tests for the Liiga (``liiga``) flat-API family: parsers, runtime and YAML.

Parsers are asserted against the real captures in ``tests/fixtures/liiga/`` (refs captures,
truncated) and the two full bodies in ``tests/fixtures/liiga/full/`` (dtypes come from those).
The runtime is driven through a fake transport (``_codegen_runtime.download``), so the error
vocabulary is exercised end to end. No network.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse import _codegen_runtime
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime
from sportsdataverse.hockey.liiga import liiga_parsers as P
from sportsdataverse.hockey.liiga import liiga_runtime as R

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "liiga"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "liiga.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "liiga"
MANUAL = ROOT / "tools" / "codegen" / "manual_column_descriptions.yaml"
HOST = "https://liiga.fi/api/v2"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_liiga  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
ALL_PARSERS = [getattr(P, n) for n in P.__all__ if n.startswith("parse_")]


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _parsed(short: str) -> pl.DataFrame:
    return gen_liiga._frame(gen_liiga.ENDPOINTS[short])


def _id_cols(df: pl.DataFrame) -> List[str]:
    return [c for c in df.columns if c == "id" or c.endswith("_id")]


# --- parsers over every committed fixture ------------------------------------------------------


@pytest.mark.parametrize("short", sorted(gen_liiga.ENDPOINTS))
def test_every_wrapper_parses_its_fixture(short):
    df = _parsed(short)
    assert isinstance(df, pl.DataFrame) and df.height > 0, short
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    assert {c: df.schema[c] for c in _id_cols(df)} == {c: pl.Int64 for c in _id_cols(df)}
    if "season" in df.columns:
        assert df.schema["season"] == pl.Int64 and df["season"].null_count() == 0


@pytest.mark.parametrize("fixture", sorted(p.name for p in FIXTURES.glob("*__bool__*.json")))
def test_every_player_data_type_parses(fixture):
    df = P.parse_liiga(_load(fixture))
    assert df.height == 3 and df.schema["player_id"] == pl.Int64 and df.schema["team_id"] == pl.Int64


@pytest.mark.parametrize("fixture", sorted(p.name for p in FIXTURES.glob("teams__stats__*.json")))
def test_every_team_data_type_parses(fixture):
    df = P.parse_liiga(_load(fixture))
    assert df.height == 3 and df.schema["team_id"] == pl.Int64


_SCHEMA_DTYPE = {"integer": pl.Int64, "numeric": pl.Float64, "logical": pl.Boolean, "character": pl.String}


def _schema(short: str) -> Dict[str, Any]:
    return yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))


def _body(short: str) -> Any:
    entry = gen_liiga.ENDPOINTS[short]
    body = json.loads((FIXTURES / entry[3]).read_text(encoding="utf-8"))
    return body if entry[4] is None else R._stamp(HOST + entry[4][0], entry[4][1], body)


@pytest.mark.parametrize("short", sorted(gen_liiga.ENDPOINTS))
def test_committed_schema_matches_the_parser(short):
    committed = _schema(short)
    df = _parsed(short)
    assert [c["name"] for c in committed["columns"]] == df.columns
    assert {c["name"]: _SCHEMA_DTYPE[c["type"]] for c in committed["columns"]} == dict(df.schema)


@pytest.mark.parametrize("short", sorted(gen_liiga.ENDPOINTS))
@pytest.mark.parametrize("empty", [{}, [], None])
def test_empty_frame_stacks_onto_the_populated_one(short, empty):
    # Parser SHOULD 1: an empty payload carries the populated frame's dtypes (from the returns-schema).
    parser = gen_liiga.ENDPOINTS[short][2]
    cols = [c["name"] for c in _schema(short)["columns"]]
    full, none = parser(_body(short), columns=cols), parser(empty, columns=cols)
    assert none.height == 0 and dict(none.schema) == dict(full.schema)
    assert pl.concat([none, full], how="vertical").height == full.height
    assert pl.concat([parser(empty), parser(_body(short))], how="vertical").height == full.height


@pytest.mark.parametrize("short", sorted(gen_liiga.ENDPOINTS))
def test_dtype_module_is_the_returns_schema(short):
    parser = gen_liiga.ENDPOINTS[short][2].__name__
    for col in _schema(short)["columns"]:
        assert P._dtype_for(col["name"], P.DTYPES[parser]) == _SCHEMA_DTYPE[col["type"]], (parser, col)


def test_all_null_column_takes_the_schema_dtype():
    # home_time_out is an integer second when sent; a body where nobody called a timeout stays Int64.
    games = [
        {**g, "homeTeam": {**g["homeTeam"], "timeOut": None}} for g in _load("full/games__playoffs__2026__3games.json")
    ]
    df = P.parse_liiga_games(games)
    assert df.schema["home_time_out"] == pl.Int64 and df["home_time_out"].null_count() == df.height


@pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
@pytest.mark.parametrize("payload", [{}, [], None, "x", 3, {"_request": {"season": 2026}}, {"data": None}])
def test_empty_or_malformed_payload_is_a_zero_row_frame(parser, payload):
    df = parser(payload)
    assert isinstance(df, pl.DataFrame) and df.height == 0


@pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
def test_empty_payload_keeps_the_documented_columns(parser):
    df = parser([], columns=["season", "game_id", "team_slug", "expected_goals", "goals"])
    assert df.height == 0
    assert dict(df.schema) == {
        "season": pl.Int64,
        "game_id": pl.Int64,
        "team_slug": pl.String,
        "expected_goals": pl.Float64,
        "goals": pl.Int64,
    }


def test_missing_documented_column_is_added_as_null():
    df = P.parse_liiga_shotmap(_load("shotmap__season__id.json"), columns=["shot_x", "xg_never_sent"])
    assert df["xg_never_sent"].null_count() == df.height


@pytest.mark.parametrize("short", ["schedule", "game_stats", "standings"])
def test_return_as_pandas(short):
    entry = gen_liiga.ENDPOINTS[short]
    body = json.loads((FIXTURES / entry[3]).read_text(encoding="utf-8"))
    if entry[4] is not None:
        body = R._stamp(HOST + entry[4][0], entry[4][1], body)
    out = entry[2](body, return_as_pandas=True)
    assert isinstance(out, pd.DataFrame) and len(out) > 0


# --- the team-id trap ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "want"),
    [
        ("55786244:hpk", (55786244, "hpk")),
        (55786244, (55786244, None)),
        ("495643563", (495643563, None)),
        ("zzz:bogus", (None, None)),
        (None, (None, None)),
        (True, (None, None)),
    ],
)
def test_split_team_id_reads_all_three_forms(value, want):
    assert P._split_team_id(value) == want


def test_three_team_id_forms_normalise_to_one_int64_key():
    schedule = _parsed("schedule")  # "55786244:hpk"
    shots = _parsed("shotmap")  # bare integer
    teams = _parsed("teams")  # digit string, keyed "<num>:<slug>"
    assert schedule.schema["home_team_id"] == shots.schema["shooting_team_id"] == teams.schema["team_id"] == pl.Int64
    assert schedule.row(0, named=True)["home_team_id"] == 55786244
    assert schedule.row(0, named=True)["home_team_slug"] == "hpk"
    assert set(shots["shooting_team_id"]) <= {55786244, 651304385}
    assert teams.filter(pl.col("team_slug") == "hifk")["team_id"].to_list() == [168761288]
    # the forms join: game 1's two teams (string form) against its shots (integer form)
    game1 = schedule.filter(pl.col("game_id") == 1)
    pair = pl.concat([game1.select(pl.col(f"{s}_team_id").alias("shooting_team_id")) for s in ("home", "away")])
    assert pair.schema["shooting_team_id"] == shots.schema["shooting_team_id"]
    assert shots.join(pair, on="shooting_team_id").height == shots.height


def test_string_team_ids_keep_their_slug_and_integer_forms_have_none():
    shootout = _parsed("game_shootout")
    assert {"shooting_team_slug", "blocking_team_slug"} <= set(shootout.columns)
    assert not any(c.endswith("_slug") for c in _parsed("shotmap").columns)


# --- full-body dtypes ---------------------------------------------------------------------------


def test_box_score_rates_are_float_even_when_sent_whole():
    df = _parsed("game_stats")
    for col in (
        "distance",
        "expected_goals_player",
        "expected_goals_team",
        "hardest_shot",
        "top_speed",
        "saves_percentage",
    ):
        assert df.schema[col] == pl.Float64, col
    assert df.schema["faceoffs_total"] == pl.Int64  # null for every player of this game
    goalie = df.filter((pl.col("is_goalie") == True) & (pl.col("player_id") == 40107053) & (pl.col("period") == 1))
    assert goalie["saves_percentage"].to_list() == [92.86]


def test_box_score_is_one_row_per_player_and_period_with_tracking():
    df = _parsed("game_stats")
    assert df.select("player_id", "period").is_duplicated().sum() == 0
    assert df.filter(pl.col("is_goalie") == False)["top_speed"].null_count() < df.height
    assert set(df["season"]) == {2026} and set(df["game_id"]) == {1}
    assert df.schema["jersey"] == pl.Int64 and "jersey_id" not in df.columns


def test_game_xg_sent_as_integer_is_float():
    games = _parsed("games")
    assert games.schema["home_expected_goals"] == games.schema["away_expected_goals"] == pl.Float64
    assert games.filter(pl.col("game_id") == 55116)["away_expected_goals"].to_list() == [3.0]
    assert "goal_events" not in " ".join(games.columns)


@pytest.mark.parametrize("bad", [[1], {"n": 1}, [[1]]])
def test_team_box_score_tolerates_unhashable_period_numbers(bad):
    body = _body("game_team_stats")
    body = {**body, "puckStats": [{**p, "periodNumber": bad} for p in body["puckStats"]]}
    body["homeTeam"] = [{**t, "period": bad} for t in body["homeTeam"]]
    df = P.parse_liiga_game_team_stats(body)
    assert df.height == 10 and df.filter(pl.col("side") == "away")["puck_control_duration"].null_count() == 5


def test_team_box_score_carries_puck_possession():
    df = _parsed("game_team_stats")
    row = df.filter((pl.col("side") == "home") & (pl.col("period") == 1)).row(0, named=True)
    assert row["puck_control_duration"] == 284.96 and row["opponent_puck_control_duration"] == 397.04


# --- shapers ------------------------------------------------------------------------------------


def test_goals_flatten_the_games_goal_events():
    games = _load("full/games__playoffs__2026__3games.json")
    goals = _parsed("goals")
    n = sum(len(g[s]["goalEvents"]) for g in games for s in ("homeTeam", "awayTeam"))
    assert goals.height == n
    first = goals.row(0, named=True)
    assert first["assist1_player_id"] == 32873373 and first["plus_jerseys"] == "7 16 20 52 71"
    assert goals.select("season", "game_id", "event_id").is_duplicated().sum() == 0


def test_game_frames_share_the_game_key():
    for short in ("game", "game_goals", "game_penalties", "game_shootout", "game_lineups", "game_players_stats"):
        df = _parsed(short)
        assert set(df["season"]) == {2026} and set(df["game_id"]) == {1}, short


def test_game_goal_types_and_shootout_order():
    goals = _parsed("game_goals")
    assert goals.schema["goal_types"] == pl.String
    assert _parsed("game_shootout")["shot_number"].to_list() == [1, 2, 3]


def test_schedule_serie_is_the_tournament_id():
    df = _parsed("schedule")
    assert df.schema["serie_id"] == pl.Int64 and set(df["serie_id"]) == {1188} and "serie" not in df.columns


def test_standings_tag_each_table_and_keep_fractions():
    df = _parsed("standings")
    assert set(df["tournament"]) == {"runkosarja", "playoffs", "valmistavat_ottelut"}
    assert df.schema["power_play_percentage"] == pl.Float64
    assert df["win_percentage"].max() <= 1.0  # 0-1 fraction, as sent
    assert (df["internal_id"] == df["team_id"]).all()


@pytest.mark.parametrize(
    ("text", "want"),
    [
        ("92.86", 92.86),
        ("0,00", 0.0),
        ("12,5", 12.5),
        ("4,583.33", 4583.33),
        ("4,583", 4583.0),
        ("1,234,567", 1234567.0),
        ("1,234,567.5", 1234567.5),
        ("-3,5", -3.5),
        ("", None),
        ("n/a", None),
    ],
)
def test_number_reads_the_comma_by_its_grouping(text, want):
    # A comma followed by exactly three digits groups thousands ("4,583" -> 4583, never 4.583).
    assert pl.select(P._number(pl.lit(text))).item() == want


def test_player_career_parses_both_decimal_marks():
    df = _parsed("player_career")
    practice = df.filter(pl.col("tournament") == "PRACTICE").sort("season", descending=True).row(0, named=True)
    assert practice["save_percentage"] == 0.0  # "0,00"
    assert df.filter(pl.col("tournament") == "RUNKOSARJA")["win_percentage"].to_list()[0] == 4583.33  # "4,583.33"


def test_player_game_log_rows_carry_the_request():
    df = _parsed("player_game_log")
    assert set(df["player_id"]) == {21870028} and set(df["season"]) == {2026}
    assert set(df["tournament"]) == {"practice", "regular", "playoffs"}


def test_player_profile_maps_stay_json():
    row = _parsed("player").row(0, named=True)
    assert row["player_id"] == 21870028
    assert json.loads(row["team_list"]).keys() == {"FPS", "HIFK", "HPK"}


def test_generic_row_rule():
    assert P.parse_liiga({"teamStats": [{"teamId": 1}, {"teamId": 2}]}).height == 2
    assert P.parse_liiga({"a": 1, "b": 2}).height == 1
    assert P.parse_liiga({"1": {"x": 1}, "2": {"x": 2}})["id"].to_list() == [1, 2]


# --- YAML ---------------------------------------------------------------------------------------


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def test_yaml_lists_every_route():
    # The spec's route list is committed by gen_liiga (spec_paths.json): CI has no sdv-internal-refs.
    doc = _yaml()
    paths = json.loads(gen_liiga.SPEC_PATHS.read_text(encoding="utf-8"))
    assert {e[0] for e in gen_liiga.ENDPOINTS.values()} == set(paths) and len(paths) == 14
    assert [e["short"] for e in doc["endpoints"]] == list(gen_liiga.ENDPOINTS)
    assert len(doc["endpoints"]) == 23
    assert doc["getter_module"] == "sportsdataverse.hockey.liiga.liiga_runtime"


def _descriptions() -> Dict[str, Dict[str, str]]:
    """The ``liiga_<short>`` blocks of manual_column_descriptions.yaml (gen_liiga's snippet, merged)."""
    loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
    manual = yaml.load(MANUAL.read_text(encoding="utf-8"), Loader=loader)
    return {k: v for k, v in manual.items() if k.startswith("liiga_")}


def test_schemas_carry_no_descriptions_and_the_manual_file_does():
    # Descriptions live in manual_column_descriptions.yaml (merged from the snippet), keyed liiga_<short>.
    assert _yaml()["schema_prefix"] == "liiga_"
    snippet = _descriptions()
    assert set(snippet) == {f"liiga_{s}" for s in gen_liiga.ENDPOINTS}
    banned = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.I)
    for short in gen_liiga.ENDPOINTS:
        committed = _schema(short)
        assert committed["schema"] == f"liiga_{short}"
        assert all(c["description"] == "" for c in committed["columns"]), short
        descs = snippet[f"liiga_{short}"]
        assert set(descs) == {c["name"] for c in committed["columns"]}
        for col, desc in descs.items():
            assert len(desc) >= 15 and desc.lower() != col and not banned.match(desc), (short, col, desc)
            assert not gen_liiga._filler(col, desc), (short, col, desc)


def test_descriptions_keep_their_route_context():
    snippet = _descriptions()
    assert "season to date" in snippet["liiga_standings"]["games"]
    assert "season to date" in snippet["liiga_standings"]["points"]
    assert "this period" not in snippet["liiga_standings"]["power_play_goals"]
    assert "this season" in snippet["liiga_game_players_stats"]["season_stats_games"]
    assert "all-time" in snippet["liiga_game_players_stats"]["all_time_stats_games"]
    assert "all-time" not in " ".join(snippet["liiga_standings"].values())
    assert "player" in snippet["liiga_game_stats"]["goals"] and "team" in snippet["liiga_game_team_stats"]["goals"]
    assert (
        snippet["liiga_game"]["home_name"].startswith("Home team:") and "rink" not in snippet["liiga_game"]["home_name"]
    )


def test_yaml_enums_are_the_probed_sets():
    by = {e["short"]: e for e in _yaml()["endpoints"]}
    dt = {p["name"]: p for p in by["team_stats"]["extra_params"]}["data_type"]
    assert dt["choices"] == gen_liiga.TEAM_DATA_TYPES and len(dt["choices"]) == 7
    pdt = {p["name"]: p for p in by["player_stats"]["extra_params"]}["data_type"]
    assert pdt["choices"] == gen_liiga.PLAYER_DATA_TYPES and len(pdt["choices"]) == 13
    tp = {p["name"]: p for p in by["player_stats"]["path_params"]}["tournament"]
    assert tuple(tp["choices"]) == R.TOURNAMENTS and len(R.TOURNAMENTS) == 6


def test_game_wrappers_take_season_and_game_id():
    for e in _yaml()["endpoints"]:
        if "{game_id}" in e["path"]:
            assert [p["name"] for p in e["path_params"]] == ["season", "game_id"], e["short"]


def test_teams_info_team_param_is_not_exposed():
    # /teams/info?team= does not filter (any value answers every team's history), so no wrapper takes it.
    by = {e["short"]: e for e in _yaml()["endpoints"]}
    assert by["teams_history"]["fixed_params"] == {"team": "all"}
    for e in _yaml()["endpoints"]:
        assert "team" not in {p["name"] for p in e.get("extra_params", [])}, e["short"]


def test_no_path_token_named_league_or_sport():
    for e in _yaml()["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"])), e["path"]


# --- runtime ------------------------------------------------------------------------------------


class _Resp:
    def __init__(self, status: int, body: Any = None, text: str = None):
        self.status_code = status
        self.text = text if text is not None else json.dumps(body)
        self.headers = {"content-type": "application/json"}

    def json(self) -> Any:
        return json.loads(self.text)


@pytest.fixture
def transport(monkeypatch):
    """Fake ``download``: answers queued responses, records each call and each attempt over SESSION."""
    state: Dict[str, Any] = {"calls": [], "queue": [], "waited": []}

    def fake(url: str, params: Any = None, **kw: Any) -> _Resp:
        state["calls"].append({"url": url, "params": dict(params or {}), **kw})
        if kw.get("session") is euro_hockey_runtime.SESSION:  # SESSION waits for the host before every attempt
            state["waited"].append(url)
        return state["queue"].pop(0) if len(state["queue"]) > 1 else state["queue"][0]

    monkeypatch.setattr(_codegen_runtime, "download", fake)
    monkeypatch.setattr(_codegen_runtime.time, "sleep", lambda s: None)
    return state


def test_runtime_throttles_and_never_retries_a_500(transport):
    transport["queue"] = [_Resp(500, text='"Remote server error"')]
    with pytest.raises(AssetFetchError, match="500"):
        R._get(f"{HOST}/teams/stats", {"tournament": "runkosarja", "dataType": "shots"})
    assert transport["waited"] == [f"{HOST}/teams/stats"]
    assert 500 not in transport["calls"][0]["retry_statuses"]
    assert {429, 502, 503, 504} <= set(transport["calls"][0]["retry_statuses"])


@pytest.mark.parametrize(
    ("body", "error", "match"),
    [
        ({"stats": "Remote server error"}, NoDataError, "no such game"),
        ("Remote server error", AssetFetchError, "a JSON str, not an object"),  # the shared _json_body refuses it first
        ({"message": "Internal server error"}, AssetFetchError, "error envelope"),
        ({"games": "unexpected error"}, AssetFetchError, "error body"),
        ([], NoDataError, "no rows"),
        ({"teamStats": []}, NoDataError, "no rows"),
        ({"a": None}, NoDataError, "no rows"),
    ],
)
def test_runtime_refuses_error_and_stub_bodies(transport, body, error, match):
    transport["queue"] = [_Resp(200, body)]
    with pytest.raises(error, match=match):
        R._get(f"{HOST}/games/stats/2026/999")


@pytest.mark.parametrize(("status", "error"), [(404, NoDataError), (400, ValueError), (502, AssetFetchError)])
def test_runtime_non_200_follows_the_vocabulary(transport, status, error):
    transport["queue"] = [_Resp(status, {"x": 1})]
    with pytest.raises(error):
        R._get(f"{HOST}/schedule", {"season": 2026})


def test_runtime_refuses_a_truncated_body(transport):
    transport["queue"] = [_Resp(200, text='[{"id": 1, "season": 20')]
    with pytest.raises(AssetFetchError):
        R._get(f"{HOST}/schedule", {"season": 2026})
    assert len(transport["calls"]) == 3  # fetched again twice before giving up


def test_runtime_refuses_an_empty_200(transport):
    transport["queue"] = [_Resp(200, text="")]
    with pytest.raises(AssetFetchError, match="empty body"):
        R._get(f"{HOST}/tournament")


@pytest.mark.parametrize(
    ("url", "params", "body"),
    [
        (f"{HOST}/games/2026/1", None, {"game": {"season": 2025, "id": 1}}),
        (f"{HOST}/games/2026/1", None, {"game": {"season": 2026, "id": True}}),  # True == 1 must not pass
        (f"{HOST}/schedule", {"season": 2026}, [{"id": 1, "season": 2025}]),
        (f"{HOST}/games", {"season": 2026}, [{"id": 1, "season": 2026}, {"id": 2, "season": "2026"}]),
        (f"{HOST}/gameweeks", {"season": 2026, "tournament": "runkosarja"}, {"season": 2025, "nbWeeks": 27}),
        (f"{HOST}/players/info/1/games/2026", None, {"regular": [{"gameId": 1, "date": "2024-09-10T15:30:00Z"}]}),
    ],
)
def test_runtime_refuses_an_out_of_scope_body(transport, url, params, body):
    transport["queue"] = [_Resp(200, body)]
    with pytest.raises(AssetFetchError):
        R._get(url, params)


def test_runtime_accepts_the_example_bodies_and_stamps_the_request(transport):
    transport["queue"] = [_Resp(200, _load("games__season__id.json"))]
    assert R._get(f"{HOST}/games/2026/1")["_request"] == {"season": 2026, "game_id": 1}
    transport["queue"] = [_Resp(200, _load("shotmap__season__id.json"))]
    shots = R._get(f"{HOST}/shotmap/2026/1")
    assert all(s["_request"] == {"season": 2026, "game_id": 1} for s in shots)
    transport["queue"] = [_Resp(200, _load("players__info__playerId__games__season.json"))]
    assert R._get(f"{HOST}/players/info/21870028/games/2026")["_request"] == {"player_id": 21870028, "season": 2026}
    transport["queue"] = [_Resp(200, _load("standings.json"))]
    assert R._get(f"{HOST}/standings/", {"season": 2026})["_request"] == {"season": 2026}


def test_runtime_sends_the_end_year_season_unchanged(transport):
    # Liiga keys seasons by END year, the sdv key: no mapping at the boundary.
    transport["queue"] = [_Resp(200, _load("schedule.json"))]
    R._get(f"{HOST}/schedule", {"season": 2026, "tournament": "runkosarja"})
    assert transport["calls"][0]["params"] == {"season": 2026, "tournament": "runkosarja"}
    transport["queue"] = [_Resp(200, _load("games__stats__season__id.json"))]
    body = R._get(f"{HOST}/games/stats/2026/1")
    assert transport["calls"][1]["url"].endswith("/games/stats/2026/1") and body["_request"]["season"] == 2026


def test_runtime_checks_the_tournament_before_sending(transport):
    transport["queue"] = [_Resp(200, [])]
    with pytest.raises(ValueError, match="tournament must be one of"):
        R._get(f"{HOST}/games", {"season": 2026, "tournament": "harjoitusottelut"})
    assert transport["calls"] == []


def test_runtime_lets_an_omitted_season_through_unchecked(transport):
    transport["queue"] = [_Resp(200, [{"id": 1, "season": 2027}])]
    assert R._get(f"{HOST}/schedule", {"season": None})[0]["season"] == 2027
