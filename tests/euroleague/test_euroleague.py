"""Offline tests for the EuroLeague (``euroleague``) flat-API family -- three hosts.

Asserts the parsers against the real trimmed captures in ``tests/fixtures/euroleague/``
(never synthetic payloads), the generated endpoint YAML + returns-schemas, the wrappers'
URL construction per host, the live API's empty-200 "no such game" path through a fake
transport, and the empty/malformed contract. No network, except the gated live smokes at
the bottom (``SDV_PY_LIVE_TESTS=1``; one request per host).
"""

from __future__ import annotations

import inspect
import json
import re
from pathlib import Path
from typing import Any, Dict

import polars as pl
import pytest
import yaml

from sportsdataverse.euroleague import euroleague, euroleague_runtime
from sportsdataverse.euroleague._euroleague_schemas import SCHEMAS
from sportsdataverse.euroleague.euroleague_parsers import (
    parse_euroleague,
    parse_euroleague_boxscore,
    parse_euroleague_header,
    parse_euroleague_pbp,
    parse_euroleague_points,
)
from tests.conftest import skip_if_no_live

FIXTURES = Path(__file__).parents[1] / "fixtures" / "euroleague"
YAML_PATH = Path(__file__).parents[2] / "tools" / "codegen" / "endpoints" / "euroleague.yaml"
SCHEMA_DIR = Path(__file__).parents[2] / "tools" / "codegen" / "schemas" / "native" / "euroleague"

V2 = "https://api-live.euroleague.net/v2"
V3 = "https://api-live.euroleague.net/v3"
LIVE = "https://live.euroleague.net/api"

_STEMS = sorted(p.stem for p in FIXTURES.glob("*.json"))
_LIVE_STEMS = [s for s in _STEMS if "api__" in s]
_API_LIVE_STEMS = [s for s in _STEMS if "api__" not in s]
_ENVELOPE_STEMS = [s for s in _API_LIVE_STEMS if not s.endswith(("__stats", "__report")) and "rounds__1__" not in s]
_STANDINGS_STEMS = [s for s in _API_LIVE_STEMS if "rounds__1__" in s]
_GAME_STATS = "competitions__E__seasons__E2025__games__1__stats"
_GAME_REPORT = "v3__competitions__E__seasons__E2025__games__1__report"

# The live parser per fixture stem (``U2025__`` copies share the E2025 route's parser).
_LIVE_PARSER = {
    "Points": parse_euroleague_points,
    "Header": parse_euroleague_header,
    "PlayByPlay": parse_euroleague_pbp,
    "Boxscore": parse_euroleague_boxscore,
}

# Every ``id`` / ``*_id`` / ``code`` / ``*_code`` column is an opaque join key.
_ID = re.compile(r"(^|_)(id|code)$")
_LIVE_ID = re.compile(r"^(id_\w+|\w+_id|codeteam|code_team_[ab]|tv_code_[ab]|team)$")
_SNAKE = re.compile(r"^[a-z0-9_]+$")

_SHORTS = [
    "clubs",
    "competitions",
    "game_boxscore",
    "game_header",
    "game_pbp",
    "game_points",
    "game_report",
    "game_stats",
    "games",
    "people",
    "player_stats",
    "rounds",
    "seasons",
    "standings",
    "team_stats",
]


def _load(stem: str) -> Any:
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


def _live_parse(stem: str) -> pl.DataFrame:
    return _LIVE_PARSER[stem.rsplit("api__", 1)[1]](_load(stem))


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def _schema(short: str) -> Dict[str, Any]:
    return yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))


class _Recorder:
    """Stand-in for the runtime ``_get`` that records the URL + params."""

    def __init__(self, payload: Any = None) -> None:
        self.payload = payload if payload is not None else {"total": 0, "data": []}
        self.url: str = ""
        self.params: Dict[str, Any] = {}

    def __call__(self, url: str, params: Any = None, **kwargs: Any) -> Any:
        self.url = url
        self.params = params or {}
        return self.payload


@pytest.fixture()
def recorder(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    rec = _Recorder()
    monkeypatch.setattr(euroleague, "_get", rec)
    return rec


# ---------------------------------------------------------------------------
# parse_euroleague -- every api-live (v2 + v3) capture
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("stem", _API_LIVE_STEMS)
def test_every_api_live_fixture_parses_to_a_tidy_frame(stem: str) -> None:
    df = parse_euroleague(_load(stem))
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0
    for col in df.columns:
        assert _SNAKE.match(col), f"{stem}.{col} is not snake_case"
        if _ID.search(col):
            assert df.schema[col] == pl.String, f"{stem}.{col} must stay Utf8"


@pytest.mark.parametrize("stem", _ENVELOPE_STEMS)
def test_envelope_rows_come_from_the_one_list(stem: str) -> None:
    """List routes answer ``{"total": n, <list>: [...]}``: rows are the list, not the envelope."""
    raw = _load(stem)
    lists = [k for k, v in raw.items() if isinstance(v, list)]
    assert set(raw) == {"total", lists[0]}, "capture no longer carries the envelope"
    df = parse_euroleague(raw)
    assert df.height == len(raw[lists[0]])
    assert "total" not in df.columns
    assert lists[0] not in df.columns


@pytest.mark.parametrize("stem", _STANDINGS_STEMS)
def test_standings_rows_are_teams(stem: str) -> None:
    """The v3 standings are ``{"winner", "teams"}``: one row per team, the winner dropped."""
    raw = _load(stem)
    assert set(raw) == {"winner", "teams"}
    df = parse_euroleague(raw)
    assert df.height == len(raw["teams"]) == 3
    assert {"position", "games_won", "games_lost", "club_code", "club_name"} <= set(df.columns)
    assert df.schema["club_code"] == pl.String
    assert "winner" not in df.columns and "teams" not in df.columns


def test_standings_kinds_differ_in_columns() -> None:
    basic = parse_euroleague(_load("v3__competitions__E__seasons__E2025__rounds__1__basicstandings"))
    cal = parse_euroleague(_load("v3__competitions__E__seasons__E2025__rounds__1__calendarstandings"))
    streaks = parse_euroleague(_load("v3__competitions__E__seasons__E2025__rounds__1__streaks"))
    ahead = parse_euroleague(_load("v3__competitions__E__seasons__E2025__rounds__1__aheadbehind"))
    assert {"points_for", "points_against", "home_record", "last_ten_record"} <= set(basic.columns)
    assert "streaks" in cal.columns and cal["streaks"][0].startswith("[{")
    assert "longest_wins_streak_current_season" in streaks.columns
    assert {"quater1_ahead", "half1_tied", "quater3_behind"} <= set(ahead.columns)


def test_page_object_is_one_row() -> None:
    """The v2 box score is a ``{"local", "road"}`` page object -> ONE row, not one per side."""
    raw = _load(_GAME_STATS)
    assert set(raw) == {"local", "road"}
    df = parse_euroleague(raw)
    assert df.height == 1
    assert {"local_coach_code", "local_total_points", "road_total_points", "local_players"} <= set(df.columns)
    assert df.schema["local_coach_code"] == pl.String
    # the nested player list survives as a JSON cell, never a Python repr
    assert df["local_players"].item().startswith("[{")


def test_game_report_is_one_row_with_both_clubs() -> None:
    df = parse_euroleague(_load(_GAME_REPORT))
    assert df.height == 1
    assert {"game_code", "local_club_code", "road_club_code", "local_score", "road_score"} <= set(df.columns)
    assert df.schema["game_code"] == pl.String and df["game_code"][0] == "1"
    assert df["local_last5_form"][0].startswith("[")


def test_game_code_is_utf8_without_a_float_suffix() -> None:
    """``gameCode`` is an integer on the wire; the join key must read ``"406"``, never ``"406.0"``."""
    raw = _load("competitions__E__seasons__E2025__games")
    assert isinstance(raw["data"][0]["gameCode"], int)
    df = parse_euroleague(raw)
    assert df.schema["game_code"] == pl.String
    assert not df["game_code"].str.ends_with(".0").any()
    assert df["game_code"][0] == str(raw["data"][0]["gameCode"])


def test_nested_objects_flatten_to_prefixed_columns() -> None:
    df = parse_euroleague(_load("competitions__E__seasons__E2025__clubs"))
    assert {"code", "name", "country_code", "country_name", "images_crest"} <= set(df.columns)
    assert df.schema["country_code"] == pl.String


def test_season_stats_flatten_player_and_team() -> None:
    players = parse_euroleague(_load("v3__competitions__E__statistics__players__traditional"))
    teams = parse_euroleague(_load("v3__competitions__E__statistics__teams__advanced"))
    assert {"player_code", "player_name", "player_team_code", "pir"} <= set(players.columns)
    assert players.schema["player_code"] == pl.String and players.schema["player_team_code"] == pl.String
    assert {"team_code", "team_name", "effective_field_goal_percentage"} <= set(teams.columns)
    assert teams.schema["team_code"] == pl.String


@pytest.mark.parametrize("raw", [None, [], {}, "x", 17, {"total": 0, "data": []}, [None]])
def test_empty_payload_is_zero_row_frame(raw: Any) -> None:
    df = parse_euroleague(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


@pytest.mark.parametrize("raw", [None, [], {}, "x", 17, {"Rows": []}, {"FirstQuarter": []}, {"Stats": []}, [None]])
@pytest.mark.parametrize(
    "short,parser",
    [
        ("game_points", parse_euroleague_points),
        ("game_header", parse_euroleague_header),
        ("game_pbp", parse_euroleague_pbp),
        ("game_boxscore", parse_euroleague_boxscore),
    ],
)
def test_live_empty_payload_is_zero_row_frame_with_the_documented_schema(raw: Any, short: str, parser: Any) -> None:
    """The parser contract: an empty live body keeps the returns table's columns (and dtypes)."""
    df = parser(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0
    assert df.columns == [c["name"] for c in _schema(short)["columns"]]
    assert df.columns == list(SCHEMAS[short])
    assert {c: str(t) for c, t in df.schema.items()} == SCHEMAS[short]
    pdf = parser(raw, return_as_pandas=True)
    assert list(pdf.columns) == df.columns and len(pdf) == 0


def test_return_as_pandas() -> None:
    pdf = parse_euroleague(_load("competitions"), return_as_pandas=True)
    assert type(pdf).__module__.startswith("pandas")
    assert len(pdf) == 3
    pdf = parse_euroleague_points(_load("api__Points"), return_as_pandas=True)
    assert type(pdf).__module__.startswith("pandas")
    assert len(pdf) == 3


# ---------------------------------------------------------------------------
# live.euroleague.net/api parsers -- every live capture (E2025 + U2025)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("stem", _LIVE_STEMS)
def test_every_live_fixture_parses_to_a_tidy_frame(stem: str) -> None:
    df = _live_parse(stem)
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    for col in df.columns:
        assert _SNAKE.match(col), f"{stem}.{col} is not snake_case"
        if _LIVE_ID.match(col):
            assert df.schema[col] == pl.String, f"{stem}.{col} must stay Utf8"
        assert df.schema[col] != pl.Null, f"{stem}.{col} must not drift to Null on a thin capture"
        if df.schema[col] == pl.String:
            assert not (df[col].str.starts_with(" ") | df[col].str.ends_with(" ")).any(), f"{stem}.{col} not stripped"


@pytest.mark.parametrize("stem", ["api__Points", "U2025__api__Points"])
def test_points_is_the_shot_chart_in_cm_from_the_hoop(stem: str) -> None:
    raw = _load(stem)
    assert set(raw) == {"Rows"}
    assert raw["Rows"][0]["TEAM"].endswith(" "), "capture no longer carries the space padding"
    df = parse_euroleague_points(raw)
    assert df.height == len(raw["Rows"])
    assert {"team", "id_player", "id_action", "coord_x", "coord_y", "zone", "points_a", "points_b"} <= set(df.columns)
    assert df.schema["coord_x"] == pl.Int64 and df.schema["coord_y"] == pl.Int64
    assert df["team"][0] == raw["Rows"][0]["TEAM"].strip()
    assert df["id_player"][0] == raw["Rows"][0]["ID_PLAYER"].strip()
    assert set(df["id_action"].to_list()) <= {"2FGM", "2FGA", "3FGM", "3FGA", "FTM"}


def test_points_free_throws_carry_the_minus_one_sentinel() -> None:
    """``FTM`` rows are not located: ``-1, -1`` and a blank zone, never a spot near the hoop."""
    rows = [r for s in ("api__Points", "U2025__api__Points") for r in _load(s)["Rows"]]
    fts = [r for r in rows if r["ID_ACTION"] == "FTM"]
    if not fts:  # the 3-row trims may hold no free throw; the rule is still asserted on the frame
        pytest.skip("no FTM row in the trimmed captures")
    df = parse_euroleague_points({"Rows": fts})
    assert (df["coord_x"] == -1).all() and (df["coord_y"] == -1).all()
    assert (df["zone"] == "").all()


@pytest.mark.parametrize("stem", ["api__PlayByPlay", "U2025__api__PlayByPlay"])
def test_pbp_unrolls_quarters_in_game_order(stem: str) -> None:
    raw = _load(stem)
    df = parse_euroleague_pbp(raw)
    expected = sum(len(raw[k]) for k in ("FirstQuarter", "SecondQuarter", "ThirdQuarter", "ForthQuarter", "ExtraTime"))
    assert df.height == expected == 12
    assert df.columns[:5] == ["quarter", "team_a", "team_b", "code_team_a", "code_team_b"]
    assert df["quarter"].to_list() == [1] * 3 + [2] * 3 + [3] * 3 + [4] * 3
    assert df["code_team_a"][0] == raw["CodeTeamA"].strip()
    assert df.schema["points_a"] == pl.Int64 and df.schema["points_b"] == pl.Int64
    assert df.schema["codeteam"] == pl.String and df.schema["player_id"] == pl.String
    assert df["playtype"][0] == "BP"


def test_boxscore_rows_are_players_then_team_and_totals_per_side() -> None:
    raw = _load("api__Boxscore")
    df = parse_euroleague_boxscore(raw)
    assert len(raw["Stats"]) == 2
    assert df.height == sum(len(s["PlayersStats"]) + 2 for s in raw["Stats"]) == 10
    assert df["row_type"].to_list() == ["player"] * 3 + ["team", "total"] + ["player"] * 3 + ["team", "total"]
    assert {"team_name", "coach", "player_id", "team", "points", "valuation"} <= set(df.columns)
    assert df.schema["is_starter"] == pl.Int64 and df.schema["is_playing"] == pl.Int64
    # the game-level and side-level header fields repeat on every row
    assert df["attendance"].unique().to_list() == [raw["Attendance"]]
    assert df["referees"].unique().to_list() == [raw["Referees"]]
    for i, side in enumerate(raw["Stats"]):
        rows = df.filter(pl.col("team_name") == side["Team"])
        assert rows.height == len(side["PlayersStats"]) + 2
        for q in (1, 2, 3, 4):
            assert rows[f"by_quarter_q{q}"].unique().to_list() == [raw["ByQuarter"][i][f"Quarter{q}"]]
            assert rows[f"end_of_quarter_q{q}"].unique().to_list() == [raw["EndOfQuarter"][i][f"Quarter{q}"]]
    # EndOfQuarter is cumulative (21/40/60/85), ByQuarter is per period (21/19/20/25)
    first = df.row(0, named=True)
    assert first["end_of_quarter_q4"] == sum(first[f"by_quarter_q{q}"] for q in (1, 2, 3, 4))
    assert (
        df.schema["player_id"] == pl.String
        and df["player_id"][0] == raw["Stats"][0]["PlayersStats"][0]["Player_ID"].strip()
    )
    assert df["team_name"][0] == raw["Stats"][0]["Team"]
    assert df.filter(pl.col("row_type") == "total")["points"].to_list() == [s["totr"]["Points"] for s in raw["Stats"]]


def test_header_is_one_row() -> None:
    raw = _load("api__Header")
    df = parse_euroleague_header(raw)
    assert df.height == 1
    # the header's quarter scores are cumulative, not per period
    assert df["score_quarter4_a"][0] == int(raw["ScoreA"]) - raw["ScoreExtraTimeA"]
    assert "cumulative" in {c["name"]: c["description"] for c in _schema("game_header")["columns"]}["score_quarter1_a"]
    assert {"code_team_a", "code_team_b", "score_a", "score_b", "score_quarter1_a", "referee1"} <= set(df.columns)
    assert df["code_team_a"][0] == raw["CodeTeamA"].strip()


# ---------------------------------------------------------------------------
# generated endpoint YAML + schemas (gen_euroleague.py output)
# ---------------------------------------------------------------------------


def test_yaml_lists_every_wrapper_with_its_host() -> None:
    doc = _yaml()
    assert doc["getter_module"] == "sportsdataverse.euroleague.euroleague_runtime"
    assert doc["host"] == V2
    by_short = {e["short"]: e for e in doc["endpoints"]}
    assert sorted(by_short) == _SHORTS
    hosts = {s: e.get("host", V2) for s, e in by_short.items()}
    assert {s for s, h in hosts.items() if h == LIVE} == {"game_boxscore", "game_header", "game_pbp", "game_points"}
    assert {s for s, h in hosts.items() if h == V3} == {"game_report", "player_stats", "standings", "team_stats"}
    assert {s for s, h in hosts.items() if h == V2} == {
        "clubs",
        "competitions",
        "game_stats",
        "games",
        "people",
        "rounds",
        "seasons",
    }
    # the live routes name the ``{}`` sentinel in their raw-return prose
    for short in ("game_points", "game_pbp", "game_boxscore", "game_header"):
        assert "no such game" in by_short[short]["docstring"]["raw_doc"]
        assert [p["query_key"] for p in by_short[short]["extra_params"]] == ["gamecode", "seasoncode"]
        assert [p["name"] for p in by_short[short]["extra_params"]] == ["game_code", "season_code"]
        # an omitted code answers a silent empty 200, so both are positional-required
        assert all(p["required"] for p in by_short[short]["extra_params"])
        sig = inspect.signature(getattr(euroleague, f"euroleague_{short}"))
        assert sig.parameters["game_code"].default is inspect.Parameter.empty
        assert sig.parameters["season_code"].default is inspect.Parameter.empty


def test_merged_wrappers_take_the_segment_as_a_defaulted_argument() -> None:
    by_short = {e["short"]: e for e in _yaml()["endpoints"]}
    kind = by_short["standings"]["path_params"][-1]
    assert by_short["standings"]["path"].endswith("/rounds/{round}/{kind}")
    assert (kind["name"], kind["default"], kind["required"]) == ("kind", "basicstandings", False)
    assert kind["choices"] == ["basicstandings", "calendarstandings", "streaks", "aheadbehind"]
    for short in ("player_stats", "team_stats"):
        mode = by_short[short]["path_params"][-1]
        assert by_short[short]["path"].endswith("/{mode}")
        assert (mode["name"], mode["default"]) == ("mode", "traditional")
        assert mode["choices"] == ["traditional", "advanced"]
        defaults = {p["name"]: p.get("default") for p in by_short[short]["extra_params"]}
        assert defaults["season_mode"] == "Single" and defaults["statistic_mode"] == "PerGame"
        assert defaults["season_code"] is None


def test_no_path_token_named_league_or_sport() -> None:
    """The renderer blanks ``{league}``/``{sport}`` as ESPN slugs; no EuroLeague route may use them."""
    for ep in _yaml()["endpoints"]:
        tokens = set(re.findall(r"\{(\w+)\}", ep["path"]))
        names = {p["name"] for p in ep.get("path_params", [])}
        assert not {"league", "sport"} & (tokens | names), ep["path"]
        assert tokens == names, f"{ep['short']}: path tokens {tokens} != path_params {names}"


def test_every_endpoint_schema_is_verified_and_described() -> None:
    for ep in _yaml()["endpoints"]:
        schema = _schema(ep["short"])
        assert not schema.get("unverified"), ep["short"]
        tables = [schema["columns"]] if schema["kind"] == "dataframe" else [f["columns"] for f in schema["frames"]]
        assert tables and all(tables), ep["short"]
        for cols in tables:
            assert all(c["description"] for c in cols), ep["short"]


@pytest.mark.parametrize(
    "short,by,sections", [("standings", "kind", 4), ("player_stats", "mode", 2), ("team_stats", "mode", 2)]
)
def test_merged_schemas_document_one_table_per_value(short: str, by: str, sections: int) -> None:
    schema = _schema(short)
    assert schema["kind"] == "frames" and schema["frames_by"] == by
    assert len(schema["frames"]) == sections
    assert schema["frames"][0]["section"] in ("basicstandings", "traditional")


def test_points_schema_documents_the_shot_frame() -> None:
    cols = {c["name"]: c["description"] for c in _schema("game_points")["columns"]}
    assert "centimeters from the hoop" in cols["coord_x"] and "centimeters from the hoop" in cols["coord_y"]
    assert "UNVERIFIED" in cols["coord_x"]
    assert "toward the court" in cols["coord_y"]
    assert "-1 sentinel" in cols["coord_x"]


def test_schemas_match_the_parsers_on_the_captures() -> None:
    """Column names and order on every returns table are what the parser emits on its capture."""
    checks = {
        "game_points": parse_euroleague_points(_load("api__Points")),
        "game_header": parse_euroleague_header(_load("api__Header")),
        "game_pbp": parse_euroleague_pbp(_load("api__PlayByPlay")),
        "game_boxscore": parse_euroleague_boxscore(_load("api__Boxscore")),
        "game_report": parse_euroleague(_load(_GAME_REPORT)),
    }
    for short, df in checks.items():
        assert [c["name"] for c in _schema(short)["columns"]] == df.columns, short
    for section in ("basicstandings", "calendarstandings", "streaks", "aheadbehind"):
        frame = next(f for f in _schema("standings")["frames"] if f["section"] == section)
        df = parse_euroleague(_load(f"v3__competitions__E__seasons__E2025__rounds__1__{section}"))
        assert [c["name"] for c in frame["columns"]] == df.columns, section


# ---------------------------------------------------------------------------
# generated wrappers: URL per host, defaults, the empty-200 path
# ---------------------------------------------------------------------------


def test_wrapper_builds_the_nested_url(recorder: _Recorder) -> None:
    euroleague.euroleague_clubs(competition_code="E", season_code="E2025")
    assert recorder.url == f"{V2}/competitions/E/seasons/E2025/clubs"


def test_wrapper_passes_paging_as_query_params(recorder: _Recorder) -> None:
    euroleague.euroleague_games(competition_code="E", season_code="E2025", limit="3")
    assert recorder.url == f"{V2}/competitions/E/seasons/E2025/games"
    assert recorder.params["limit"] == "3"


def test_v3_wrappers_use_the_v3_host(recorder: _Recorder) -> None:
    euroleague.euroleague_standings(competition_code="E", season_code="E2025", round=1)
    assert recorder.url == f"{V3}/competitions/E/seasons/E2025/rounds/1/basicstandings"
    euroleague.euroleague_standings(competition_code="E", season_code="E2025", round=1, kind="aheadbehind")
    assert recorder.url == f"{V3}/competitions/E/seasons/E2025/rounds/1/aheadbehind"
    euroleague.euroleague_game_report(competition_code="E", season_code="E2025", game_code=1)
    assert recorder.url == f"{V3}/competitions/E/seasons/E2025/games/1/report"


@pytest.mark.parametrize(
    "call",
    [
        lambda: euroleague.euroleague_standings(competition_code="E", season_code="E2025", round=1, kind="basic"),
        lambda: euroleague.euroleague_player_stats(competition_code="E", season_code="E2025", mode="Traditional"),
        lambda: euroleague.euroleague_team_stats(competition_code="E", season_code="E2025", mode="adv"),
    ],
)
def test_invalid_kind_or_mode_raises_before_any_request(recorder: _Recorder, call: Any) -> None:
    """A typo would otherwise reach the host as a path segment and surface as a misleading NoDataError."""
    with pytest.raises(ValueError, match="must be one of"):
        call()
    assert recorder.url == "", "no transport call may happen"


def test_stats_wrappers_default_the_captured_modes(recorder: _Recorder) -> None:
    euroleague.euroleague_player_stats(competition_code="E", season_code="E2025")
    assert recorder.url == f"{V3}/competitions/E/statistics/players/traditional"
    # the runtime strips the None paging keys; the recorder sees them raw
    assert {k: v for k, v in recorder.params.items() if v is not None} == {
        "SeasonMode": "Single",
        "SeasonCode": "E2025",
        "statisticMode": "PerGame",
    }
    euroleague.euroleague_team_stats(competition_code="E", season_code="E2025", mode="advanced", limit="5")
    assert recorder.url == f"{V3}/competitions/E/statistics/teams/advanced"
    assert recorder.params["limit"] == "5"


def test_live_wrappers_use_the_live_host_and_query_keys(recorder: _Recorder) -> None:
    recorder.payload = {}
    for fn, route in (
        (euroleague.euroleague_game_points, "Points"),
        (euroleague.euroleague_game_pbp, "PlayByPlay"),
        (euroleague.euroleague_game_boxscore, "Boxscore"),
        (euroleague.euroleague_game_header, "Header"),
    ):
        df = fn(game_code=1, season_code="E2025")
        assert recorder.url == f"{LIVE}/{route}"
        assert recorder.params == {"gamecode": 1, "seasoncode": "E2025"}
        assert isinstance(df, pl.DataFrame) and df.height == 0


def test_live_empty_200_is_a_zero_row_frame_through_the_real_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fake transport answering 200 + empty body (the API's "no such game") -> zero rows, no exception."""

    class _Resp:
        status_code = 200
        text = ""

    seen: Dict[str, Any] = {}

    def download(url: str, **kw: Any) -> _Resp:
        seen["url"] = url
        seen.update(kw)
        return _Resp()

    monkeypatch.setattr(euroleague_runtime._rt, "download", download)
    assert euroleague.euroleague_game_points(game_code=9999, season_code="E2025", return_parsed=False) == {}
    df = euroleague.euroleague_game_points(game_code=9999, season_code="E2025")
    assert isinstance(df, pl.DataFrame) and df.height == 0
    assert seen["url"] == f"{LIVE}/Points" and seen["params"] == {"gamecode": 9999, "seasoncode": "E2025"}
    assert "headers" not in seen


def test_live_wrapper_parses_the_capture_end_to_end(recorder: _Recorder) -> None:
    recorder.payload = _load("api__Points")
    df = euroleague.euroleague_game_points(game_code=1, season_code="E2025")
    assert df.height == 3 and df["team"][0] == "IST"


# ---------------------------------------------------------------------------
# live smokes: one request per host (SDV_PY_LIVE_TESTS=1)
# ---------------------------------------------------------------------------


@skip_if_no_live
def test_live_v2_competitions() -> None:
    df = euroleague.euroleague_competitions()
    assert "E" in df["code"].to_list()


@skip_if_no_live
def test_live_v3_standings_round_1() -> None:
    """20 teams in the 2025-26 EuroLeague regular season (measured 2026-10-06)."""
    df = euroleague.euroleague_standings(competition_code="E", season_code="E2025", round=1)
    assert df.height == 20
    assert df.schema["club_code"] == pl.String


@skip_if_no_live
def test_live_points_e2025_game_1() -> None:
    """E2025 game 1 (Efes - Maccabi) had 158 shot rows on 2026-10-06; codes come back stripped."""
    df = euroleague.euroleague_game_points(game_code=1, season_code="E2025")
    assert df.height >= 150
    assert set(df["team"].to_list()) == {"IST", "TEL"}
    assert df.schema["coord_x"] == pl.Int64
