"""Offline tests for the NBL Rosetta (``nbl_rosetta``) flat-API family: parsers, runtime and YAML.

Parsers are asserted against the real captures in ``tests/fixtures/nbl_rosetta/`` (refs captures, truncated)
and the full match body in ``tests/fixtures/nbl_rosetta/full/`` (dtypes come from that). The runtime is driven
through a fake transport (``_codegen_runtime.download``), so the error vocabulary is exercised end to end.
Nothing here reads the private refs checkout, and nothing touches the network.
"""

from __future__ import annotations

import copy
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
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball import nbl_au as nbl_pkg
from sportsdataverse.basketball.nbl_au import nbl_rosetta_parsers as P
from sportsdataverse.basketball.nbl_au import nbl_rosetta_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "nbl_rosetta"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "nbl_rosetta.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "nbl_rosetta"
HOST = "https://prod.rosetta.nbl.com.au/get"
FULL = "full/match__match_id__live__all--nbl_2025.json"
MATCH = "b217a699-4bef-11f0-bc08-933162362355"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_nbl_rosetta  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
ENDPOINTS = gen_nbl_rosetta.ENDPOINTS
_SCHEMA_DTYPE = {"integer": pl.Int64, "numeric": pl.Float64, "logical": pl.Boolean, "character": pl.String}


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _stamped(name: str) -> Any:
    path = FIXTURES / name
    return gen_nbl_rosetta._stamp(path, json.loads(path.read_text(encoding="utf-8")))


def _schema(short: str) -> Dict[str, pl.DataType]:
    doc = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    return {c["name"]: _SCHEMA_DTYPE[c["type"]] for c in doc["columns"]}


def _parsed(short: str) -> List[pl.DataFrame]:
    return gen_nbl_rosetta._frames(ENDPOINTS[short])


def _id_cols(df: pl.DataFrame) -> List[str]:
    return [c for c in df.columns if (c == "id" or c.endswith("_id")) and c != "action_id"]


# --- parsers over every committed fixture ------------------------------------------------------


@pytest.mark.parametrize("short", sorted(ENDPOINTS))
def test_every_wrapper_parses_its_fixtures(short):
    frames = _parsed(short)
    assert frames, short
    for df in frames:
        assert isinstance(df, pl.DataFrame)
        assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
        # every id is a String (UUIDs and hashes), never a float-origin "123.0"
        assert {c: df.schema[c] for c in _id_cols(df)} == {c: pl.String for c in _id_cols(df)}
        for c in _id_cols(df):
            assert not df[c].drop_nulls().str.contains(r"^\d+\.0$").any(), c
    assert any(df.height > 0 for df in frames), short


@pytest.mark.parametrize("short", sorted(ENDPOINTS))
def test_parser_dtypes_match_the_returns_schema(short):
    want = _schema(short)
    for df in _parsed(short):
        for name, dtype in df.schema.items():
            assert want[name] == dtype or df[name].null_count() == df.height, (short, name, dtype, want[name])


@pytest.mark.parametrize("short", sorted(ENDPOINTS))
def test_empty_payload_is_a_typed_zero_row_frame(short):
    parser = ENDPOINTS[short][2]
    want = _schema(short)
    for empty in ({}, [], None, "x", 3, {"data": []}, {"data": None}, {"data": "oops"}, [3, "a"]):
        df = parser(empty, columns=list(want))
        assert df.height == 0 and df.schema == pl.Schema(want), (short, empty)
    assert parser({}, return_as_pandas=True).empty


@pytest.mark.parametrize("short", sorted(ENDPOINTS))
def test_empty_and_populated_frames_stack(short):
    parser, cols = ENDPOINTS[short][2], list(_schema(short))
    empty = parser({}, columns=cols)
    for name in ENDPOINTS[short][3]:
        for path in gen_nbl_rosetta._variants(name):
            full = parser(_stamped(str(path.relative_to(FIXTURES))), columns=cols)
            stacked = pl.concat([empty, full])  # a dtype mismatch would raise here
            assert stacked.height == full.height and stacked.columns == full.columns


@pytest.mark.parametrize("short", sorted(ENDPOINTS))
def test_return_as_pandas(short):
    parser = ENDPOINTS[short][2]
    path = gen_nbl_rosetta._variants(ENDPOINTS[short][3][0])[0]
    out = parser(_stamped(str(path.relative_to(FIXTURES))), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame) and len(out) > 0


def test_presentation_and_betting_fields_are_dropped():
    pat = re.compile(r"blurhash|logo|colou?r|social|ticket|broadcaster|betting|odds|image|override")
    for short in ENDPOINTS:
        for df in _parsed(short):
            assert not [c for c in df.columns if pat.search(c)], (short, df.columns)


# --- the match page: one body, five frames -----------------------------------------------------


@pytest.fixture(scope="module")
def full() -> Any:
    return _load(FULL)


def test_pbp_is_the_2025_26_event_log_with_coordinates(full):
    df = P.parse_nbl_rosetta_match_pbp(full)
    assert df.height == 96 and set(df["feed"]) == {"datacore"} and set(df["match_id"]) == {MATCH}
    assert df["action_id"].is_sorted() and df.schema["action_id"] == pl.Int64  # the feed arrives newest first
    shots = df.filter(pl.col("x").is_not_null())
    assert shots.height > 10 and df.schema["x"] == pl.Float64
    assert shots["x"].min() >= 0 and shots["x"].max() <= 100 and shots["y"].min() >= 0 and shots["y"].max() <= 100
    assert {"2pt", "3pt"} <= set(shots["action_type"])
    assert df.filter(pl.col("sub_type") == "Jump Shot")["sub_type_key"].unique().to_list() == ["jump_shot"]
    first = df.row(0, named=True)
    assert first["action_id"] < df["action_id"].max() and first["period"] == 1


def test_pbp_score_and_team_flags_follow_the_body(full):
    df = P.parse_nbl_rosetta_match_pbp(full)
    header = P.parse_nbl_rosetta_match(full).row(0, named=True)
    assert df.schema["score_home"] == pl.Int64 and df["score_home"].max() <= int(header["home_score"])
    teams = df.filter(pl.col("team_id").is_not_null())
    assert set(teams["team_id"]) <= {header["home_team_id"], header["away_team_id"]}
    assert teams.filter(pl.col("is_home") == True)["team_id"].unique().to_list() == [header["home_team_id"]]  # noqa: E712
    assert df.schema["clock_seconds"] == pl.Float64 and df["clock_seconds"].drop_nulls().max() <= 600


def test_backfill_feed_is_flagged_and_sorted_oldest_first():
    df = P.parse_nbl_rosetta_match_pbp(_stamped("match__match_id__live__all--nbl_2015.json"))
    assert df.height == 3 and set(df["feed"]) == {"backfill"} and df["action_id"].is_sorted()
    assert df.schema["period"] == pl.Int64


def test_results_only_match_has_box_but_no_pbp():
    body = _load("match__match_id__live__all--nbl_1979.json")
    assert P.parse_nbl_rosetta_match_pbp(body).height == 0
    assert P.parse_nbl_rosetta_match_player_period_box(body).height == 0
    assert P.parse_nbl_rosetta_match_player_box(body).height == 3
    team = P.parse_nbl_rosetta_match_team_box(body)
    assert team.height == 2 and set(team["period"]) == {0}  # period 0 only: no quarter split in 1979
    assert P.parse_nbl_rosetta_match_player_box(body).schema["minutes"] == pl.Float64


def test_sub_type_key_folds_both_spellings():
    assert P._sub_type_key("Driving Layup") == P._sub_type_key("drivingLayup") == "driving_layup"
    assert P._sub_type_key("1Of2") == "1_of2" and P._sub_type_key("") is None and P._sub_type_key(None) is None


def test_clock_text_to_seconds():
    assert P._seconds("09:57") == 597.0 and P._seconds("00:00:02") == 2.0 and P._seconds(12) == 12.0
    assert P._seconds("x") is None and P._seconds(None) is None and P._seconds(True) is None


def test_player_box_minutes_are_decimal_and_rates_float(full):
    df = P.parse_nbl_rosetta_match_player_box(full)
    pepper = df.filter(pl.col("player_id") == "7619d3ea-410d-11f0-9235-2902fc2f936f")
    assert pepper.height == 1 and pepper["minutes"][0] == pytest.approx(30 + 32 / 60)  # "30:32"
    for col in ("field_goals_percentage", "free_throws_percentage", "three_points_percentage", "minutes"):
        assert df.schema[col] == pl.Float64, col
    assert df.schema["period"] == pl.Int64 and set(df["period"]) == {0}
    assert df["points"].sum() == 76 + 77  # both teams' lines add up to the final score


def test_team_box_has_a_row_per_period(full):
    df = P.parse_nbl_rosetta_match_team_box(full)
    assert sorted(set(df["period"])) == [0, 1, 2, 3, 4] and df.height == 10
    whole = df.filter(pl.col("period") == 0)
    assert sorted(whole["points"].to_list()) == [76, 77]


def test_period_box_carries_both_id_spaces(full):
    df = P.parse_nbl_rosetta_match_player_period_box(full)
    assert {"player_id", "rosetta_player_id", "team_id", "rosetta_team_id"} <= set(df.columns)
    assert (df["player_id"] != df["rosetta_player_id"]).all()  # DataCore vs Rosetta, never the same string
    assert set(df["match_status"]) == {"complete"} and df.schema["minutes"] == pl.Float64


def test_ids_join_across_the_match_frames(full):
    pbp = P.parse_nbl_rosetta_match_pbp(full)
    box = P.parse_nbl_rosetta_match_player_box(full)
    assert pbp.schema["player_id"] == box.schema["player_id"] == pl.String  # asserted before the join
    joined = pbp.filter(pl.col("player_id").is_not_null()).join(
        box.select("player_id", "team_id").unique(), on="player_id", how="left", suffix="_box"
    )
    assert joined["team_id_box"].null_count() == 0  # every event's person is in the box score
    assert (joined["team_id"] == joined["team_id_box"]).all()


def test_header_ids_are_datacore(full):
    row = P.parse_nbl_rosetta_match(full).row(0, named=True)
    assert row["match_id"] == MATCH and "rosetta_match_id" not in row
    assert row["home_score"] == 76 and row["away_score"] == 77 and row["attendance"] == 10802
    assert row["org"] == "nbl" and row["season_year"] == 2025 and row["season_end_year"] == 2026


# --- other routes ------------------------------------------------------------------------------


def test_season_end_year_per_org_and_era():
    assert P.parse_nbl_rosetta_matches(_stamped("org__matches__in__season__year__type--nbl_2025.json"))[
        "season_end_year"
    ].unique().to_list() == [2026]
    assert P.parse_nbl_rosetta_matches(_stamped("org__matches__in__season__year__type--nbl1_2025.json"))[
        "season_end_year"
    ].unique().to_list() == [2025]
    assert P.parse_nbl_rosetta_matches(_stamped("org__matches__in__season__year__type--nbl_1979.json"))[
        "season_end_year"
    ].unique().to_list() == [1979]
    assert P.parse_nbl_rosetta_matches(_stamped("org__matches__in__season__year__type--wnbl_2014.json"))[
        "season_end_year"
    ].unique().to_list() == [2015]


def test_end_year_and_start_year_are_inverse_and_split_1998():
    assert (
        R.start_year("nbl", 2026) == 2025 and R.start_year("nbl1", 2025) == 2025 and R.start_year("wnbl", 1997) == 1997
    )
    assert R.start_year("nbl", 1998) == R.start_year("nbl", 1999) == 1998  # one Rosetta year, two seasons
    assert R.end_year("nbl", 1998, "1998-01-30T00:00:00") == 1998 and R.end_year("wnbl", 1998, "1998-10-09") == 1999
    assert (
        R.end_year("nbl", 1998, None) is None and R.end_year("nbl", 1979) == 1979 and R.end_year("nbl1", 2025) == 2025
    )
    for org in ("nbl", "wnbl"):
        for end in list(range(1980, 1998)) + list(range(2000, 2028)):
            assert R.end_year(org, R.start_year(org, end)) == end


def test_matches_keep_only_the_season_asked_for_in_year_1998():
    body = {
        "data": [
            {"id": "a", "external_id": "a", "start_time": "1998-02-01T00:00:00", "season": {"year": "1998"}},
            {"id": "b", "external_id": "b", "start_time": "1998-11-01T00:00:00", "season": {"year": "1998"}},
        ],
        "_request": {"org": "nbl", "season": 1999, "year": 1998},
    }
    # the body names no org, so the stamped one is used
    df = P.parse_nbl_rosetta_matches(body)
    assert df.height == 0 or df["match_id"].to_list() == ["b"]


def test_matches_have_text_numbers_as_ints():
    df = P.parse_nbl_rosetta_matches(_stamped("org__matches__in__season__year__type--nbl_2025.json"))
    assert df.schema["home_score"] == pl.Int64 and df.schema["season_year"] == pl.Int64
    assert df.schema["start_time"] == pl.String and df.schema["external_media_id"] == pl.String


def test_game_log_makes_text_counts_ints():
    df = P.parse_nbl_rosetta_player_game_log(
        _stamped("org__player_boxscores__for__player_id__in__season__year__regular--nbl_2025.json")
    )
    assert df.schema["field_goals_made"] == pl.Int64 and df.schema["period"] == pl.Int64
    assert df.schema["minutes"] == pl.Float64 and df.schema["match_id"] == pl.String
    assert "rosetta_match_home_team_id" in df.columns  # the game's teams carry Rosetta ids only


def test_outcomes_map_keys_become_team_names():
    df = P.parse_nbl_rosetta_outcomes(_load("match__outcomes__for__org__teams__in__season__year__type--nbl_2025.json"))
    assert df.height == 3 and df.columns[0] == "team_name" and df.schema["wins"] == pl.Int64


def test_next_matches_one_row_per_team_and_match():
    df = P.parse_nbl_rosetta_next_matches(_stamped("next__matches__for__teams__in__org__year--nbl_2026.json"))
    assert (
        df.height == 3
        and df["season_end_year"].unique().to_list() == [2027]
        and df["org"].unique().to_list() == ["nbl"]
    )
    assert df.schema["is_home"] == pl.Boolean and df.schema["match_id"] == pl.String


def test_standings_rates_are_float_and_ratio_is_not_a_percentage():
    df = P.parse_nbl_rosetta_standings(_stamped("org__standings__year__regular--nbl_2025.json"))
    assert df.schema["win_percentage"] == pl.Float64 and df.schema["points_percentage"] == pl.Float64
    assert df.schema["position"] == pl.Int64 and df["points_percentage"].max() < 3  # a ratio around 1.1, not 113


def test_roster_bio_numbers_are_float_whatever_the_org_sends():
    a = P.parse_nbl_rosetta_players(_load("org__players__for__team__team_id__in__season__year--nbl1_2025.json"))
    b = P.parse_nbl_rosetta_players(_load("org__players__in__season__year--nbl_2025.json"))
    assert a.schema["player_height"] == b.schema["player_height"] == pl.Float64


def test_roster_drops_the_embedded_stat_and_roster_lists():
    df = P.parse_nbl_rosetta_players(_load("org__players__in__season__year--nbl_2025.json"))
    assert not {"player_season_statistics", "player_rosters"} & set(df.columns)


def test_public_parsers_are_exported_and_documented():
    names = [n for n in P.__all__ if n.startswith("parse_")]
    assert len(names) == 16 and set(names) <= set(nbl_pkg.__all__)
    for n in names:
        doc = getattr(P, n).__doc__ or ""
        assert all(k in doc for k in ("Args:", "Returns:", "Raises:", "Example:")), n
        assert ">>>" not in doc, n


# --- runtime: transport, gate, refusals --------------------------------------------------------


class _Resp:
    def __init__(self, status: int, body: Any = None, text: str | None = None):
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
        if kw.get("session") is basketball_runtime.SESSION:  # SESSION waits for the host before every attempt
            state["waited"].append(url)
        return state["queue"].pop(0) if len(state["queue"]) > 1 else state["queue"][0]

    monkeypatch.setattr(_codegen_runtime, "download", fake)
    monkeypatch.setattr(_codegen_runtime.time, "sleep", lambda s: None)
    return state


def _answer(transport, body: Any, status: int = 200) -> None:
    transport["queue"] = [_Resp(status, body)]


def test_gate_headers_and_polite_session_on_every_request(transport):
    _answer(transport, _load("org__seasons--nbl.json"))
    R._get(f"{HOST}/nbl/seasons")
    call = transport["calls"][0]
    assert (
        call["headers"]["Origin"] == "https://www.nbl.com.au"
        and call["headers"]["Referer"] == "https://www.nbl.com.au/"
    )
    assert call["session"] is basketball_runtime.SESSION and transport["waited"] == [call["url"]]


def test_caller_headers_merge_without_losing_the_gate(transport):
    _answer(transport, _load("org__seasons--nbl.json"))
    R._get(f"{HOST}/nbl/seasons", headers={"X-Test": "1"})
    assert transport["calls"][0]["headers"] == {**R.GATE_HEADERS, "X-Test": "1"}


def test_every_retry_attempt_waits_its_turn(monkeypatch):
    """A 503 retry inside download() goes through SESSION, so each attempt is spaced (one wait per attempt)."""
    waits: List[str] = []
    monkeypatch.setattr(basketball_runtime, "_polite_wait", lambda url, *a, **k: waits.append(url))
    answers = iter([_Resp(503), _Resp(200, _load("org__seasons--nbl.json"))])
    monkeypatch.setattr(
        basketball_runtime.requests.Session, "request", lambda self, method, url, *a, **k: next(answers)
    )
    monkeypatch.setattr("sportsdataverse.dl_utils.time.sleep", lambda s: None)
    body = R._get(f"{HOST}/nbl/seasons")
    assert body["type"] == "seasons" and len(waits) == 2


def test_retry_after_is_honoured_on_429(monkeypatch):
    slept: List[float] = []
    answers = iter([_Resp(429), _Resp(200, _load("org__seasons--nbl.json"))])
    first = next(answers)
    first.headers = {"content-type": "application/json", "Retry-After": "7"}
    answers = iter([first, _Resp(200, _load("org__seasons--nbl.json"))])
    monkeypatch.setattr(basketball_runtime, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr(
        basketball_runtime.requests.Session, "request", lambda self, method, url, *a, **k: next(answers)
    )
    monkeypatch.setattr("sportsdataverse.dl_utils.time.sleep", slept.append)
    R._get(f"{HOST}/nbl/seasons")
    assert any(s >= 7 for s in slept)


@pytest.mark.parametrize(
    ("org", "season", "path"),
    [
        ("nbl", 2026, "nbl/matches/in/season/2025/regular"),
        ("wnbl", 2026, "wnbl/matches/in/season/2025/regular"),
        ("nbl1", 2025, "nbl1/matches/in/season/2025/regular"),
        ("nbl", 1997, "nbl/matches/in/season/1997/regular"),
        ("nbl", 1998, "nbl/matches/in/season/1998/regular"),
        ("nbl", 1999, "nbl/matches/in/season/1998/regular"),
        ("nbl", 2000, "nbl/matches/in/season/1999/regular"),
    ],
)
def test_season_end_year_is_mapped_to_rosetta_year_in_the_url(transport, org, season, path):
    body = {
        "type": "matches",
        "data": [
            {"season": {"year": path.split("/")[4], "competition": {"league": {"organisation": {"shortcode": org}}}}}
        ],
    }
    _answer(transport, body)
    out = R._get(f"{HOST}/{org}/matches/in/season/{season}/regular")
    assert transport["calls"][0]["url"] == f"{HOST}/{path}"
    assert out[R.REQUEST_KEY] == {"org": org, "season": season, "year": int(path.split("/")[4])}


def test_next_routes_map_the_season_too(transport):
    _answer(transport, _load("next__match__for__org__year--nbl_2026.json"))
    R._get(f"{HOST}/next/match/for/nbl/2027")
    assert transport["calls"][0]["url"] == f"{HOST}/next/match/for/nbl/2026"
    _answer(transport, _load("next__match__for__team__team_code__org__year--nbl_2026.json"))
    R._get(f"{HOST}/next/match/for/team/PER/nbl/2027")
    assert transport["calls"][1]["url"] == f"{HOST}/next/match/for/team/PER/nbl/2026"


def test_list_routes_get_a_large_limit_the_caller_can_override(transport):
    _answer(transport, _load("org__teams--nbl.json"))
    R._get(f"{HOST}/nbl/teams")
    assert transport["calls"][0]["params"] == {"limit": 5000}
    R._get(f"{HOST}/nbl/teams", {"limit": 3})
    assert transport["calls"][1]["params"] == {"limit": 3}


def test_the_gate_body_is_never_data(transport):
    gate = {"error": "Access Denied", "code": "MISSING_API_KEY", "timestamp": "t", "request_id": "r", "client_ip": "i"}
    _answer(transport, gate, status=403)
    with pytest.raises(AssetFetchError):
        R._get(f"{HOST}/nbl/seasons")
    _answer(transport, gate)  # the same body under a 200
    with pytest.raises(AssetFetchError, match="MISSING_API_KEY"):
        R._get(f"{HOST}/nbl/seasons")


@pytest.mark.parametrize("stub", [[], "", {"hello": "x"}, {"type": "seasons"}, {"type": "seasons", "data": 3}])
def test_a_stub_or_non_envelope_is_a_failed_fetch(transport, stub):
    _answer(transport, stub)
    with pytest.raises((AssetFetchError, NoDataError)) as exc:
        R._get(f"{HOST}/nbl/seasons")
    assert isinstance(exc.value, AssetFetchError)


def test_empty_data_is_nothing_here_and_a_404_too(transport):
    _answer(transport, {"type": "matches", "count": 1, "data": []})  # an unknown match id even says count 1
    with pytest.raises(NoDataError):
        R._get(f"{HOST}/match/00000000-0000-0000-0000-000000000000/live/all")
    _answer(transport, {}, status=404)
    with pytest.raises(NoDataError):
        R._get(f"{HOST}/nbl/seasons")
    assert not issubclass(NoDataError, AssetFetchError) and not issubclass(AssetFetchError, NoDataError)


def test_a_failed_fetch_is_not_nothing_here(transport):
    _answer(transport, {"message": "boom"}, status=500)
    with pytest.raises(AssetFetchError):
        R._get(f"{HOST}/nbl/teams")


def test_org_and_type_are_checked_before_sending(transport):
    _answer(transport, _load("org__teams--nbl.json"))
    with pytest.raises(ValueError, match="org must be"):
        R._get(f"{HOST}/nba/teams")
    with pytest.raises(ValueError, match="type must be"):
        R._get(f"{HOST}/nbl/matches/in/season/2025/bogus")
    with pytest.raises(ValueError, match="not an NBL Rosetta route"):
        R._get(f"{HOST}/nbl/unknown")
    assert transport["calls"] == []


def _with(body: Any, **path_value: Any) -> Any:
    """A copy of a fixture body with dotted fields of every row replaced."""
    out = copy.deepcopy(body)
    for row in out["data"]:
        for dotted, value in path_value.items():
            *head, leaf = dotted.split("__")
            node = row
            for hop in head:
                node = node[hop]
            node[leaf] = value
    return out


_SC = "season__competition__league__organisation__shortcode"
SCOPE_CASES = [
    # (url, fixture, field to corrupt, ids the request may legitimately name)
    ("nbl/seasons", "org__seasons--nbl.json", {"competition__league__organisation__shortcode": "wnbl"}),
    (
        "nbl/matches/in/season/2026/regular",
        "org__matches__in__season__year__type--nbl_2025.json",
        {"season__year": "2024"},
    ),
    ("nbl/matches/in/season/2026/regular", "org__matches__in__season__year__type--nbl_2025.json", {_SC: "nbl1"}),
    ("nbl/standings/2026/regular", "org__standings__year__regular--nbl_2025.json", {"season__year": "2024"}),
    (
        f"match/{MATCH}/live/all",
        "match__match_id__live__all--nbl_2025.json",
        {"id": "11111111-1111-1111-1111-111111111111"},
    ),
    (f"match/{MATCH}", "match__match_id--nbl_2025.json", {"id": "11111111-1111-1111-1111-111111111111"}),
    (
        "nbl/players/for/team/befee10a-410c-11f0-b3f4-1b3c55488bbb/in/season/2026",
        "org__players__for__team__team_id__in__season__year--nbl_2025.json",
        {"team__external_id": "11111111-1111-1111-1111-111111111111", "team__id": "x"},
    ),
    (
        "nbl/player_boxscores/for/7619d3ea-410d-11f0-9235-2902fc2f936f/in/season/2026/regular",
        "org__player_boxscores__for__player_id__in__season__year__regular--nbl_2025.json",
        {"player__external_id": "11111111-1111-1111-1111-111111111111", "player__id": "x"},
    ),
    (
        "nbl/statistics/for/player/7619d3ea-410d-11f0-9235-2902fc2f936f",
        "org__statistics__for__player__player_id--nbl.json",
        {"player__external_id": "11111111-1111-1111-1111-111111111111", "player__id": "x"},
    ),
    (
        "nbl/stats/leaders/for/season/id/1f8e4a79-e98b-457b-85a5-e4b898c6c0bd",
        "org__stats__leaders__for__season__id__season_id--nbl_2025.json",
        {"season__id": "x", "season__external_id": "y"},
    ),
    (
        "nbl/stats/leaders/for/season/id/1f8e4a79-e98b-457b-85a5-e4b898c6c0bd/team/PER",
        "org__stats__leaders__for__season__id__season_id__team__team_code--nbl_2025.json",
        {"team__team_code": "SYD"},
    ),
    (
        "nbl/team/stats/for/season/2026/regular",
        "org__team__stats__for__season__year__regular--nbl_2025.json",
        {"season__year": "2024"},
    ),
    ("next/match/for/nbl/2027", "next__match__for__org__year--nbl_2026.json", {"season__year": "2025"}),
]


@pytest.mark.parametrize(
    ("url", "fixture", "corrupt"),
    SCOPE_CASES,
    ids=[c[0].split("/")[0] + "-" + c[0].split("/")[-1][:12] for c in SCOPE_CASES],
)
def test_a_body_for_another_scope_is_refused_not_emptied(transport, url, fixture, corrupt):
    good = _load(fixture)
    _answer(transport, good)
    assert R._get(f"{HOST}/{url}")["data"]  # the real capture answers its own request
    _answer(transport, _with(good, **corrupt))
    with pytest.raises(AssetFetchError, match="out of scope"):
        R._get(f"{HOST}/{url}")


def test_a_row_missing_the_scope_field_is_refused(transport):
    body = _load("org__standings__year__regular--nbl_2025.json")
    del body["data"][0]["season"]["year"]
    _answer(transport, body)
    with pytest.raises(AssetFetchError, match="out of scope"):
        R._get(f"{HOST}/nbl/standings/2026/regular")


def test_ids_of_either_space_are_accepted_for_scope(transport):
    body = _load("org__players__for__team__team_id__in__season__year--nbl_2025.json")
    rosetta = body["data"][0]["team"]["id"]
    _answer(transport, body)
    assert R._get(f"{HOST}/nbl/players/for/team/{rosetta}/in/season/2026")["data"]


@pytest.mark.parametrize("n", [100, 2000, 500])
def test_a_page_cut_by_the_path_cache_is_refused(transport, n):
    row = _load("org__matches__in__season__year__type--nbl_2025.json")["data"][0]
    _answer(transport, {"type": "matches", "ttlRemaining": 30, "data": [copy.deepcopy(row) for _ in range(n)]})
    with pytest.raises(AssetFetchError, match="cut by a cached request"):
        R._get(f"{HOST}/nbl/matches/in/season/2026/regular")


def test_a_caller_limit_is_never_second_guessed(transport):
    row = _load("org__matches__in__season__year__type--nbl_2025.json")["data"][0]
    _answer(transport, {"type": "matches", "data": [copy.deepcopy(row) for _ in range(100)]})
    assert len(R._get(f"{HOST}/nbl/matches/in/season/2026/regular", {"limit": 100})["data"]) == 100


def test_other_row_counts_pass_the_page_check(transport):
    row = _load("org__matches__in__season__year__type--nbl_2025.json")["data"][0]
    _answer(transport, {"type": "matches", "data": [copy.deepcopy(row) for _ in range(179)]})
    assert len(R._get(f"{HOST}/nbl/matches/in/season/2026/regular")["data"]) == 179


# --- wrappers end to end -----------------------------------------------------------------------

_EXAMPLE_FIXTURE = {
    "seasons": "org__seasons--nbl.json",
    "seasons_current": "org__seasons__current--nbl.json",
    "matches": "org__matches__in__season__year__type--nbl_2025.json",
    "teams": "org__teams--nbl.json",
    "match_by_id": "match__match_id--nbl_2025.json",
    "match": "match__match_id__live__all--nbl_2025.json",
    "match_player_box": FULL,
    "match_team_box": FULL,
    "match_player_period_box": FULL,
    "match_pbp": FULL,
    "standings": "org__standings__year__regular--nbl_2025.json",
    "players": "org__players__in__season__year--nbl_2025.json",
    "team_players": "org__players__for__team__team_id__in__season__year--nbl_2025.json",
    "outcomes": "match__outcomes__for__org__teams__in__season__year__type--nbl_2025.json",
    "player_game_log": "org__player_boxscores__for__player_id__in__season__year__regular--nbl_2025.json",
    "player_stats": "org__statistics__for__player__player_id--nbl.json",
    "stats_leaders": "org__stats__leaders__for__season__id__season_id--nbl_2025.json",
    "team_stats_leaders": "org__stats__leaders__for__season__id__season_id__team__team_code--nbl_2025.json",
    "team_stats": "org__team__stats__for__season__year__regular--nbl_2025.json",
    "next_match": "next__match__for__org__year--nbl_2026.json",
    "next_match_team": "next__match__for__team__team_code__org__year--nbl_2026.json",
    "next_matches": "next__matches__for__teams__in__org__year--nbl_2026.json",
}


@pytest.mark.parametrize("short", sorted(ENDPOINTS))
def test_every_wrapper_runs_end_to_end_on_its_example(transport, short):
    assert set(_EXAMPLE_FIXTURE) == set(ENDPOINTS)
    fn = getattr(nbl_pkg, f"nbl_rosetta_{short}")
    example = ENDPOINTS[short][5]["example_args"]
    _answer(transport, _load(_EXAMPLE_FIXTURE[short]))
    df = fn(**example)
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert list(df.columns)[: len(_schema(short))] == list(_schema(short))  # documented columns, in order
    assert transport["calls"][0]["url"].startswith(HOST + "/")
    raw = fn(**example, return_parsed=False)
    assert isinstance(raw, dict) and R.REQUEST_KEY in raw


def test_wrappers_reject_an_unknown_org_and_season_type():
    with pytest.raises(ValueError):
        nbl_pkg.nbl_rosetta_matches(season=2026, org="nba")
    with pytest.raises(ValueError):
        nbl_pkg.nbl_rosetta_matches(season=2026, season_type="bogus")


def test_the_match_page_fetched_once_feeds_every_frame(transport):
    _answer(transport, _load(FULL))
    raw = nbl_pkg.nbl_rosetta_match(match_id=MATCH, return_parsed=False)
    assert len(transport["calls"]) == 1
    assert P.parse_nbl_rosetta_match_pbp(raw).height == 96
    assert P.parse_nbl_rosetta_match_player_box(raw).height == 24
    assert P.parse_nbl_rosetta_match_team_box(raw).height == 10


# --- the endpoint YAML and the generator -------------------------------------------------------


def test_yaml_lists_every_route_of_the_spec():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    paths = json.loads((FIXTURES / "spec_paths.json").read_text(encoding="utf-8"))
    assert len(paths) == 18 and {e[0] for e in ENDPOINTS.values()} == set(paths)
    assert [e["short"] for e in doc["endpoints"]] == list(ENDPOINTS) and len(doc["endpoints"]) == 22
    assert all((SCHEMA_DIR / f"{e['short']}.yaml").exists() for e in doc["endpoints"])
    assert doc["getter_module"] == "sportsdataverse.basketball.nbl_au.nbl_rosetta_runtime"


def test_no_path_token_named_league_or_sport():
    for short, entry in ENDPOINTS.items():
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", entry[1])), short


def test_every_fixture_is_used_and_the_full_body_is_small_and_betting_free():
    used = {p.name for e in ENDPOINTS.values() for pre in e[3] for p in gen_nbl_rosetta._variants(pre)}
    stray = {p.name for p in FIXTURES.glob("*.json") if p.name != "spec_paths.json"} - used
    assert not stray, sorted(stray)
    full = FIXTURES / FULL
    assert full.stat().st_size < 300_000
    assert not re.search(r'"(betting|odds|championship_odds)"', full.read_text(encoding="utf-8"))


def test_manual_descriptions_snippet_has_no_filler():
    # the snippet is gitignored and regenerated by the gen script; it exists after a gen run
    snippet = ROOT / "tools" / "codegen" / "nbl_rosetta_column_descriptions.snippet.yaml"
    if not snippet.exists():
        pytest.skip("run tools/codegen/gen_nbl_rosetta.py to write the snippet")
    data = yaml.safe_load(snippet.read_text(encoding="utf-8"))
    assert set(data) == {f"nbl_rosetta_{s}" for s in ENDPOINTS}
    for key, cols in data.items():
        assert set(cols) == set(_schema(key.removeprefix("nbl_rosetta_"))), key
        for name, desc in cols.items():
            assert not gen_nbl_rosetta._filler(name, desc), (key, name, desc)


@pytest.mark.parametrize("short", sorted(ENDPOINTS))
def test_an_unusable_body_without_columns_still_has_the_documented_schema(short):
    """``parse_x(None)`` (no ``columns=``) is the documented zero-row frame, so it stacks onto a populated one."""
    parser, cols = ENDPOINTS[short][2], list(_schema(short))
    none = parser(None)
    # DTYPES is per parser (several endpoints can share one), so the fallback may carry extra columns
    assert none.height == 0 and set(cols) <= set(none.columns)
    assert all(none.schema[c] == _schema(short)[c] for c in cols)
    for name in ENDPOINTS[short][3]:
        for path in gen_nbl_rosetta._variants(name):
            full = parser(_stamped(str(path.relative_to(FIXTURES))), columns=cols)
            assert pl.concat([none, full], how="diagonal").height == full.height
