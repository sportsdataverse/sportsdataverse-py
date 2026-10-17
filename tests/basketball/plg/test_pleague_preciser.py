"""Offline tests for the Preciser (P. LEAGUE+ vendor) family (``pleague_preciser``): parsers, runtime, YAML.

Parsers are asserted against real bodies of one game (gid 197): the FULL shot list (168 shots) in
``tests/fixtures/pleague_preciser/full/``, the whole summary, rotation and injury bodies, and the site's box and
play-by-play of the same game (the join and frame tests). The runtime is driven through a scripted ``requests`` adapter
mounted on a ``PoliteSession`` (the vendor) and a scripted transport (the site, for the verified mode), so every refusal
is exercised end to end and every attempt is counted. No network, and nothing reads the private sdv-internal-refs
checkout.
"""

from __future__ import annotations

import copy
import json
import math
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import polars as pl
import pytest
import requests
import yaml
from requests.adapters import HTTPAdapter

import sportsdataverse.basketball.plg as plg_pkg
from sportsdataverse import dl_utils
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball.plg import pleague_parsers as SP
from sportsdataverse.basketball.plg import pleague_preciser_parsers as V
from sportsdataverse.basketball.plg import pleague_preciser_runtime as R
from sportsdataverse.basketball.plg import pleague_runtime as SR
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "pleague_preciser"
SITE_FIXTURES = ROOT / "tests" / "fixtures" / "pleague"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "pleague_preciser.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "pleague_preciser"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_pleague  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
ALL_PARSERS = {n: getattr(V, f"parse_pleague_preciser_{n}") for n in V.SPECS}
UUID = "e6f98675-3e5b-11ed-949e-5d17350689cf"
VENDOR = "https://api.preciser.io"
CHART = f"/v1/chart-data/id/{UUID}"
SUMMARY = f"/post/game/summary/o/plg/gameId/{UUID}"
ROTATION = f"/game/rotation/chart/o/plg/gameId/{UUID}"
INJURY = f"/game/injury/list/o/plg/gameId/{UUID}"
JSON = {"content-type": "application/json"}


def _json(root: Path, name: str) -> Any:
    return json.loads((root / name).read_text(encoding="utf-8"))


def _vendor(name: str) -> Any:
    return _json(FIXTURES, name)


def _raw(name: str, uuid: str = UUID) -> Dict[str, Any]:
    return {"data": _vendor(name)[0], "_request": {"kind": "x", "uuid": uuid, "verified": False, "gid": None}}


def _box() -> Dict[str, Any]:
    return _json(SITE_FIXTURES, "box_197.json")


# --- parsers over every committed body -------------------------------------------------------------


@pytest.mark.parametrize("short", sorted(gen_pleague.VENDOR))
def test_every_wrapper_parses_its_fixture(short):
    df = gen_pleague._frame(gen_pleague.VENDOR[short])
    assert isinstance(df, pl.DataFrame) and df.height > 0, short
    assert all(SNAKE.match(c) for c in df.columns)
    assert dict(df.schema) == dict(V.SPECS[short]), short


def test_shots_dtypes_come_from_the_full_body_and_the_ids_are_uuids():
    s = V.parse_pleague_preciser_shots(_raw("full/chart_data_197.json"))
    assert s.height == 168 and s.schema["x"] == pl.Float64 and s.schema["y"] == pl.Float64
    assert s.schema["success"] == pl.Boolean and s.schema["period"] == pl.Int64 and s.schema["has_video"] == pl.Boolean
    uuid_re = r"^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$"
    for c in ("person_id", "team_uuid", "home_team_uuid", "away_team_uuid", "game_uuid"):
        assert s[c].null_count() == 0 and s[c].str.contains(uuid_re).all(), c
    assert s["side"].null_count() == 0 and set(s["side"].unique()) == {"home", "away"}
    assert set(s["type"].unique()) == {"2pt", "3pt"} and s["subtype"].null_count() <= 2


def test_shots_join_the_site_box_on_person_id_and_the_made_counts_agree():
    s = V.parse_pleague_preciser_shots(_raw("full/chart_data_197.json"))
    box = SP.parse_pleague_box_players({**_box(), SP._REQUEST: {"gid": 197}})
    assert s["person_id"].dtype == box["person_id"].dtype == pl.Utf8  # one dtype per join key
    joined = s.join(box.select(["person_id", "name_zh", "side", "jersey"]), on="person_id", how="left", suffix="_box")
    assert joined["name_zh"].null_count() == 0  # every shooter is on the box
    assert (joined["name_zh"] == joined["player_name"]).all() and (joined["side"] == joined["side_box"]).all()
    made = s.filter(pl.col("success") == True)  # noqa: E712
    assert (
        made.filter(pl.col("type") == "2pt").height,
        made.filter(pl.col("type") == "3pt").height,
    ) == V.box_field_goals(_box()["data"])


def test_the_vendor_frame_puts_the_home_team_on_the_right_and_the_distance_matches_the_text():
    s = V.parse_pleague_preciser_shots(_raw("full/chart_data_197.json"))
    assert s.filter(pl.col("side") == "home")["x"].median() > 50 > s.filter(pl.col("side") == "away")["x"].median()
    errs = []
    for r in s.iter_rows(named=True):
        m = re.search(r"(\d+(?:\.\d+)?)公尺", r["message_zh"] or "")
        if not m:
            continue
        bx = 95 if r["x"] > 50 else 5
        errs.append(abs(math.hypot((r["x"] - bx) * 0.2875, (r["y"] - 50) * 0.1525) - float(m.group(1))))
    assert len(errs) > 100 and sum(errs) / len(errs) < 0.05  # the documented 28.75 x 15.25 m scale


def test_the_play_by_play_frame_is_the_vendor_frame_rotated_in_the_halves_the_home_team_attacks_left():
    pbp = SP.parse_pleague_pbp({**_json(SITE_FIXTURES, "full/pbp_197.json"), SP._REQUEST: {"gid": 197}})
    shots = V.parse_pleague_preciser_shots(_raw("full/chart_data_197.json"))
    key: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for r in shots.iter_rows(named=True):
        key.setdefault((r["period"], r["seconds_left"], r["player_name"], r["success"]), r)
    same = rotated = other = 0
    for r in pbp.filter(pl.col("x").is_not_null() & pl.col("action").str.contains("^fg[23]_")).iter_rows(named=True):
        v = key.get((r["period_no"], r["seconds_left"], r["player_zh"], r["action"].endswith("_made")))
        if v is None:
            continue
        near = lambda a, b: abs(a - b) < 0.51  # noqa: E731  (the site rounds to 2 decimals)
        if near(v["x"], r["x"]) and near(v["y"], r["y"]):
            same += 1
        elif near(v["x"], 100 - r["x"]) and near(v["y"], 100 - r["y"]):
            rotated += 1
        else:
            other += 1
    # 80 identical + 86 rotated of 168 matched; 2 shots fit neither (one differs in y only): the documented rule is
    # the rule for ~99% of shots, not an identity, which is why the descriptions keep the y caveat.
    assert same > 20 and rotated > 20 and other <= 3 and same + rotated >= 0.97 * (same + rotated + other)


def test_summary_frames_agree_with_the_final_score():
    teams = V.parse_pleague_preciser_summary_teams(_raw("summary_197.json"))
    assert teams["side"].to_list() == ["home", "away"] and teams["points"].to_list() == [102, 94]
    assert teams.schema["fg2_pct"] == pl.Float64 and teams.schema["fg_made"] == pl.Int64
    assert teams["team_uuid"].null_count() == 0 and teams["team_uuid"].n_unique() == 2
    log = V.parse_pleague_preciser_score_log(_raw("summary_197.json"))
    last = log.tail(1).row(0, named=True)
    assert (last["home_score"], last["away_score"]) == (102, 94) and log["seq"].to_list() == list(
        range(1, log.height + 1)
    )
    flow = V.parse_pleague_preciser_lead_flow(_raw("summary_197.json"))
    assert flow["score_difference"].to_list() == (flow["home_score"] - flow["away_score"]).to_list()
    wp = V.parse_pleague_preciser_win_probability(_raw("summary_197.json"))
    assert wp.height == 49 and wp["minute"].to_list() == list(range(49)) and wp["home_win_pct"][0] == 50.0


def test_rotation_stints_match_the_box_minutes_within_thirty_seconds():
    stints = V.parse_pleague_preciser_rotation(_raw("rotation_197.json"))
    box = SP.parse_pleague_box_players({**_box(), SP._REQUEST: {"gid": 197}}).filter(pl.col("played") == True)  # noqa: E712
    on = (
        stints.with_columns((pl.col("end_seconds") - pl.col("start_seconds")).alias("secs"))
        .group_by("name_zh")
        .agg(pl.col("secs").sum())
    )
    j = on.join(box.select(["name_zh", "seconds"]), on="name_zh", how="inner")
    assert j.height == box.height and ((j["secs"] - j["seconds"]).abs() <= V.ROTATION_TOLERANCE).all()
    assert stints.schema["jersey"] == pl.Int64 and stints["side"].unique().sort().to_list() == ["away", "home"]


def test_injury_report_shape_is_unverified_and_null():
    inj = V.parse_pleague_preciser_injury(_raw("injury_197.json"))
    assert inj["side"].to_list() == ["home", "away"] and inj["injury_report"].null_count() == 2
    assert inj["team_name_en"].null_count() == 0


def test_the_game_uuid_is_the_request_not_the_body():
    assert V.parse_pleague_preciser_shots(_raw("full/chart_data_197.json", uuid="abc"))[
        "game_uuid"
    ].unique().to_list() == ["abc"]
    s = _raw("summary_197.json")
    s["_request"] = {}
    assert V.parse_pleague_preciser_score_log(s)["game_uuid"].unique().to_list() == [UUID]  # the summary echoes it


# --- the parser contract: empty, malformed, pandas, columns --------------------------------------


@pytest.mark.parametrize("short", sorted(V.SPECS))
@pytest.mark.parametrize(
    "bad", [{}, [], None, "x", 7, {"data": None}, {"data": []}, {"data": {"shot": "x"}}, {"data": {"pbp": [1]}}]
)
def test_empty_or_malformed_payload_is_a_typed_zero_row_frame(short, bad):
    df = ALL_PARSERS[short](bad)
    assert isinstance(df, pl.DataFrame) and df.height == 0
    assert dict(df.schema) == dict(V.SPECS[short])


@pytest.mark.parametrize("short", sorted(gen_pleague.VENDOR))
def test_two_empty_frames_stack_and_stack_with_a_populated_one(short):
    empty = ALL_PARSERS[short]({})
    full = gen_pleague._frame(gen_pleague.VENDOR[short])
    assert pl.concat([empty, empty]).height == 0
    assert pl.concat([empty, full]).height == full.height
    assert pl.concat([full, empty]).schema == full.schema


@pytest.mark.parametrize("short", sorted(gen_pleague.VENDOR))
def test_return_as_pandas(short):
    entry = gen_pleague.VENDOR[short]
    pdf = entry[2](entry[3](), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and len(pdf) > 0 and list(pdf.columns) == list(V.SPECS[short])


def test_columns_argument_adds_missing_documented_columns_as_nulls():
    df = V.parse_pleague_preciser_shots({}, columns=["game_uuid", "brand_new_column"])
    assert df.height == 0 and df.schema["brand_new_column"] == pl.Utf8


def test_no_frame_keeps_a_url_or_a_float_origin_id():
    for short in V.SPECS:
        df = gen_pleague._frame(gen_pleague.VENDOR[short])
        assert not [c for c in df.columns if "url" in c and c != "logo_url"], short
        for c, dt in df.schema.items():
            if dt == pl.Utf8:
                assert not df[c].drop_nulls().str.contains(r"^\d+\.0$").any(), (short, c)
                assert not df[c].drop_nulls().str.contains(r"key=").any(), (short, c)


def test_every_public_parser_has_a_documented_example_and_helpers_are_private():
    assert set(V.__all__) == {f"parse_pleague_preciser_{n}" for n in V.SPECS}
    for name in V.__all__:
        doc = getattr(V, name).__doc__ or ""
        for part in ("Args:", "Returns:", "Raises:", "Example:", "Quick start::"):
            assert part in doc, (name, part)
        assert ">>>" not in doc
        wrapper = re.search(r"``(pleague_preciser_\w+)`` wrapper", doc)
        assert wrapper and wrapper.group(1) in plg_pkg.__all__, name  # the docstring names a real wrapper
    assert set(V.__all__) <= set(plg_pkg.__all__)


# --- scrubbing --------------------------------------------------------------------------------


def test_scrub_replaces_the_vendor_access_key_everywhere():
    key = "0123456789abcdef0123456789abcdef"
    body = {"a": [{"u": f"https://x/v.mp4?key={key}"}], "b": f"https://x/t.jpg?a=1&key={key}", "c": "key=short"}
    out = V.scrub(body)
    assert key not in json.dumps(out) and out["a"][0]["u"].endswith("?key=<redacted>")
    assert out["b"].endswith("&key=<redacted>") and out["c"] == "key=short"
    assert V.has_secret(body) and not V.has_secret(out)


def test_the_committed_bodies_carry_no_access_key():
    for p in list(FIXTURES.rglob("*.json")) + list(SITE_FIXTURES.rglob("*.json")):
        assert not V.has_secret(json.loads(p.read_text(encoding="utf-8"))), p.name


# --- the YAML, schemas and generator ----------------------------------------------------------------


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def test_yaml_lists_every_spec_route():
    spec_paths = set(json.loads((FIXTURES / "spec_paths.json").read_text(encoding="utf-8")))
    assert len(spec_paths) == 4 and spec_paths == {p for e in gen_pleague.VENDOR.values() for p in e[0]}
    doc = _yaml()
    assert {(e["short"], e["path"]) for e in doc["endpoints"]} == {(s, e[1]) for s, e in gen_pleague.VENDOR.items()}
    assert len(doc["endpoints"]) == 7 and set(V.SPECS) == {e["short"] for e in doc["endpoints"]}
    for e in doc["endpoints"]:
        assert [p["name"] for p in e["path_params"]] == ["uuid"] and [p["name"] for p in e["extra_params"]] == ["gid"]
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"]))
    assert doc["host"] == "https://api.preciser.io" and doc["name_pattern"] == "pleague_preciser_{short}"


def test_yaml_carries_the_third_party_flag_owner_flag_and_coverage_notes():
    notes = " ".join(_yaml()["docstring"]["notes"])
    assert "data release needs owner sign-off" in notes and "THIRD-PARTY" in notes and "no robots.txt" in notes
    assert "at least 1 s" in notes and "2022-23" in notes and "x_vendor = 100 - x_pbp" in notes and "?key=" in notes
    assert "e86394aa-3d88-11ed-8ba8-7f44ba46b0b1" in notes  # the clubs table (site team_id -> Preciser uuid)


@pytest.mark.parametrize("short", sorted(gen_pleague.VENDOR))
def test_schema_matches_the_parser_and_carries_no_descriptions(short):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert [c["name"] for c in schema["columns"]] == list(V.SPECS[short])
    assert {c["description"] for c in schema["columns"]} == {""}


def test_unverified_caveats_and_the_frame_documentation_stay_in_the_descriptions():
    notes = yaml.safe_load(gen_pleague.NOTES.read_text(encoding="utf-8"))
    shots = notes["pleague_preciser_shots"]
    assert "unverified" in shots["y"] and "x_vendor = 100 - x_pbp" in shots["x"] and "28.75" in shots["x"]
    assert "unverified" in notes["pleague_preciser_injury"]["injury_report"]
    assert (
        "COUNT" in notes["pleague_preciser_summary_teams"]["fg_made"]
        or "count" in notes["pleague_preciser_summary_teams"]["fg_made"]
    )
    assert "e86394aa-3d88-11ed-8ba8-7f44ba46b0b1" in shots["team_uuid"]


# --- runtime: a scripted vendor ---------------------------------------------------------------------


class Vendor(HTTPAdapter):
    """A scripted Preciser host: ``handler(request)`` returns ``(status, headers, body)``; every request is recorded."""

    def __init__(self, handler: Callable[[requests.PreparedRequest], Tuple[int, Dict[str, str], Any]]) -> None:
        super().__init__()
        self.handler = handler
        self.calls: List[requests.PreparedRequest] = []

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        self.calls.append(request)
        status, headers, body = self.handler(request)
        resp = requests.Response()
        resp.status_code = status
        resp.headers.update(headers)
        resp._content = body if isinstance(body, bytes) else str(body).encode("utf-8")
        resp.encoding = "utf-8"
        resp.url = headers.get("x-final-url") or request.url
        resp.request = request
        return resp

    def paths(self) -> List[str]:
        return [c.url.split("api.preciser.io", 1)[1] for c in self.calls]


@pytest.fixture
def waits(monkeypatch):
    """Count one polite wait per attempt on both runtimes; sleeping is free."""
    SR.clear_cache()
    seen: List[str] = []
    monkeypatch.setattr(basketball_runtime, "_polite_wait", lambda url, *a, **k: seen.append(url))
    monkeypatch.setattr(SR, "_polite_wait", lambda url, *a, **k: seen.append(url))
    slept: List[float] = []
    monkeypatch.setattr(dl_utils.time, "sleep", lambda s: slept.append(s))
    yield type("W", (), {"urls": seen, "slept": slept})
    SR.clear_cache()


@pytest.fixture
def vendor(monkeypatch, waits):
    def install(handler):
        adapter = Vendor(handler)
        session = basketball_runtime.PoliteSession()
        session.mount("https://", adapter)
        monkeypatch.setattr(R, "SESSION", session)
        return adapter

    return install


def _bodies(
    overrides: Optional[Dict[str, Any]] = None,
) -> Callable[[requests.PreparedRequest], Tuple[int, Dict[str, str], Any]]:
    table: Dict[str, Any] = {
        CHART: json.dumps(_vendor("full/chart_data_197.json")),
        SUMMARY: json.dumps(_vendor("summary_197.json")),
        ROTATION: json.dumps(_vendor("rotation_197.json")),
        INJURY: json.dumps(_vendor("injury_197.json")),
    }
    table.update(overrides or {})

    def handler(req):
        body = table.get(req.url.split("api.preciser.io", 1)[1])
        if body is None:
            return 404, JSON, "not found"
        return body if isinstance(body, tuple) else (200, JSON, body)

    return handler


def _edited(name: str, fn: Callable[[Dict[str, Any]], None]) -> str:
    body = copy.deepcopy(_vendor(name))
    fn(body[0])
    return json.dumps(body)


def test_wrappers_build_the_vendors_url_and_every_request_waits(vendor, waits):
    adapter = vendor(_bodies())
    assert plg_pkg.pleague_preciser_shots(uuid=UUID).height == 168
    assert plg_pkg.pleague_preciser_summary_teams(uuid=UUID).height == 2
    assert plg_pkg.pleague_preciser_rotation(uuid=UUID).height > 50
    assert plg_pkg.pleague_preciser_injury(uuid=UUID).height == 2
    assert adapter.paths() == [CHART, SUMMARY, ROTATION, INJURY]
    assert len(waits.urls) == len(adapter.calls)  # one throttled wait per request


def test_return_parsed_false_hands_back_the_stamped_game_object(vendor):
    vendor(_bodies())
    raw = plg_pkg.pleague_preciser_score_log(uuid=UUID, return_parsed=False)
    assert raw[R.REQUEST_KEY] == {"kind": "summary", "uuid": UUID, "verified": False, "gid": None}
    assert raw["data"]["game_id"] == UUID and isinstance(raw["data"]["pbp"], list)


def test_status_codes_follow_the_error_vocabulary(vendor, waits):
    vendor(_bodies({SUMMARY: (404, JSON, "x")}))
    with pytest.raises(NoDataError):
        plg_pkg.pleague_preciser_summary_teams(uuid=UUID)
    vendor(_bodies({SUMMARY: (422, JSON, "x")}))
    with pytest.raises(ValueError, match="422"):
        plg_pkg.pleague_preciser_summary_teams(uuid=UUID)
    vendor(_bodies({SUMMARY: (403, JSON, "x")}))
    with pytest.raises(AssetFetchError, match="403"):
        plg_pkg.pleague_preciser_summary_teams(uuid=UUID)


def test_http_500_is_not_retried_and_is_assetfetcherror(vendor, waits):
    adapter = vendor(_bodies({CHART: (500, {"content-type": "text/html"}, "oops")}))
    with pytest.raises(AssetFetchError, match="500"):
        plg_pkg.pleague_preciser_shots(uuid=UUID)
    assert len(adapter.calls) == 1  # an unknown uuid also answers 500: it is not retried


def test_every_retry_attempt_waits_its_turn_and_5xx_exhausts(vendor, waits):
    adapter = vendor(_bodies({SUMMARY: (503, {**JSON, "Retry-After": "2"}, "busy")}))
    with pytest.raises(AssetFetchError, match="503"):
        plg_pkg.pleague_preciser_summary_teams(uuid=UUID)
    assert len(adapter.calls) == R.ATTEMPTS == len(waits.urls) and max(waits.slept) >= 2  # Retry-After honoured


def test_a_transient_429_then_200_succeeds(vendor, waits):
    state = {"n": 0}
    inner = _bodies()

    def handler(req):
        state["n"] += 1
        return (429, {**JSON, "Retry-After": "1"}, "slow") if state["n"] == 1 else inner(req)

    adapter = vendor(handler)
    assert plg_pkg.pleague_preciser_injury(uuid=UUID).height == 2
    assert len(adapter.calls) == 2 == len(waits.urls)


def test_a_connection_error_is_assetfetcherror_without_the_detail(vendor):
    def boom(req):
        raise requests.ConnectionError("secret-host-detail")

    vendor(boom)
    with pytest.raises(AssetFetchError) as exc:
        plg_pkg.pleague_preciser_shots(uuid=UUID)
    assert "ConnectionError" in str(exc.value) and "secret-host-detail" not in str(exc.value)


def test_an_empty_list_and_empty_rotation_lists_are_nodata(vendor):
    vendor(_bodies({SUMMARY: "[]", ROTATION: json.dumps([{"awayTeam": [], "homeTeam": []}])}))
    with pytest.raises(NoDataError, match="\\[\\]"):
        plg_pkg.pleague_preciser_summary_teams(uuid=UUID)
    with pytest.raises(NoDataError, match="empty rotation"):
        plg_pkg.pleague_preciser_rotation(uuid=UUID)


def test_non_json_wrong_shape_and_off_host_answers_are_refused(vendor):
    for over, match in (
        ({CHART: (200, {"content-type": "text/html"}, "<html>")}, "not JSON"),
        ({CHART: (200, JSON, "not json{")}, "not JSON"),
        ({CHART: (200, JSON, "{}")}, "one-element list"),
        ({CHART: (200, JSON, "[1]")}, "one-element list"),
        ({CHART: (200, {**JSON, "x-final-url": "https://example.com/v1"}, "[{}]")}, "off the Preciser host"),
    ):
        vendor(_bodies(over))
        with pytest.raises(AssetFetchError, match=match):
            plg_pkg.pleague_preciser_shots(uuid=UUID)


@pytest.mark.parametrize(
    "short,name,fn,match",
    [
        (
            "summary_teams",
            "summary_197.json",
            lambda b: b.update(game_id="00000000-0000-0000-0000-000000000000"),
            "game_id",
        ),
        ("summary_teams", "summary_197.json", lambda b: b.update(pbp=[]), "play log is empty"),
        ("summary_teams", "summary_197.json", lambda b: b["home_team"].pop("id"), "team id is missing"),
        ("injury", "injury_197.json", lambda b: b.update(game_id="x"), "game_id"),
        ("shots", "full/chart_data_197.json", lambda b: b.update(game_status="live"), "not post"),
        ("shots", "full/chart_data_197.json", lambda b: b["shot"][0].update(player="nobody"), "names a player"),
    ],
)
def test_a_body_that_is_not_this_games_is_refused(vendor, short, name, fn, match):
    path = {"summary_teams": SUMMARY, "injury": INJURY, "shots": CHART}[short]
    vendor(_bodies({path: _edited(name, fn)}))
    with pytest.raises(AssetFetchError, match=match):
        getattr(plg_pkg, f"pleague_preciser_{short}")(uuid=UUID)


def test_the_access_key_is_scrubbed_from_the_returned_body_and_a_survivor_is_refused(vendor, monkeypatch):
    key = "0123456789abcdef0123456789abcdef"
    body = _edited("full/chart_data_197.json", lambda b: b["shot"][0].update(video_url=f"https://c/v.mp4?key={key}"))
    vendor(_bodies({CHART: body}))
    raw = plg_pkg.pleague_preciser_shots(uuid=UUID, return_parsed=False)
    assert key not in json.dumps(raw) and raw["data"]["shot"][0]["video_url"].endswith("?key=<redacted>")
    monkeypatch.setattr(V, "scrub", lambda obj: obj)  # a scrub that failed
    with pytest.raises(AssetFetchError, match="access key survived"):
        plg_pkg.pleague_preciser_shots(uuid=UUID)


def test_argument_checks_happen_before_any_request(vendor):
    adapter = vendor(_bodies())
    with pytest.raises(ValueError, match="uuid must be"):
        plg_pkg.pleague_preciser_shots(uuid="197")
    with pytest.raises(ValueError, match="gid"):
        plg_pkg.pleague_preciser_shots(uuid=UUID, gid="x")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="unsupported"):
        R._get(f"{VENDOR}{CHART}", proxy=1)
    with pytest.raises(ValueError, match="not a Preciser route"):
        R._get(f"{VENDOR}/elsewhere/{UUID}")
    assert adapter.calls == []


# --- the verified mode: the vendor body against the site's game page and box ------------------------


class FakeResp:
    def __init__(self, status: int = 200, text: str = "", ctype: str = "text/html; charset=UTF-8") -> None:
        self.status_code, self.text, self.headers = status, text, {"content-type": ctype}


@pytest.fixture
def both(monkeypatch, vendor):
    """Vendor + site scripted together; returns ``(vendor adapter, site calls)``."""

    def install(vendor_over=None, box=None, game_html=None):
        adapter = vendor(_bodies(vendor_over))
        site_calls: List[str] = []
        pages = {
            "/game/197": (SITE_FIXTURES / "game_197.html").read_text(encoding="utf-8"),
            "/game/206": (SITE_FIXTURES / "game_206.html").read_text(encoding="utf-8"),
        }
        if game_html is not None:
            pages["/game/197"] = game_html
        box_text = json.dumps(box if box is not None else _box())

        def transport(url, params):
            path = url.split("pleagueofficial.com", 1)[1]
            site_calls.append(path)
            if path == "/api/boxscore.preciser.php":
                return FakeResp(200, box_text)
            return FakeResp(200, pages[path]) if path in pages else FakeResp(404, "nope")

        monkeypatch.setattr(SR, "_transport", transport)
        return adapter, site_calls

    return install


def test_verified_mode_fetches_the_game_page_box_and_summary_first_and_stamps_it(both, waits):
    adapter, site_calls = both()
    raw = plg_pkg.pleague_preciser_shots(uuid=UUID, gid=197, return_parsed=False)
    assert site_calls == ["/game/197", "/api/boxscore.preciser.php"] and adapter.paths() == [SUMMARY, CHART]
    assert raw[R.REQUEST_KEY] == {"kind": "shots", "uuid": UUID, "verified": True, "gid": 197}
    assert len(waits.urls) == len(site_calls) + len(adapter.calls)
    plg_pkg.pleague_preciser_rotation(uuid=UUID, gid=197)
    assert site_calls == ["/game/197", "/api/boxscore.preciser.php"]  # cached: the site is not asked again
    assert adapter.paths()[-1] == ROTATION


def test_verified_summary_and_injury_pass_on_the_real_bodies(both):
    both()
    assert plg_pkg.pleague_preciser_summary_teams(uuid=UUID, gid=197).height == 2
    assert plg_pkg.pleague_preciser_injury(uuid=UUID, gid=197).height == 2


def test_a_gid_whose_game_has_another_uuid_is_refused(both):
    both()
    with pytest.raises(AssetFetchError, match="has uuid"):
        plg_pkg.pleague_preciser_shots(uuid=UUID, gid=206)


def test_verified_shots_must_match_the_box_made_counts(both):
    box = _box()
    box["data"]["home"][0]["two_m_two"] = "9-9"
    both(box=box)
    with pytest.raises(AssetFetchError, match="made 2pt/3pt"):
        plg_pkg.pleague_preciser_shots(uuid=UUID, gid=197)


def test_verified_summary_play_log_must_end_on_the_final_score(both):
    both({SUMMARY: _edited("summary_197.json", lambda b: b["pbp"][-1].update(home_score=99))})
    with pytest.raises(AssetFetchError, match="final score"):
        plg_pkg.pleague_preciser_summary_teams(uuid=UUID, gid=197)


def test_verified_shots_and_injury_teams_must_equal_the_summarys(both):
    both({CHART: _edited("full/chart_data_197.json", lambda b: b.update(home_id=b["away_id"]))})
    with pytest.raises(AssetFetchError, match="home_id / away_id"):
        plg_pkg.pleague_preciser_shots(uuid=UUID, gid=197)
    both({INJURY: _edited("injury_197.json", lambda b: b["home_team"].update(id=b["away_team"]["id"]))})
    with pytest.raises(AssetFetchError, match="team ids differ"):
        plg_pkg.pleague_preciser_injury(uuid=UUID, gid=197)


def test_verified_rotation_must_match_the_roster_and_the_box_minutes(both):
    both({ROTATION: _edited("rotation_197.json", lambda b: b["homeTeam"][0].update(name="路人甲"))})
    with pytest.raises(AssetFetchError, match="not on the home box roster"):
        plg_pkg.pleague_preciser_rotation(uuid=UUID, gid=197)

    def stretch(b: Dict[str, Any]) -> None:
        b["awayTeam"][0]["periods"][0]["endSeconds"] += 300

    both({ROTATION: _edited("rotation_197.json", stretch)})
    with pytest.raises(AssetFetchError, match="on court"):
        plg_pkg.pleague_preciser_rotation(uuid=UUID, gid=197)


def test_without_a_gid_the_cross_checks_are_skipped_and_the_site_is_not_asked(both):
    box = _box()
    box["data"]["home"][0]["two_m_two"] = "9-9"
    _adapter, site_calls = both(box=box)
    assert plg_pkg.pleague_preciser_shots(uuid=UUID).height == 168  # unverified: only the body's own checks run
    assert site_calls == []
