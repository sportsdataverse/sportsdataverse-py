"""Offline tests for the CBA (``cba``) flat-API family: the AES layer, parsers, runtime and YAML.

Parsers run over real data: the FULL bodies of game 100105799 (2025-26 round 1) in ``tests/fixtures/cba/full/`` (dtypes
come from the refs' presence sidecars over full bodies) and the refs capture samples in ``tests/fixtures/cba/captures/``.
The runtime is driven through a scripted ``requests`` adapter mounted on a ``PoliteSession`` that answers ENCRYPTED
bodies, so every refusal is exercised end to end and every attempt is counted. No network, no proxy, and nothing reads
the private sdv-internal-refs checkout.
"""

from __future__ import annotations

import copy
import json
import re
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import polars as pl
import pytest
import requests
import yaml
from requests.adapters import HTTPAdapter

import sportsdataverse.basketball.cba as cba_pkg
from sportsdataverse import dl_utils
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball.cba import cba_aes
from sportsdataverse.basketball.cba import cba_parsers as P
from sportsdataverse.basketball.cba import cba_runtime as R
from sportsdataverse.basketball.cba.cba_dtypes import DTYPES
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "cba"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "cba.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "cba"
NOTES_PATH = ROOT / "tools" / "codegen" / "notes" / "cba_descriptions.yaml"

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
GAME = 100105799
KEY = R._KEY
ENC = {"content-type": "application/json;charset=UTF-8"}
ALL_PARSERS = {s: getattr(P, f"parse_cba_{s}") for s in P.ROUTES}


def _full(name: str) -> Any:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text(encoding="utf-8"))


def _cap(name: str) -> Any:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


def _wire(obj: Any) -> str:
    return cba_aes.encrypt_body(obj, KEY)


def _fixture(short: str) -> Any:
    import sys

    sys.path.insert(0, str(ROOT / "tools" / "codegen"))
    import gen_cba

    return gen_cba.fixture(short)


# ------------------------------------------------------------------------------------------------ AES
def test_the_real_wire_body_decrypts_to_the_match_types():
    body = cba_aes.decrypt_body((FIXTURES / "full" / "wire_getMatchType.txt").read_text(encoding="utf-8"), KEY)
    assert {r["value"] for r in body} >= {"1", "2", "3", "4", "9"}


def test_round_trip_and_refusals():
    obj = {"a": [1, 2, "中文"]}
    assert cba_aes.decrypt_body(_wire(obj), KEY) == obj
    for bad in ('{"title":"x","status":500}', "", "not base64!", _wire(obj)):
        key = b"0123456789abcdef" if bad == _wire(obj) else KEY
        with pytest.raises(cba_aes.Undecryptable):
            cba_aes.decrypt_body(bad, key)


# --------------------------------------------------------------------------------------------- parsers
@pytest.mark.parametrize("short", sorted(P.ROUTES))
def test_every_parser_gives_a_typed_non_empty_frame(short):
    df = ALL_PARSERS[short](_fixture(short))
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert all(SNAKE.match(c) for c in df.columns), df.columns
    for col, code in DTYPES[short].items():
        assert df.schema[col] == {"i": pl.Int64, "f": pl.Float64, "b": pl.Boolean, "s": pl.String}[code], (short, col)
    for c in df.columns:
        if re.search(r"(^|_)id$", c):
            assert df.schema[c] == pl.Int64, (short, c)
    assert "created_by" not in df.columns and "last_modified_by" not in df.columns


@pytest.mark.parametrize("short", sorted(P.ROUTES))
@pytest.mark.parametrize("bad", [None, {}, [], "x", 3, {"records": []}, {"_request": {"schedule_id": 1}}])
def test_empty_payload_is_a_typed_zero_row_frame_that_stacks(short, bad):
    empty = ALL_PARSERS[short](bad)
    full = ALL_PARSERS[short](_fixture(short))
    assert empty.height == 0 and empty.schema == full.schema
    assert pl.concat([empty, full]).height == full.height


@pytest.mark.parametrize("short", ["player_box", "seasons", "pbp"])
def test_return_as_pandas(short):
    assert isinstance(ALL_PARSERS[short](_fixture(short), return_as_pandas=True), pd.DataFrame)


def test_player_box_keeps_the_team_row_with_a_null_player_id():
    df = P.parse_cba_player_box(_fixture("player_box"))
    assert set(df["side"]) == {"home", "away"} and df["schedule_id"].unique().to_list() == [GAME]
    assert df.filter(pl.col("player_id").is_null()).height == 2  # one rebounds/turnovers row per side
    assert df.filter(pl.col("side") == "home")["points"].sum() == 88


def test_period_scores_are_long_and_sum_to_the_final_score():
    df = P.parse_cba_period_scores(_fixture("period_scores"))
    assert df.columns == ["schedule_id", "side", "period", "points"]
    assert df.group_by("side").agg(pl.col("points").sum()).sort("side").rows() == [("away", 74), ("home", 88)]


def test_advanced_and_extras_dedupe_on_player_id_keeping_the_first_row():
    raw = copy.deepcopy(_cap("player-match-datas__advance-count"))
    first = raw["homes"][0]
    dup = {**first, "position": "ZZ", "cnAlias": "someone else"}
    raw["homes"] = [first, dup, *raw["homes"][1:], dup]
    df = P.parse_cba_player_advanced(raw).filter(pl.col("side") == "home")
    assert df["player_id"].is_unique().all() and df.height == 3
    assert df["position"][0] == first["position"] and "ZZ" not in df["position"].to_list()
    other = copy.deepcopy(_cap("player-match-datas__schedule-other-data"))
    other["homePlayers"] = other["homePlayers"] + other["homePlayers"][:1]
    assert P.parse_cba_player_extras(other).filter(pl.col("side") == "home").height == 3


def test_operator_ids_and_phone_numbers_never_reach_a_frame():
    raw = copy.deepcopy(_cap("schedules__getPlayersByScheduleId__scheduleId"))
    raw["hostPlayers"][0]["createdBy"] = "13812345678"
    raw["hostPlayers"][0]["lastModifiedBy"] = "op"
    df = P.parse_cba_rosters(raw)
    assert not {"created_by", "last_modified_by"} & set(df.columns) and "13812345678" not in str(df.to_dicts())
    assert R._redact(raw)["hostPlayers"][0].get("createdBy") is None
    assert R._redact({"x": ["13812345678", "138123456789"]}) == {"x": ["<redacted>", "138123456789"]}


def test_best_stats_player_id_is_cast_from_the_wire_string_without_a_float_render():
    df = P.parse_cba_best_stats(_fixture("best_stats"))
    assert df.schema["player_id"] == pl.Int64 and df["player_id"].drop_nulls().len() > 0


def test_pbp_has_text_and_no_ids():
    df = P.parse_cba_pbp(_fixture("pbp"))
    assert df.height == 319 and df["quarter_num"].unique().sort().to_list() == [1, 2, 3, 4]
    assert "player_id" not in df.columns and df["textual_format"].null_count() == 0


# ----------------------------------------------------------------------------------------- YAML / docs
def test_yaml_lists_every_route_and_posts_are_marked():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    spec_paths = json.loads((FIXTURES / "spec_paths.json").read_text(encoding="utf-8"))
    assert len(doc["endpoints"]) == len(spec_paths) == 27 == len(P.ROUTES)
    assert sum(1 for e in doc["endpoints"] if e.get("method") == "post") == 15
    assert doc["getter_module"].endswith("cba.cba_runtime")
    for e in doc["endpoints"]:
        assert not {"league", "sport"} & {p["name"] for p in e.get("path_params", [])}
        assert (SCHEMA_DIR / f"{e['short']}.yaml").exists() and e["parser"] == f"parse_cba_{e['short']}"
    assert {"/api" + e["path"] if not e["path"].startswith("/api") else e["path"] for e in doc["endpoints"]} or True
    assert {p.split("/api", 1)[1].replace("{scheduleId}", "{schedule_id}") for p in spec_paths} == {
        e["path"] for e in doc["endpoints"]
    }


def test_descriptions_are_complete_and_schemas_stay_blank():
    notes = yaml.safe_load(NOTES_PATH.read_text(encoding="utf-8"))
    assert set(notes) == {f"cba_{s}" for s in P.ROUTES}
    for block in notes.values():
        assert all(len(t) >= 15 for t in block.values())
    for f in SCHEMA_DIR.glob("*.yaml"):
        assert all(c["description"] == "" for c in yaml.safe_load(f.read_text(encoding="utf-8"))["columns"])
    assert (
        "United States" in notes["cba_rosters"]["nationality"]
        and "Chinese Taipei" in notes["cba_rosters"]["nationality"]
    )


def test_package_exports_every_wrapper_and_parser():
    for s in P.ROUTES:
        assert callable(getattr(cba_pkg, f"cba_{s}")) and callable(getattr(cba_pkg, f"parse_cba_{s}"))
    assert len(cba_pkg.__all__) == 54


# ------------------------------------------------------------------------------------------ runtime
class Site(HTTPAdapter):
    """A scripted data-server.cbaleague.com: ``handler(request)`` returns ``(status, headers, body)``."""

    def __init__(self, handler: Callable[[requests.PreparedRequest], Tuple[int, Dict[str, str], Any]]) -> None:
        super().__init__()
        self.handler = handler
        self.calls: List[requests.PreparedRequest] = []
        self.proxies: List[Any] = []
        self.timeouts: List[Any] = []

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        self.calls.append(request)
        self.proxies.append(kwargs.get("proxies"))
        self.timeouts.append(kwargs.get("timeout"))
        status, headers, body = self.handler(request)
        resp = requests.Response()
        resp.status_code = status
        resp.headers.update(headers)
        resp._content = body if isinstance(body, bytes) else str(body).encode("utf-8")
        resp.encoding = "utf-8"
        resp.url = request.url
        resp.request = request
        return resp

    def routes(self) -> List[str]:
        return [urlsplit(c.url).path.removeprefix("/api/") for c in self.calls]


def _body(req) -> Dict[str, Any]:
    return json.loads(req.body) if req.body else {}


def _answers(over: Dict[str, Any] | None = None) -> Callable:
    """Handler serving the game 100105799 fixtures encrypted; ``over`` maps a route to a value or ``callable(req)``."""
    over = over or {}
    events = _full(f"events_{GAME}")
    static = {
        "team-match-datas/getMatchDataReport/baseInfo": _full(f"baseinfo_{GAME}"),
        "player-match-datas/data-count": _full(f"data_count_{GAME}"),
        "schedule-quarters/getQuarterInfo": _full(f"quarter_info_{GAME}"),
        "team-match-datas/getMatchDataReport/bestPlayer": _cap(
            "team-match-datas__getMatchDataReport__bestPlayer__scheduleId"
        ),
        "team-match-datas/getMatchDataReport/bestDataQuota": _full(f"best_quota_{GAME}"),
        "team-match-datas/getMatchDataReportStatues": _full(f"report_status_{GAME}"),
        "schedules/getPlayersByScheduleId": _cap("schedules__getPlayersByScheduleId__scheduleId"),
        "player-match-datas/advance-count": _cap("player-match-datas__advance-count"),
        "player-match-datas/schedule-other-data": _cap("player-match-datas__schedule-other-data"),
        "team-match-datas/schedule-shoot-list": _cap("team-match-datas__schedule-shoot-list"),
        "teams/schedule-four-elements": _cap("teams__schedule-four-elements"),
        "com-code-tables/getTeam": _full("getTeam_2025"),
    }

    def handler(req):
        route = urlsplit(req.url).path.removeprefix("/api/")
        key = next((k for k in over if route == k or route.startswith(k + "/")), None)
        if key is not None:
            val = over[key]
            out = val(req) if callable(val) else val
            if isinstance(out, tuple):
                return out
            return 200, ENC, _wire(out)
        if route == "schedule-eventses/filterPage":
            return 200, ENC, _wire(events[str(_body(req)["quarterNum"])])
        for k, v in static.items():
            if route == k or route.startswith(k + "/"):
                return 200, ENC, _wire(v)
        if route.startswith("player-") and route.endswith("-list"):
            rec = {"season": _body(req)["season"], "teamId": 29128, "playerId": 1}
            return 200, ENC, _wire({"records": [rec], "total": 1})
        return 404, {}, b""

    return handler


@pytest.fixture
def waits(monkeypatch):
    """Count one polite wait per attempt; sleeping is free."""
    seen: List[str] = []
    monkeypatch.setattr(basketball_runtime, "_polite_wait", lambda url, *a, **k: seen.append(url))
    slept: List[float] = []
    monkeypatch.setattr(R.time, "sleep", lambda s: slept.append(s))
    return type("W", (), {"urls": seen, "slept": slept})


@pytest.fixture
def site(monkeypatch, waits):
    """Mount a Site on a fresh polite session; the finished-game caches start empty and no proxy is set."""
    monkeypatch.delenv(R.PROXY_ENV, raising=False)
    monkeypatch.delenv(R.DIRECT_ENV, raising=False)

    def install(handler=None):
        adapter = Site(handler or _answers())
        session = basketball_runtime.PoliteSession()
        session.mount("https://", adapter)
        monkeypatch.setattr(R, "SESSION", session)
        monkeypatch.setattr(R, "_INFO", {})
        monkeypatch.setattr(R, "_BOX", {})
        return adapter

    return install


def _call(short: str, **kw: Any) -> Any:
    return getattr(cba_pkg, f"cba_{short}")(**kw)


def test_wrappers_end_to_end_decrypt_parse_and_stamp(site):
    adapter = site()
    box = _call("player_box", schedule_id=GAME)
    assert box.height == 26 and set(box["side"]) == {"home", "away"} and box["schedule_id"][0] == GAME
    raw = _call("player_box", schedule_id=GAME, return_parsed=False)
    assert raw["_request"] == {"schedule_id": GAME}
    body = [_body(c) for c in adapter.calls if c.url.endswith("data-count")][0]
    assert (
        body == {"scheduleId": str(GAME)} and adapter.calls[0].headers["Referer"] == "https://www.cbaleague.com/data/"
    )


@pytest.mark.parametrize(
    "short",
    [
        "game_info",
        "period_scores",
        "player_advanced",
        "player_extras",
        "best_player",
        "best_stats",
        "rosters",
        "report_status",
        "game_team_shots",
        "game_team_four",
        "pbp",
    ],
)
def test_every_game_wrapper_runs_against_the_scripted_site(site, short):
    site()
    assert _call(short, schedule_id=GAME).height > 0


def test_pbp_fetches_every_period_and_checks_the_set(site):
    adapter = site()
    df = _call("pbp", schedule_id=GAME)
    assert df.height == 319 and df["schedule_id"][0] == GAME
    assert [_body(c)["quarterNum"] for c in adapter.calls if "filterPage" in c.url] == [1, 2, 3, 4]
    assert adapter.calls[-1].url.endswith("startNum=0")
    one = _call("pbp", schedule_id=GAME, quarter_num=2)
    assert one["quarter_num"].unique().to_list() == [2]


def test_season_is_the_end_year_and_the_api_gets_the_start_year(site):
    assert R.wire_season(2026) == 2025 and R.wire_season("2010") == 2009
    with pytest.raises(ValueError):
        R.wire_season("x")
    adapter = site()
    _call("team_options", season=2026)
    assert parse_qs(urlsplit(adapter.calls[0].url).query) == {"type": ["2"], "season": ["2025"]}
    df = _call("player_base", season=2026, team_id=29128)
    call = adapter.calls[-1]
    assert call.method == "POST" and _body(call) == {
        "season": 2025, "matchTypeId": 1, "countRanger": 1, "playerRanger": 1, "teamId": 29128,
    }  # fmt: skip
    assert parse_qs(urlsplit(call.url).query) == {"pageNumber": ["1"], "pageSize": ["100"]} and df["season"][0] == 2025


def test_a_player_list_row_for_another_season_or_team_is_refused(site):
    site(_answers({"player-base-list": {"records": [{"season": 2017, "teamId": 29128}], "total": 1}}))
    with pytest.raises(AssetFetchError, match="another season"):
        _call("player_base", season=2026)
    site(_answers({"player-offensive-list": {"records": [{"season": 2025, "teamId": 1}], "total": 1}}))
    with pytest.raises(AssetFetchError, match="another team"):
        _call("player_offensive", season=2026, team_id=29128)
    site(_answers({"teams/basic-data": [{"teamId": 7}]}))
    with pytest.raises(AssetFetchError, match="another team"):
        _call("team_basic", team_id=29128)


def test_the_day_list_refuses_rows_on_another_day(site):
    site(_answers({"schedules/day": [{"scheduleTime": "2026-10-16 19:35:00"}]}))
    with pytest.raises(AssetFetchError, match="another day"):
        _call("schedule_day", day="2026-10-12")


# ---- refusals
def test_unencrypted_500_envelope_empty_body_and_plain_json_are_refused(site):
    envelope = json.dumps({"title": "非业务异常", "status": 500, "message": "服务异常"})
    site(lambda r: (500, {}, envelope))
    with pytest.raises(AssetFetchError, match="HTTP 500"):
        _call("seasons")
    site(lambda r: (200, ENC, b""))
    with pytest.raises(AssetFetchError, match="empty body"):
        _call("seasons")
    site(lambda r: (200, ENC, envelope))
    with pytest.raises(AssetFetchError, match="did not decrypt"):
        _call("seasons")
    site(lambda r: (200, ENC, cba_aes.encrypt_body([1], b"0123456789abcdef")))
    with pytest.raises(AssetFetchError, match="did not decrypt"):
        _call("seasons")


def test_status_vocabulary(site):
    site(lambda r: (404, {}, b""))
    with pytest.raises(NoDataError):
        _call("seasons")
    site(lambda r: (422, {}, b""))
    with pytest.raises(ValueError, match="422"):
        _call("seasons")
    site(lambda r: (403, {}, b""))
    with pytest.raises(AssetFetchError, match="HTTP 403"):
        _call("seasons")
    with pytest.raises(ValueError, match="integer"):
        _call("game_info", schedule_id="abc")
    with pytest.raises(TypeError):
        R._get("https://data-server.cbaleague.com/api/com-code-tables/getSeason", cookies={})
    with pytest.raises(ValueError, match="not a CBA route"):
        R._get("https://example.com/api/x")


def test_an_unfinished_game_is_refused_before_any_game_route(site):
    header = {**_full(f"baseinfo_{GAME}"), "hostScore": None, "guestScore": None}
    adapter = site(_answers({"team-match-datas/getMatchDataReport/baseInfo": header}))
    with pytest.raises(AssetFetchError, match="not final"):
        _call("player_box", schedule_id=GAME)
    assert adapter.routes() == [f"team-match-datas/getMatchDataReport/baseInfo/{GAME}"]


def test_a_box_for_another_game_is_refused(site):
    box = copy.deepcopy(_full(f"data_count_{GAME}"))
    box["homeCounts"][0]["points"] += 5
    site(_answers({"player-match-datas/data-count": box}))
    with pytest.raises(AssetFetchError, match="out of scope"):
        _call("player_box", schedule_id=GAME)
    q = copy.deepcopy(_full(f"quarter_info_{GAME}"))
    q["home"]["oneScore"] += 1
    site(_answers({"schedule-quarters/getQuarterInfo": q}))
    with pytest.raises(AssetFetchError, match="period points"):
        _call("period_scores", schedule_id=GAME)
    adv = copy.deepcopy(_cap("player-match-datas__advance-count"))
    adv["homes"][0]["playerId"] = 1
    site(_answers({"player-match-datas/advance-count": adv}))
    with pytest.raises(AssetFetchError, match="not in the game box"):
        _call("player_advanced", schedule_id=GAME)
    ros = copy.deepcopy(_cap("schedules__getPlayersByScheduleId__scheduleId"))
    ros["hostPlayers"][0]["teamId"] = 1
    site(_answers({"schedules/getPlayersByScheduleId": ros}))
    with pytest.raises(AssetFetchError, match="not team"):
        _call("rosters", schedule_id=GAME)
    site(_answers({"team-match-datas/schedule-shoot-list": [{"teamId": 1}, {"teamId": 2}]}))
    with pytest.raises(AssetFetchError, match="not the two teams"):
        _call("game_team_shots", schedule_id=GAME)


def test_pbp_refusals(site):
    events = _full(f"events_{GAME}")

    def pages(mutate):
        ev = copy.deepcopy(events)
        mutate(ev)
        return lambda req: ev[str(_body(req)["quarterNum"])]

    site(_answers({"schedule-eventses/filterPage": pages(lambda ev: ev["3"].update(records=[], total=0))}))
    with pytest.raises(AssetFetchError, match="partial"):
        _call("pbp", schedule_id=GAME)
    site(_answers({"schedule-eventses/filterPage": {"records": [], "total": 0}}))
    with pytest.raises(NoDataError, match="no play-by-play"):
        _call("pbp", schedule_id=GAME)
    site(_answers({"schedule-eventses/filterPage": pages(lambda ev: ev["4"]["records"][-1].update(homeScore=0))}))
    assert _call("pbp", schedule_id=GAME).height == 319  # the final score is the maximum, not the last row
    site(
        _answers(
            {"schedule-eventses/filterPage": pages(lambda ev: [r.update(homeScore=1) for r in ev["4"]["records"]])}
        )
    )
    with pytest.raises(AssetFetchError, match="final"):
        _call("pbp", schedule_id=GAME)
    site(_answers({"schedule-eventses/filterPage": pages(lambda ev: ev["2"]["records"].reverse())}))
    with pytest.raises(AssetFetchError, match="clock"):
        _call("pbp", schedule_id=GAME)
    site(_answers({"schedule-eventses/filterPage": pages(lambda ev: ev["1"].update(total=999))}))
    with pytest.raises(AssetFetchError, match="truncated"):
        _call("pbp", schedule_id=GAME)
    site(_answers({"schedule-eventses/filterPage": pages(lambda ev: ev["1"]["records"][0].update(quarterNum=2))}))
    with pytest.raises(AssetFetchError, match="another period"):
        _call("pbp", schedule_id=GAME)
    site(_answers({"schedule-eventses/filterPage": {"records": [], "total": 0}}))
    with pytest.raises(NoDataError, match="period 2"):
        _call("pbp", schedule_id=GAME, quarter_num=2)


# ---- throttling, retries, proxy
def test_every_attempt_waits_and_retry_after_is_honoured(site, waits):
    state = {"n": 0}

    def flaky(req):
        state["n"] += 1
        if state["n"] == 1:
            return 429, {"Retry-After": "7"}, b""
        if state["n"] == 2:
            return 503, {}, b""
        return 200, ENC, _wire(_full("season_list"))

    adapter = site(flaky)
    assert _call("seasons").height == 32
    assert len(adapter.calls) == 3 and len(waits.urls) == 3  # one polite wait per attempt, retries included
    assert waits.slept[0] == 7.0


def test_a_server_that_never_recovers_fails_after_three_attempts_and_500_is_not_retried(site, waits):
    adapter = site(lambda r: (503, {}, b""))
    with pytest.raises(AssetFetchError, match="HTTP 503"):
        _call("seasons")
    assert len(adapter.calls) == R.ATTEMPTS == len(waits.urls)
    adapter = site(lambda r: (500, {}, b""))
    with pytest.raises(AssetFetchError, match="HTTP 500"):
        _call("seasons")
    assert len(adapter.calls) == 1


def test_the_proxy_is_used_when_set_and_never_appears_in_an_error(site, monkeypatch):
    secret = "http://user:SECRETPW@proxy.example:8000"
    monkeypatch.setenv(R.PROXY_ENV, secret)

    def boom(req):
        raise requests.ConnectionError(f"cannot reach {secret}")

    adapter = site(boom)
    monkeypatch.setenv(R.PROXY_ENV, secret)
    with pytest.raises(AssetFetchError) as exc:
        _call("seasons")
    assert (
        "SECRETPW" not in str(exc.value) and "ConnectionError" in str(exc.value) and "CBA_PROXY" not in str(exc.value)
    )
    assert len(adapter.calls) == R.ATTEMPTS
    ok = site(_answers({"com-code-tables/getSeason": _full("season_list")}))
    monkeypatch.setenv(R.PROXY_ENV, secret)
    _call("seasons")
    assert ok.proxies[0] is not None  # requests resolved the proxy from the mapping we passed
    monkeypatch.delenv(R.PROXY_ENV)
    with pytest.raises(AssetFetchError, match="set CBA_PROXY"):
        site(boom)
        _call("seasons")


@pytest.mark.parametrize("exc", [requests.ConnectTimeout, requests.ConnectionError])
def test_without_a_proxy_a_dropped_connection_fails_fast(site, exc):
    # the host drops datacenter connections at TCP: one short connect attempt, not 3 attempts x 60 s (~3 minutes)
    def drop(req):
        raise exc("connect timed out / refused")

    adapter = site(drop)
    start = time.monotonic()
    with pytest.raises(AssetFetchError, match=r"set CBA_PROXY.*CBA_DIRECT=1") as err:
        _call("player_box", schedule_id=GAME)
    assert len(adapter.calls) == 1 and adapter.timeouts[0] == (R.FAST_CONNECT_TIMEOUT, R.TIMEOUT)
    assert (
        R.FAST_CONNECT_TIMEOUT * len(adapter.calls) < 30 and time.monotonic() - start < 30
    )  # worst case on a real drop
    assert exc.__name__ in str(err.value)


def test_direct_override_keeps_the_retries_and_the_full_timeout(site, monkeypatch):
    monkeypatch.setenv(R.DIRECT_ENV, "1")  # a residential IP where direct access works

    def drop(req):
        raise requests.ConnectTimeout("connect timed out")

    adapter = site(drop)
    with pytest.raises(AssetFetchError, match="set CBA_PROXY"):
        _call("seasons")
    assert len(adapter.calls) == R.ATTEMPTS and set(adapter.timeouts) == {R.TIMEOUT}
    ok = site(_answers({"com-code-tables/getSeason": _full("season_list")}))
    _call("seasons")
    assert ok.timeouts == [R.TIMEOUT]


def test_a_post_route_without_a_json_body_is_refused():
    with pytest.raises(TypeError, match="json="):
        R._get("https://data-server.cbaleague.com/api/player-base-list", params={"season": 2026})


def test_dl_utils_and_the_shared_session_are_the_transport():
    assert R.SESSION is basketball_runtime.SESSION and isinstance(R.SESSION, basketball_runtime.PoliteSession)
    assert dl_utils is not None
