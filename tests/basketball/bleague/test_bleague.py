"""B.LEAGUE family: parsers over the committed fixtures, the YAML, the runtime's refusals and throttling.

Reads only committed files (CI has no sdv-internal-refs checkout): the YAML, the spec-path snapshot, the fixtures.
The generator is imported for its ENDPOINTS table only; its ``main()`` (which reads the refs) is not called.
"""

from __future__ import annotations

import copy
import gzip
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd
import polars as pl
import pytest
import requests
import yaml
from requests.adapters import HTTPAdapter

import sportsdataverse.basketball.bleague as pkg
from sportsdataverse import dl_utils
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball.bleague import bleague as api
from sportsdataverse.basketball.bleague import bleague_codes as C
from sportsdataverse.basketball.bleague import bleague_parsers as P
from sportsdataverse.basketball.bleague import bleague_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).resolve().parents[3]
FIX = ROOT / "tests" / "fixtures" / "bleague"
DOC = yaml.safe_load((ROOT / "tools" / "codegen" / "endpoints" / "bleague.yaml").read_text(encoding="utf-8"))
SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
S3 = "https://b-league.s3.amazonaws.com/web_json"
SITE = "https://www.bleague.jp"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_bleague  # noqa: E402


def _full(name: str) -> Dict[str, Any]:
    with gzip.open(FIX / "full" / f"{name}.json.gz", "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _cap(name: str) -> Dict[str, Any]:
    return json.loads((FIX / "captures" / f"{name}.json").read_text(encoding="utf-8"))


def _html(name: str) -> str:
    p = FIX / "html" / name
    if name.endswith(".gz"):
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            return fh.read()
    return p.read_text(encoding="utf-8")


def _stamp(body: Dict[str, Any], key: int, feed: str) -> Dict[str, Any]:
    body = copy.deepcopy(body)
    body[R.REQUEST_KEY] = {"schedule_key": key, "feed": feed}
    return body


V2 = lambda: _stamp(_full("v2_504734"), 504734, "v2")  # noqa: E731
V1 = lambda: _stamp(_full("v1_10693"), 10693, "v1")  # noqa: E731


# ---------------------------------------------------------------- parsers over every fixture
@pytest.mark.parametrize("short", sorted(gen_bleague.ENDPOINTS))
def test_every_wrapper_parses_its_fixture(short):
    df = gen_bleague._frame(gen_bleague.ENDPOINTS[short])
    assert isinstance(df, pl.DataFrame) and df.height > 0, short
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    assert dict(df.schema) == dict(P.SPECS[short]), short


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_id_columns_are_text_and_never_float_stringified(short):
    df = gen_bleague._frame(gen_bleague.ENDPOINTS[short])
    for c in df.columns:
        if (
            c.endswith("_id") and c != "event_id"
        ) or c == "convention_key":  # event_id is a competition-type code (Int64)
            assert df.schema[c] == pl.Utf8, c
            assert not df[c].drop_nulls().str.contains(r"\.").any(), c
    assert df.schema["schedule_key"] == pl.Int64 if "schedule_key" in df.columns else True


def test_v1_and_v2_ids_agree_on_dtype_and_season_is_the_end_year():
    for body in (V1(), V2()):
        box = P.parse_bleague_player_box(body)
        assert box.schema["team_id"] == pl.Utf8 and box.schema["player_id"] == pl.Utf8
        assert box["season"].unique().to_list() == [2026]
    a, b = P.parse_bleague_pbp(V1()), P.parse_bleague_pbp(V2())
    assert a.schema["team_id"] == b.schema["team_id"] == pl.Utf8


def test_numeric_dtypes_come_from_the_full_bodies():
    box = P.parse_bleague_player_box(V2())
    assert box.schema["efg"] == pl.Float64 and box.schema["points"] == pl.Int64
    assert box["efg"].drop_nulls().len() > 0
    pbp = P.parse_bleague_pbp(V2())
    assert pbp.schema["x"] == pl.Float64 and pbp["x"].drop_nulls().len() > 0
    assert pbp.schema["recorded_at"] == pl.Datetime("ms", "UTC")


def test_v2_placeholders_become_null_not_zero():
    box = P.parse_bleague_team_box(V2())
    quarter_team = box.filter((pl.col("category") == 2) & (pl.col("period_category") == 1))
    assert quarter_team["points"].null_count() == quarter_team.height > 0


def test_team_rebound_codes_swap_between_feeds_and_substitutions_differ():
    assert C.action_name(18, "v2") == "team_rebound_def" and C.action_name(18, "v1") == "team_rebound_off"
    assert C.action_name(19, "v2") == "team_rebound_off" and C.action_name(19, None) is None
    v1 = P.parse_bleague_pbp(V1())
    assert (v1["action"] == "substitution").sum() > 0 and v1["player2_id"].drop_nulls().len() > 0
    v2 = P.parse_bleague_pbp(V2())
    assert {"player_in", "player_out"} <= set(v2["action"].drop_nulls())


def test_shots_exist_only_with_locations():
    shots = P.parse_bleague_shots(V2())
    assert shots.height > 100 and shots["x"].null_count() == 0
    assert set(shots["shot_type"].unique()) == {"2pt", "3pt"} and shots["side"].drop_nulls().len() > 0
    assert P.parse_bleague_shots(V1()).height == 0  # v1 has no coordinates
    assert P.parse_bleague_shots(_cap("v2_genius_contexts__ScheduleKey__latestId@5856")).height == 0  # 2020-21


def test_leaders_and_score_track_are_v1_only():
    assert P.parse_bleague_leaders(V1()).height > 0 and P.parse_bleague_score_track(V1()).height > 0
    assert P.parse_bleague_leaders(V2()).height == 0 and P.parse_bleague_score_track(V2()).height == 0


def test_game_info_and_period_scores_match_the_header():
    info = P.parse_bleague_game_info(V2())
    assert info["year"][0] == 2025 and info["season"][0] == 2026 and info["home_score"][0] == 56
    assert info["game_datetime"][0].isoformat().startswith("2025-10-03T10:05")  # 19:05 JST
    per = P.parse_bleague_period_scores(V2())
    tot = per.group_by("team_side").agg(pl.col("points").sum()).sort("team_side")
    assert tot["points"].to_list() == [81, 56]
    emb = P.parse_bleague_game_info(_embed_body())
    assert emb["season"][0] == 2017 and emb["game_datetime"][0] is not None  # epoch seconds


def _embed_body() -> Dict[str, Any]:
    body, feed = R._embed_body(_html("game_detail_1.html"), 1)
    body[R.REQUEST_KEY] = {"schedule_key": 1, "feed": feed}
    return body


def test_embed_context_parses_as_v1():
    pbp = P.parse_bleague_pbp(_embed_body())
    assert pbp.height > 0 and pbp["feed"].unique().to_list() == ["v1"]


def test_schedule_standings_pages_parse_and_match_the_refs_records():
    sch = P._schedule_record(_html("schedule_2025_1004.html.gz"), 1, 2025, 2, 10, 4)
    cap = _cap("schedule")
    assert [g["schedule_key"] for g in sch["games"][:3]] == [g["schedule_key"] for g in cap["games"]]
    df = P.parse_bleague_schedule(sch)
    assert df.height == 12 and df["season"].unique().to_list() == [2026] and df.schema["home_team_id"] == pl.Utf8
    assert P.parse_bleague_schedule_events(sch)["selected"].sum() == 1
    assert P.parse_bleague_schedule_days(sch).height >= 3
    st = P._standings_record(_html("standings_2025_b1.html.gz"), 2025, 1)
    assert P.parse_bleague_standings(st).height > 10 and {r["team_id"] for r in cap_standings()["rows"]} <= {
        r["team_id"] for r in st["rows"]
    }


def cap_standings() -> Dict[str, Any]:
    return _cap("standings")


# ------------------------------------------------------------------- parser contract
@pytest.mark.parametrize("short", sorted(P.SPECS))
@pytest.mark.parametrize("bad", [{}, [], None, "x", {"Game": 3}])
def test_empty_payload_is_a_typed_zero_row_frame_and_stacks(short, bad):
    fn = getattr(P, f"parse_bleague_{short}")
    a, b = fn(bad), fn(bad)
    assert a.height == 0 and dict(a.schema) == dict(P.SPECS[short])
    assert pl.concat([a, b]).height == 0


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_return_as_pandas(short):
    df = getattr(P, f"parse_bleague_{short}")(gen_bleague.ENDPOINTS[short][3](), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame) and len(df) > 0


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_every_public_parser_documents_args_returns_raises_example(short):
    doc = getattr(P, f"parse_bleague_{short}").__doc__
    for head in ("Args:", "Returns:", "Raises:", "Example:"):
        assert head in doc, (short, head)
    assert ">>>" not in doc


# --------------------------------------------------------------------------- the YAML
def test_the_yaml_lists_every_route_and_shape():
    paths = set(json.loads((FIX / "spec_paths.json").read_text(encoding="utf-8")))
    assert {"/game_detail/", "/schedule/", "/standings/"} <= paths and len(paths) == 7
    eps = DOC["endpoints"]
    assert len(eps) == 14 == len({e["short"] for e in eps}) == len(gen_bleague.ENDPOINTS)
    for e in eps:
        tokens = re.findall(r"\{([^}]+)\}", e["path"])
        assert not {"league", "sport"} & set(tokens)
        assert tokens == [p["name"] for p in e.get("path_params", [])]
        assert e["parser"].startswith("parse_bleague_") and e["returns_schema"] == f"native/bleague/{e['short']}"
        assert (ROOT / "tools/codegen/schemas/native/bleague" / f"{e['short']}.yaml").exists()
    assert DOC["getter_module"] == "sportsdataverse.basketball.bleague.bleague_runtime"
    notes = " ".join(DOC["docstring"]["notes"])
    for needle in ("Unofficial", "START year", "v2", "embed"):
        assert needle in notes


def test_descriptions_are_complete_translated_and_not_in_schemas():
    notes = yaml.safe_load((ROOT / "tools/codegen/notes/bleague_descriptions.yaml").read_text(encoding="utf-8"))
    assert len(notes) == 14
    for short in gen_bleague.ENDPOINTS:
        cols = yaml.safe_load((ROOT / "tools/codegen/schemas/native/bleague" / f"{short}.yaml").read_text())["columns"]
        block = notes[f"bleague_{short}"]
        assert {c["name"] for c in cols} == set(block)
        for c in cols:
            assert c["description"] == ""
            assert len(block[c["name"]]) >= 15 and block[c["name"]].lower() != c["name"]
    assert "East" in notes["bleague_standings"]["section"] and "Central" in notes["bleague_standings"]["section"]


def test_wrappers_are_exported_and_documented():
    names = [f"bleague_{e['short']}" for e in DOC["endpoints"]]
    assert set(names) <= set(pkg.__all__) and all(hasattr(api, n) for n in names)
    assert not hasattr(pkg, "SPECS")
    for n in names:
        doc = getattr(api, n).__doc__
        assert "Example:" in doc and "Raises:" in doc


# ----------------------------------------------------------------- the runtime, mocked
class Site(HTTPAdapter):
    def __init__(self, handler: Callable[[requests.PreparedRequest], Tuple[int, Dict[str, str], Any]]) -> None:
        super().__init__()
        self.handler, self.calls = handler, []

    def send(self, request, **kwargs):  # type: ignore[override]
        self.calls.append(request)
        status, headers, body = self.handler(request)
        resp = requests.Response()
        resp.status_code = status
        resp.headers.update(headers)
        resp._content = body if isinstance(body, bytes) else str(body).encode("utf-8")
        resp.encoding = "utf-8"
        resp.url, resp.request = request.url, request
        return resp


@pytest.fixture
def waits(monkeypatch):
    seen: List[str] = []
    monkeypatch.setattr(basketball_runtime, "_polite_wait", lambda url, *a, **k: seen.append(url))
    slept: List[float] = []
    monkeypatch.setattr(dl_utils.time, "sleep", lambda s: slept.append(s))
    return type("W", (), {"urls": seen, "slept": slept})


@pytest.fixture
def site(monkeypatch, waits):
    def install(handler):
        adapter = Site(handler)
        session = basketball_runtime.PoliteSession()
        session.mount("https://", adapter)
        monkeypatch.setattr(R, "SESSION", session)
        return adapter

    return install


JSON = {"content-type": "application/json"}
DENIED = (403, {"content-type": "application/xml"}, "<Error><Code>AccessDenied</Code></Error>")


def _s3(feed_bodies: Dict[str, Any], *, embed: str = ""):
    """feed_bodies: {"v2_genius_contexts/504734": body}; anything else is S3 AccessDenied."""

    def handler(req):
        u = req.url.split("?")[0]
        if u.startswith(S3):
            rest = u[len(S3) + 1 :]
            for k, body in feed_bodies.items():
                if rest == f"{k}/latestid":
                    return 200, {"content-type": "application/octet-stream"}, "110"
                if rest == f"{k}/110.json":
                    return 200, JSON, json.dumps(body)
            return DENIED
        if u.startswith(f"{SITE}/game_detail/"):
            return 200, {"content-type": "text/html; charset=utf-8"}, embed or "<html></html>"
        return 404, {}, b""

    return handler


def test_v2_game_is_served_and_stamped(site):
    adapter = site(_s3({"v2_genius_contexts/504734": _full("v2_504734")}))
    out = api.bleague_game_info(schedule_key=504734, season=2026, return_parsed=False)
    assert out["_request"] == {"schedule_key": 504734, "feed": "v2"}
    assert len(adapter.calls) == 2
    assert api.bleague_pbp(schedule_key=504734).height == 809


def test_feed_fallback_v2_miss_goes_to_v1_then_to_the_embed(site):
    a = site(_s3({"v1_contexts/10693": _full("v1_10693")}))
    assert R._get(f"{S3}/contexts/10693")["_request"]["feed"] == "v1"
    assert [c.url.split("/")[4] for c in a.calls[:2]] == ["v2_genius_contexts", "v1_contexts"]
    site(_s3({}, embed=_html("game_detail_1.html")))
    out = R._get(f"{S3}/contexts/1")
    assert out["_request"] == {"schedule_key": 1, "feed": "v1"} and P.parse_bleague_pbp(out).height > 0


def test_a_key_nobody_holds_is_no_data(site):
    site(_s3({}))
    with pytest.raises(NoDataError):
        api.bleague_pbp(schedule_key=999999)


def test_season_guard_wrong_game_unfinished_and_shape_refusals(site):
    site(_s3({"v2_genius_contexts/504734": _full("v2_504734")}))
    with pytest.raises(AssetFetchError, match="Year"):
        api.bleague_pbp(schedule_key=504734, season=2025)

    wrong = _full("v2_504734")
    wrong["Game"]["ScheduleKey"] = 1
    site(_s3({"v2_genius_contexts/504734": wrong}))
    with pytest.raises(AssetFetchError, match="out of scope"):
        api.bleague_pbp(schedule_key=504734)

    row = _full("v2_504734")
    row["PlayByPlays"][5]["ScheduleKey"] = 7
    site(_s3({"v2_genius_contexts/504734": row}))
    with pytest.raises(AssetFetchError, match="PlayByPlays row of game 7"):
        api.bleague_pbp(schedule_key=504734)

    team = _full("v2_504734")
    team["HomeBoxscores"][0]["TeamID"] = "1"
    site(_s3({"v2_genius_contexts/504734": team}))
    with pytest.raises(AssetFetchError, match="row of team"):
        api.bleague_player_box(schedule_key=504734)

    live = _full("v2_504734")
    live["Game"]["GameEndedFlg"] = False
    site(_s3({"v2_genius_contexts/504734": live}))
    with pytest.raises(AssetFetchError, match="not final"):
        api.bleague_pbp(schedule_key=504734)

    drift = _full("v2_504734")
    drift["Leaders"] = []
    site(_s3({"v2_genius_contexts/504734": drift}))
    with pytest.raises(AssetFetchError, match="not a v2 context"):
        api.bleague_pbp(schedule_key=504734)

    site(_s3({"v2_genius_contexts/504734": {"error": "boom", "message": "x"}}))
    with pytest.raises(AssetFetchError):
        api.bleague_pbp(schedule_key=504734)


def test_embed_of_another_game_and_bad_latestid_are_refused(site):
    site(_s3({}, embed=_html("game_detail_1.html")))
    with pytest.raises(AssetFetchError, match="out of scope"):
        R._get(f"{S3}/contexts/2")

    def handler(req):
        return 200, {}, "abc" if req.url.endswith("latestid") else b""

    site(handler)
    with pytest.raises(AssetFetchError, match="not an integer"):
        R._get(f"{S3}/contexts/5")


def test_non_200_statuses_use_the_error_vocabulary(site, waits):
    site(lambda r: (404, {}, b""))
    with pytest.raises(NoDataError):
        R._get(f"{S3}/contexts/5")
    site(lambda r: (400, {}, b""))
    with pytest.raises(ValueError, match="400"):
        R._get(f"{S3}/contexts/5")
    site(lambda r: (403, {}, "blocked"))  # a 403 that is not S3's AccessDenied is a failure, not a miss
    with pytest.raises(AssetFetchError, match="403"):
        R._get(f"{S3}/contexts/5")
    a = site(lambda r: (500, {}, b""))
    before = len(waits.urls)
    with pytest.raises(AssetFetchError):
        R._get(f"{S3}/contexts/5")
    assert len(a.calls) == 3 and len(waits.urls) - before == 3  # three attempts, each throttled


def test_retries_wait_their_turn_and_honour_retry_after(site, waits):
    n = {"n": 0}
    base = _s3({"v2_genius_contexts/504734": _full("v2_504734")})

    def flaky(req):
        if req.url.endswith("110.json"):
            n["n"] += 1
            if n["n"] == 1:
                return 429, {"Retry-After": "7"}, b""
            if n["n"] == 2:
                return 503, {}, b""
        return base(req)

    a = site(flaky)
    assert R._get(f"{S3}/contexts/504734")["Game"]["ScheduleKey"] == 504734
    assert len(a.calls) == 4 and len(waits.urls) == 4  # latestid + three attempts
    assert max(waits.slept) >= 7.0


def test_arguments_are_validated_before_any_request(site):
    a = site(lambda r: (200, {}, b""))
    for bad in ("x", 1.5, True, None):
        with pytest.raises(ValueError):
            R._get(f"{S3}/contexts/{bad}")
    with pytest.raises(ValueError):
        R._get(f"{SITE}/schedule/", {"season": 2026, "tab": 9, "month": 10, "day": 4})
    with pytest.raises(ValueError):
        R._get("https://example.com/x")
    with pytest.raises(TypeError):
        R._get(f"{S3}/contexts/1", headers={})
    assert a.calls == []


def test_pii_keys_are_redacted_by_name():
    body = {
        "Game": {"RefereeEmail": "a@b.c", "RefereeBirthDate": "1980", "Attendance": 1},
        "rows": [{"Phone": "1", "PlayerBirth": "x"}],
    }
    out = R._redact(body)
    assert out["Game"]["RefereeEmail"] == "<redacted>" and out["Game"]["RefereeBirthDate"] == "<redacted>"
    assert (
        out["rows"][0]["Phone"] == "<redacted>" and out["rows"][0]["PlayerBirth"] == "x"
    )  # a player's public bio stays


def _page(url_prefix: str, html: str):
    return lambda req: (200, {"content-type": "text/html"}, html) if req.url.startswith(url_prefix) else (404, {}, b"")


def test_schedule_season_mapping_and_scope_refusals(site):
    a = site(_page(f"{SITE}/schedule/", _html("schedule_2025_1004.html.gz")))
    rec = R._get(f"{SITE}/schedule/", {"season": 2026, "tab": 1, "month": 10, "day": 4})
    assert (
        "year=2025" in a.calls[0].url and "event=2" in a.calls[0].url and "mon=10" in a.calls[0].url
    )  # END 2026 -> 2025
    assert api.bleague_schedule(season=2026, tab=1, month=10, day=4).height == 12
    assert rec["year"] == 2025
    with pytest.raises(AssetFetchError, match="out of scope"):
        R._get(f"{SITE}/schedule/", {"season": 2026, "tab": 1, "month": 10, "day": 5})
    with pytest.raises(AssetFetchError, match="out of scope"):
        R._get(f"{SITE}/schedule/", {"season": 2025, "tab": 1, "month": 10, "day": 4})  # page is season 2025-26
    with pytest.raises(AssetFetchError, match="out of scope"):
        R._get(f"{SITE}/schedule/", {"season": 2026, "tab": 2, "month": 10, "day": 4})


def test_schedule_refuses_a_paged_list_and_an_empty_day_is_no_data(site):
    html = _html("schedule_2025_1004.html.gz")
    paged = html.replace("</body>", '<a href="javascript:schedule_b1_more()">more</a></body>', 1)
    site(_page(f"{SITE}/schedule/", paged))
    with pytest.raises(AssetFetchError, match="paged"):
        R._get(f"{SITE}/schedule/", {"season": 2026, "tab": 1, "month": 10, "day": 4})
    emptied = re.sub(r'<li[^>]*class="[^"]*list-item[^"]*"[^>]*id="\d+".*?</li>', "", html, flags=re.S)
    site(_page(f"{SITE}/schedule/", emptied))
    with pytest.raises((NoDataError, AssetFetchError)):
        R._get(f"{SITE}/schedule/", {"season": 2026, "tab": 1, "month": 10, "day": 4})


def test_standings_mapping_and_refusals(site):
    a = site(_page(f"{SITE}/standings/", _html("standings_2025_b1.html.gz")))
    assert api.bleague_standings(season=2026, tab=1).height > 10
    assert "year=2025" in a.calls[0].url
    with pytest.raises(AssetFetchError, match="out of scope"):
        R._get(f"{SITE}/standings/", {"season": 2025, "tab": 1})
    with pytest.raises(AssetFetchError, match="out of scope"):
        R._get(f"{SITE}/standings/", {"season": 2026, "tab": 3})
    site(_page(f"{SITE}/standings/", "<html><body>stub</body></html>"))
    with pytest.raises(AssetFetchError):
        R._get(f"{SITE}/standings/", {"season": 2026, "tab": 1})
    site(lambda r: (200, {"content-type": "application/json"}, "{}"))
    with pytest.raises(AssetFetchError, match="not HTML"):
        R._get(f"{SITE}/standings/", {"season": 2026, "tab": 1})
