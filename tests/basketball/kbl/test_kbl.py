"""KBL: parsers over committed fixtures, the runtime's season mapping, politeness and refusals (mocked transport)."""

from __future__ import annotations

import copy
import re
import json
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import pytest
import requests
import yaml

from sportsdataverse.basketball import basketball_runtime as bb
from sportsdataverse.basketball.kbl import kbl_codes as codes
from sportsdataverse.basketball.kbl import kbl_parsers as P
from sportsdataverse.basketball.kbl import kbl_runtime as rt
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).resolve().parents[3]
FIX = ROOT / "tests" / "fixtures" / "kbl"
API = "https://api.kbl.or.kr"
GM = "S47G01N49"


def _load(rel: str) -> Any:
    return json.loads((FIX / rel).read_text(encoding="utf-8"))


def _wrap(body: Any, **req: Any) -> Dict[str, Any]:
    out = dict(body) if isinstance(body, dict) else {"items": body}
    out["_request"] = req
    return out


# --- parsers --------------------------------------------------------------------------------------------------------

_G = {"gmkey": GM}
CASES = [
    ("parse_kbl_game", "full/match__gmkey__S47G01N49.json", _G, 1),
    ("parse_kbl_period_scores", "full/match__gmkey__S47G01N49.json", _G, 8),
    ("parse_kbl_pbp", "full/match__gmkey__text-cast__S47G01N49.json", _G, 603),
    ("parse_kbl_player_box", "full/match__gmkey__player-stat__S47G01N49.json", _G, 24),
    ("parse_kbl_team_box", "full/match__gmkey__team-record__S47G01N49.json", _G, 2),
    ("parse_kbl_shots", "full/match__gmkey__match-chart__S47G01N49.json", _G, 127),
    ("parse_kbl_score_track", "full/match__gmkey__match-chart__S47G01N49.json", _G, 32),
    ("parse_kbl_game_leaders", "full/match__gmkey__match-chart__S47G01N49.json", _G, 10),
    ("parse_kbl_player_totals", "full/leagues__glkey__stats__players__S45G01.json", {"season_code": 45}, 204),
    ("parse_kbl_standings", "full/league__rank__glkey__S45G01.json", {"season_code": 45}, 10),
    ("parse_kbl_team_tracks", "full/league__rank__glkey__team-tracks__S45G01.json", {"season_code": 45}, None),
    ("parse_kbl_rounds", "full/league__rank__glkey__team-tracks__S45G01.json", {"season_code": 45}, None),
    ("parse_kbl_playoff_seeds", "full/league__playoff__new__glkey__S45G01.json", {}, 6),
    ("parse_kbl_playoff_series", "full/league__playoff__new__glkey__S45G01.json", {}, 10),
    ("parse_kbl_playoff_games", "full/league__playoff__new__glkey__S45G01.json", {}, 22),
    ("parse_kbl_players", "full/players.json", {}, 600),
    ("parse_kbl_seasons", "captures/season__list.json", {}, None),
    ("parse_kbl_teams", "captures/common__teamList.json", {}, None),
    ("parse_kbl_schedule", "captures/match__list__20251101.json", {}, None),
    ("parse_kbl_round_mvps", "captures/league__round-mvp__seasonCode__45.json", {}, None),
    ("parse_kbl_current_standings", "captures/league__rank__team.json", {}, None),
    ("parse_kbl_season_leaders", "captures/league__season-leader.json", {}, None),
]


@pytest.mark.parametrize("name,rel,req,rows", CASES)
def test_parser_over_fixture(name, rel, req, rows):
    df = getattr(P, name)(_wrap(_load(rel), **req))
    assert df.height > 0 and (rows is None or df.height == rows)
    assert dict(df.schema) == {c: d for c, _w, d in P.SPECS[name]}
    assert all(c == c.lower() and " " not in c for c in df.columns)
    for c in df.columns:
        if c in ("pcode", "team_code", "gmkey", "glkey", "game_code"):
            assert df.schema[c] == pl.String
            assert not df[c].drop_nulls().str.contains(r"\.0$").any()
    pdf = getattr(P, name)(_wrap(_load(rel), **req), return_as_pandas=True)
    assert len(pdf) == df.height


@pytest.mark.parametrize("name", sorted(P.SPECS))
@pytest.mark.parametrize("bad", [None, {}, [], "x", 3, {"items": []}])
def test_empty_or_malformed_is_a_typed_zero_row_frame_that_stacks(name, bad):
    df = getattr(P, name)(bad)
    assert df.height == 0 and dict(df.schema) == {c: d for c, _w, d in P.SPECS[name]}
    assert pl.concat([df, df]).height == 0


def test_numeric_dtypes_come_from_the_full_body():
    box = P.parse_kbl_player_box(_wrap(_load("full/match__gmkey__player-stat__S47G01N49.json"), **_G))
    assert box.schema["per"] == pl.Float64 and box.schema["score"] == pl.Int64 and box.schema["pie"] == pl.Float64
    assert box["score"].sum() == 138  # 74 + 64
    g = P.parse_kbl_game(_wrap(_load("full/match__gmkey__S47G01N49.json"), **_G)).row(0, named=True)
    assert (g["home_score"], g["away_score"], g["overtimes"], g["season"]) == (74, 64, 0, 2026)
    assert g["game_date"].isoformat() == "2025-11-01"


def test_overtime_labels_are_normalised_and_pbp_has_no_pcode():
    pbp = P.parse_kbl_pbp(_wrap([{"n": 1, "m": 4, "s": 5, "a": "205", "t": "50", "q": "X1", "p": "A", "e": "B"}], **_G))
    row = pbp.row(0, named=True)
    assert (row["period"], row["period_label"], row["points"], row["clock_seconds_left"]) == (5, "OT1", 3, 245)
    assert "pcode" not in pbp.columns
    assert codes.period_of("EQ2") == (6, "OT2") and codes.period_of("eq1") == (5, "OT1")


def test_pbp_game_ids_come_from_the_gmkey():
    pbp = P.parse_kbl_pbp(_wrap(_load("full/match__gmkey__text-cast__S47G01N49.json"), **_G))
    assert set(pbp["season_code"]) == {47} and set(pbp["game_no"]) == {49} and set(pbp["gmkey"]) == {GM}


def test_old_playoff_format_and_standings_renames():
    old = P.parse_kbl_playoff_series(_wrap(_load("captures/league__playoff__glkey__S43G01.json"), season_code=43))
    assert set(old["bracket_format"]) == {"old"} and old.height > 0
    st = P.parse_kbl_standings(_wrap(_load("full/league__rank__glkey__S45G01.json"), season_code=45))
    assert {"ast", "defr", "offr", "to", "team_r", "pscore"} <= set(st.columns)


def test_new_bracket_series_wins_match_games():
    s = P.parse_kbl_playoff_series(_wrap(_load("full/league__playoff__new__glkey__S45G01.json")))
    final = s.filter(pl.col("round") == "final").sort("wins")
    assert final["wins"].to_list() == [3, 4] and final["team_code"].to_list() == ["55", "50"]


def test_shots_before_2006_are_empty_not_an_error():
    assert (
        P.parse_kbl_shots(_wrap(_load("captures/match__gmkey__match-chart__S2G01N1.json"), gmkey="S2G01N1")).height == 0
    )


def test_docs_say_an_early_shot_chart_is_an_empty_frame_not_nodata():
    from sportsdataverse.basketball.kbl import kbl

    texts = [(ROOT / "tools/codegen/endpoints/kbl.yaml").read_text(encoding="utf-8"), kbl.kbl_shots.__doc__]
    texts += [rt.__doc__, rt._get.__doc__]
    for text in texts:
        flat = " ".join(str(text).split())
        assert "empty: NoDataError" not in flat and "box score or shot chart" not in flat
    assert "shot chart is empty: a zero-row frame" in " ".join(str(kbl.kbl_shots.__doc__).split())
    assert "(none before 2006-07) is not: it parses to zero rows" in " ".join(str(rt.__doc__).split())


# --- crosswalk and registry -----------------------------------------------------------------------------------------


def test_season_table_is_the_committed_readme_crosswalk():
    assert [list(r) for r in codes.SEASONS] == _load("season_crosswalk.json")


@pytest.mark.parametrize(
    "season,div,code",
    [(2026, "kbl", 47), (2026, "d_league", 48), (1997, "kbl", 1), (2027, "kbl", 49), (2010, "d_league", 16)],
)
def test_end_year_maps_to_season_code(season, div, code):
    assert codes.season_code(season, div) == code and codes.season_end(code) == season


@pytest.mark.parametrize(
    "season,div", [(1996, "kbl"), (2028, "kbl"), (2008, "d_league"), ("x", "kbl"), (True, "kbl"), (2026, "wkbl")]
)
def test_bad_season_is_a_value_error(season, div):
    with pytest.raises(ValueError):
        codes.season_code(season, div)


def test_yaml_lists_every_route_and_wrapper():
    doc = yaml.safe_load((ROOT / "tools/codegen/endpoints/kbl.yaml").read_text(encoding="utf-8"))
    spec = _load("spec_paths.json")
    assert len(doc["endpoints"]) == 22 == len(spec["wrappers"])
    wired = {re.sub(r"\{[^}]+\}", "{}", r) for r in spec["wrappers"].values()} | {"/league/playoff/new/{}"}
    assert wired == {re.sub(r"\{[^}]+\}", "{}", r) for r in spec["paths"]}
    for e in doc["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"]))


def test_descriptions_snippet_is_complete():
    snip = yaml.safe_load((ROOT / "tools/codegen/notes/kbl_descriptions.yaml").read_text(encoding="utf-8"))
    assert len(snip) == 22
    for key, cols in snip.items():
        assert cols and all(len(d) >= 15 for d in cols.values()), key


# --- runtime --------------------------------------------------------------------------------------------------------


def _resp(status=200, body=None, headers=None):
    r = requests.Response()
    r.status_code = status
    r._content = json.dumps(body).encode()
    r.headers.update(headers or {"Content-Type": "application/json"})
    r.url = API
    return r


class Router:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def __call__(self, method, url, *a, **kw):
        route = url.replace(API, "")
        self.calls.append(
            {"route": route, "params": dict(kw.get("params") or {}), "headers": dict(kw.get("headers") or {})}
        )
        ans = self.routes[route]
        if isinstance(ans, list):
            ans = ans.pop(0) if len(ans) > 1 else ans[0]
        if isinstance(ans, Exception):
            raise ans
        return ans


@pytest.fixture
def transport(monkeypatch):
    def install(routes):
        t = Router(routes)
        monkeypatch.setattr(requests.Session, "request", t)
        return t

    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", lambda s: None)
    rt.clear_cache()
    yield install
    rt.clear_cache()


def _game_routes(**over):
    routes = {
        f"/match/{GM}": _resp(body=_load("full/match__gmkey__S47G01N49.json")),
        f"/match/{GM}/text-cast": _resp(body=_load("full/match__gmkey__text-cast__S47G01N49.json")),
        f"/match/{GM}/player-stat": _resp(body=_load("full/match__gmkey__player-stat__S47G01N49.json")),
        f"/match/{GM}/team-record": _resp(body=_load("full/match__gmkey__team-record__S47G01N49.json")),
        f"/match/{GM}/match-chart": _resp(body=_load("full/match__gmkey__match-chart__S47G01N49.json")),
    }
    routes.update(over)
    return routes


def test_headers_are_always_sent_and_game_is_stamped(transport):
    t = transport(_game_routes())
    out = rt._get(f"{API}/match/{GM}/player-stat")
    assert out["_request"] == {"gmkey": GM} and len(out["items"]) == 24
    assert all(c["headers"].get("Channel") == "WEB" and "TeamCode" in c["headers"] for c in t.calls)


def test_text_cast_always_asks_for_all_quarters(transport):
    t = transport(_game_routes())
    rt._get(f"{API}/match/{GM}/text-cast")
    assert [c["params"] for c in t.calls if c["route"].endswith("text-cast")] == [{"quarterList": rt.QUARTERS}]


def test_malformed_gmkey_is_a_value_error_before_any_request(transport):
    t = transport({})
    with pytest.raises(ValueError, match="gmkey"):
        rt._get(f"{API}/match/S47G01/player-stat")
    assert t.calls == []


def test_unfinished_game_is_refused(transport, monkeypatch):
    hdr = _load("full/match__gmkey__S47G01N49.json")
    hdr["game"]["isEnded"] = 0
    monkeypatch.setattr(rt, "_today_kst", lambda: "20251101")  # the game's own date: not before today
    transport(_game_routes(**{f"/match/{GM}": _resp(body=hdr)}))
    with pytest.raises(AssetFetchError, match="not final"):
        rt._get(f"{API}/match/{GM}/player-stat")


def test_isended_zero_but_past_with_game_end_row_is_final(transport):
    hdr = _load("full/match__gmkey__S47G01N49.json")
    hdr["game"]["isEnded"] = 0
    transport(_game_routes(**{f"/match/{GM}": _resp(body=hdr)}))
    assert rt._get(f"{API}/match/{GM}/team-record")["items"]


def test_past_game_without_end_row_is_refused(transport):
    hdr = _load("full/match__gmkey__S47G01N49.json")
    hdr["game"]["isEnded"] = 0
    cast = [
        r for r in _load("full/match__gmkey__text-cast__S47G01N49.json") if not (r["a"] == "009" and r["q"] == "Q4")
    ]
    transport(_game_routes(**{f"/match/{GM}": _resp(body=hdr), f"/match/{GM}/text-cast": _resp(body=cast)}))
    with pytest.raises(AssetFetchError, match="game-end"):
        rt._get(f"{API}/match/{GM}/text-cast")


def test_truncated_text_cast_is_refused(transport):
    cast = [r for r in _load("full/match__gmkey__text-cast__S47G01N49.json") if r["q"] == "Q4"]
    transport(_game_routes(**{f"/match/{GM}/text-cast": _resp(body=cast)}))
    with pytest.raises(AssetFetchError, match="periods"):
        rt._get(f"{API}/match/{GM}/text-cast")


def test_wrong_game_header_is_refused(transport):
    hdr = _load("full/match__gmkey__S47G01N49.json")
    hdr["game"]["gmkey"] = "S47G01N50"
    transport(_game_routes(**{f"/match/{GM}": _resp(body=hdr)}))
    with pytest.raises(AssetFetchError, match="gmkey"):
        rt._get(f"{API}/match/{GM}/player-stat")


def test_box_points_must_match_the_header(transport):
    box = copy.deepcopy(_load("full/match__gmkey__player-stat__S47G01N49.json"))
    box[0]["records"]["score"] += 1
    transport(_game_routes(**{f"/match/{GM}/player-stat": _resp(body=box)}))
    with pytest.raises(AssetFetchError, match="points"):
        rt._get(f"{API}/match/{GM}/player-stat")


def test_unknown_game_empty_box_is_refused_by_the_header_guard_and_empty_box_is_nodata(transport):
    transport(_game_routes(**{f"/match/{GM}/player-stat": _resp(body=[])}))
    with pytest.raises(NoDataError):
        rt._get(f"{API}/match/{GM}/player-stat")


def test_shot_chart_of_another_game_is_refused(transport):
    chart = copy.deepcopy(_load("full/match__gmkey__match-chart__S47G01N49.json"))
    chart["shootLog"][0]["tcode"] = "99"
    transport(_game_routes(**{f"/match/{GM}/match-chart": _resp(body=chart)}))
    with pytest.raises(AssetFetchError, match="shot team"):
        rt._get(f"{API}/match/{GM}/match-chart")


ENVELOPE = {"resultCode": "Fail", "message": "x", "object": -1, "errorCode": 500}


@pytest.mark.parametrize("status", [200, 400, 500])
def test_error_envelope_and_error_statuses_are_asset_fetch_errors(transport, status):
    transport({"/season/list": _resp(status, ENVELOPE)})
    with pytest.raises(AssetFetchError):
        rt._get(f"{API}/season/list")


def test_a_500_is_not_retried(transport):
    t = transport({"/season/list": _resp(500, ENVELOPE)})
    with pytest.raises(AssetFetchError):
        rt._get(f"{API}/season/list")
    assert len(t.calls) == 1


def test_every_attempt_waits_its_turn_retries_included(monkeypatch):
    waits: List[str] = []
    monkeypatch.setattr(bb, "_polite_wait", lambda url, *a, **k: waits.append(url))
    monkeypatch.setattr("time.sleep", lambda s: None)
    answers = [_resp(503, {}), _resp(200, [{"seasonCode": 47}])]
    monkeypatch.setattr(requests.Session, "request", lambda self, m, u, *a, **k: answers.pop(0))
    assert rt._get(f"{API}/season/list")["items"] == [{"seasonCode": 47}]
    assert len(waits) == 2


def test_retry_after_is_honoured(monkeypatch):
    slept: List[float] = []
    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", slept.append)
    answers = [_resp(429, {}, {"Retry-After": "7"}), _resp(200, [{"a": 1}])]
    monkeypatch.setattr(requests.Session, "request", lambda self, m, u, *a, **k: answers.pop(0))
    rt._get(f"{API}/season/list")
    assert any(s >= 7 for s in slept)


def test_season_route_is_rewritten_to_the_glkey(transport):
    t = transport({"/league/rank/S47G01": _resp(body=_load("full/league__rank__glkey__S45G01.json"))})
    with pytest.raises(AssetFetchError, match="asked for S47G01"):  # the S45 body is another season
        rt._get(f"{API}/league/rank/2026", {})
    assert t.calls[0]["route"] == "/league/rank/S47G01" and t.calls[0]["params"] == {}


def test_division_picks_the_d_league_key(transport):
    t = transport(
        {"/leagues/S48G01/stats/players": _resp(body=_load("full/leagues__glkey__stats__players__S45G01.json"))}
    )
    out = rt._get(f"{API}/leagues/2026/stats/players", {"division": "d_league"})
    assert t.calls[0]["params"] == {} and out["_request"]["glkey"] == "S48G01" and out["_request"]["season"] == 2026


def test_playoff_uses_old_route_below_45_and_new_from_45(transport):
    t = transport({"/league/playoff/S43G01": _resp(body=_load("captures/league__playoff__glkey__S43G01.json"))})
    assert rt._get(f"{API}/league/playoff/2024")["_request"]["format"] == "old"
    assert t.calls[0]["route"] == "/league/playoff/S43G01"
    t2 = transport({"/league/playoff/new/S45G01": _resp(body=_load("full/league__playoff__new__glkey__S45G01.json"))})
    assert rt._get(f"{API}/league/playoff/2025")["_request"]["format"] == "new"
    assert t2.calls[0]["route"] == "/league/playoff/new/S45G01"


def test_playoff_of_another_season_is_refused(transport):
    transport({"/league/playoff/S43G01": _resp(body=_load("full/league__playoff__new__glkey__S45G01.json"))})
    with pytest.raises(AssetFetchError, match="seasonCode"):
        rt._get(f"{API}/league/playoff/2024")


def test_team_tracks_outside_the_season_is_refused(transport):
    transport(
        {"/league/rank/S43G01/team-tracks": _resp(body=_load("full/league__rank__glkey__team-tracks__S45G01.json"))}
    )
    with pytest.raises(AssetFetchError, match="outside season"):
        rt._get(f"{API}/league/rank/2024/team-tracks")


def test_schedule_window_is_split_by_calendar_year_and_checked(transport):
    rows = _load("captures/match__list__20251101.json")
    t = transport({"/match/list": _resp(body=rows)})
    with pytest.raises(AssetFetchError, match="outside"):  # the rows are dated 2025-11, not the asked window
        rt._get(f"{API}/match/list", {"fromDate": "20251201", "toDate": "20251202"})
    assert t.calls[0]["params"] == {"fromDate": "20251201", "toDate": "20251202"}
    assert codes.season_dates(2026) == ("20250920", "20260513")
    assert rt._windows(2026, None, None) == [("20250920", "20251231"), ("20260101", "20260513")]


def test_schedule_arguments_are_validated_before_any_request(transport):
    t = transport({})
    for params in ({}, {"fromDate": "2025"}, {"season": 2026, "fromDate": "20251101", "toDate": "20251102"}):
        with pytest.raises(ValueError):
            rt._get(f"{API}/match/list", params)
    assert t.calls == []
