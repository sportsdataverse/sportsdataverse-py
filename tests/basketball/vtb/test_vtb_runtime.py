"""The VTB runtimes: season mapping, politeness per attempt, Retry-After, error vocabulary, scope and finality checks.

The transport is mocked and the bodies are the committed fixtures (``tests/fixtures/vtb_*``); nothing reads the
sdv-internal-refs checkout.
"""

from __future__ import annotations

import copy
import json
from typing import Any, Callable, Dict, List, Optional

import pytest
import requests

from sportsdataverse.basketball import basketball_runtime as bb
from sportsdataverse.basketball.vtb import vtb_infobasket_runtime as ib
from sportsdataverse.basketball.vtb import vtb_league_runtime as lg
from sportsdataverse.errors import AssetFetchError, NoDataError
from tests.basketball.vtb._fx import raw

IB = "https://org.infobasket.su"
V2 = "https://api.vtb-league.com"


def _response(status: int = 200, body: Any = None, *, text: Optional[str] = None, headers: Optional[dict] = None):
    resp = requests.Response()
    resp.status_code = status
    resp._content = (text if text is not None else json.dumps(body)).encode()
    resp.headers.update(headers or {"Content-Type": "application/json"})
    resp.url = IB
    return resp


class Router:
    """Replaces ``requests.Session.request``: answers by (path), records every attempt (path + query)."""

    def __init__(self, routes: Dict[str, Any]):
        self.routes = routes
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, method, url, *args, **kwargs):
        path = url.split("//", 1)[1].split("/", 1)[1]
        key = "/" + path
        self.calls.append({"route": key, "params": dict(kwargs.get("params") or {})})
        answer = self.routes[key]
        if isinstance(answer, list):
            answer = answer.pop(0) if len(answer) > 1 else answer[0]
        if isinstance(answer, Exception):
            raise answer
        return answer


@pytest.fixture
def transport(monkeypatch):
    """Install a router under the real session; no polite wait, no retry sleep. Season-table cache starts empty."""

    def install(routes: Dict[str, Any]) -> Router:
        t = Router(routes)
        monkeypatch.setattr(requests.Session, "request", t)
        return t

    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", lambda s: None)
    ib.clear_cache()
    yield install
    ib.clear_cache()


SEASONS = _response(body=raw("vtb_infobasket", "captures/Comp__GetSeasonsForTag__vtb.json"))
TABLE = [
    {"SeasonYear": y, "NameEn": f"Season {y - 1}/{y}", "CompID": c, "Db": "org"} for y, c in ib.SEASON_COMP_IDS.items()
]


def _seasons() -> requests.Response:
    return _response(body=TABLE)


def _online(mutate: Optional[Callable[[Any], None]] = None) -> requests.Response:
    data = copy.deepcopy(raw("vtb_infobasket", "full/online_921391.json.gz"))
    if mutate:
        mutate(data)
    return _response(body=data)


# --- season mapping -----------------------------------------------------------------------------------------------------


def test_the_season_table_in_the_readme_is_what_the_live_list_maps():
    cap = raw("vtb_infobasket", "captures/Comp__GetSeasonsForTag__vtb.json")
    assert {r["SeasonYear"]: r["CompID"] for r in cap} == {k: ib.SEASON_COMP_IDS[k] for k in (2010, 2011, 2012)}
    assert ib.SEASON_COMP_IDS[2026] == 50714 and ib.SEASON_COMP_IDS[2023] == 39799 and ib.SEASON_COMP_IDS[2014] == 9113
    assert len(ib.SEASON_COMP_IDS) == 18 and max(ib.SEASON_COMP_IDS) == 2027


@pytest.mark.parametrize("end,comp", [(2010, 5052), (2014, 9113), (2023, 39799), (2026, 50714), (2027, 55613)])
def test_end_year_maps_to_the_season_comp_id(end, comp, transport):
    t = transport({"/Comp/GetSeasonsForTag": _seasons()})
    assert ib.season_comp_id(end) == comp and ib.season_comp_id(str(end)) == comp
    assert len(t.calls) == 1 and t.calls[0]["params"] == {"tag": "vtb", "format": "json"}  # cached after the first


@pytest.mark.parametrize("bad", [2009, 2028, 50714, "x", True, None])
def test_a_season_that_is_not_a_known_end_year_is_a_value_error(bad, transport):
    transport({"/Comp/GetSeasonsForTag": _seasons()})
    with pytest.raises(ValueError, match="season"):
        ib.season_comp_id(bad)


def test_the_calendar_sends_comps_not_season_and_stamps_both(transport):
    cal = raw("vtb_infobasket", "captures/Comp__GetCalendar__2026.json")
    t = transport({"/Comp/GetSeasonsForTag": _seasons(), "/Comp/GetCalendar/": _response(body=cal)})
    out = ib._get(f"{IB}/Comp/GetCalendar/", {"season": 2026, "tag": "vtb", "format": "json"})
    assert t.calls[1]["params"] == {
        "comps": 50714,
        "format": "json",
    }  # season became comps; tag is not a wire param here
    assert out["items"] == cal and out["_request"]["season"] == 2026 and out["_request"]["season_comp_id"] == 50714


def test_a_path_season_token_is_mapped_into_the_wire_path(transport):
    body = {"Count": 1, "Page": 0, "Pages": 1, "Stats": [{"TeamID": 1}]}
    t = transport({"/Comp/GetSeasonsForTag": _seasons(), "/Comp/GetTeamStats/50714": _response(body=body)})
    out = ib._get(f"{IB}/Comp/GetTeamStats/2026", {"page": 0, "tag": "vtb"})
    assert t.calls[1] == {"route": "/Comp/GetTeamStats/50714", "params": {"page": 0}}
    assert out["_request"] == {
        "season": 2026,
        "season_comp_id": 50714,
        "tag": "vtb",
        "route": "/Comp/GetTeamStats/{season}",
    }


def test_the_tag_is_a_wire_param_on_the_profile_routes_and_picks_the_season_table(transport):
    youth = [{"SeasonYear": 2027, "NameEn": "x", "CompID": 55655, "Db": "org"}]
    team = raw("vtb_infobasket", "captures/Widget__TeamRoster__teamId__2026.json")
    t = transport({"/Comp/GetSeasonsForTag": _response(body=youth), "/Widget/TeamRoster/2650": _response(body=team)})
    out = ib._get(f"{IB}/Widget/TeamRoster/2650", {"season": 2027, "tag": "vtbyouth", "format": "json"})
    assert t.calls[0]["params"]["tag"] == "vtbyouth"
    assert t.calls[1]["params"] == {"compId": 55655, "tag": "vtbyouth", "format": "json"}
    assert out["_request"]["tag"] == "vtbyouth" and out["_request"]["team_id"] == 2650


# --- politeness and transport errors -------------------------------------------------------------------------------------


def test_every_attempt_waits_its_turn_retries_included(monkeypatch):
    waits: List[str] = []
    monkeypatch.setattr(bb, "_polite_wait", lambda url, interval=bb.MIN_INTERVAL: waits.append(url))
    monkeypatch.setattr("time.sleep", lambda s: None)
    t = Router({"/v2/leagues": [_response(503, text="down"), _response(503, text="down"), _response(body=_env())]})
    monkeypatch.setattr(requests.Session, "request", t)
    out = lg._get(f"{V2}/v2/leagues")
    assert len(t.calls) == 3 and len(waits) == 3 and out["data"]


def test_a_dependency_request_waits_too(monkeypatch, transport):
    waits: List[str] = []
    transport(
        {
            "/Comp/GetSeasonsForTag": _seasons(),
            "/Comp/GetCalendar/": _response(body=raw("vtb_infobasket", "captures/Comp__GetCalendar__2026.json")),
        }
    )
    monkeypatch.setattr(bb, "_polite_wait", lambda url, interval=bb.MIN_INTERVAL: waits.append(url))
    ib._get(f"{IB}/Comp/GetCalendar/", {"season": 2026})
    assert len(waits) == 2 and waits[0].endswith("/Comp/GetSeasonsForTag")


def test_retry_after_is_honoured_on_429_and_503(monkeypatch):
    slept: List[float] = []
    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", lambda s: slept.append(s))
    for status in (429, 503):
        slept.clear()
        t = Router(
            {"/v2/leagues": [_response(status, text="slow", headers={"Retry-After": "7"}), _response(body=_env())]}
        )
        monkeypatch.setattr(requests.Session, "request", t)
        assert lg._get(f"{V2}/v2/leagues")["data"]
        assert 7 in slept or 7.0 in slept, (status, slept)


def test_the_session_spaces_two_requests_by_the_interval(monkeypatch):
    state = {"now": 100.0, "slept": []}
    monkeypatch.setattr(bb.time, "monotonic", lambda: state["now"])
    monkeypatch.setattr(
        bb.time, "sleep", lambda s: (state["slept"].append(s), state.__setitem__("now", state["now"] + s))
    )
    bb._last.clear()
    bb._polite_wait(f"{V2}/a")
    bb._polite_wait(f"{V2}/b")
    assert len(state["slept"]) == 1 and abs(state["slept"][0] - bb.MIN_INTERVAL) < 1e-9
    bb._last.clear()


def test_404_is_no_data_and_5xx_non_json_and_empty_are_asset_fetch_errors(transport):
    transport(
        {
            "/v2/leagues/nosuch/seasons": _response(
                404, text="<html>not found</html>", headers={"Content-Type": "text/html"}
            )
        }
    )
    with pytest.raises(NoDataError):
        lg._get(f"{V2}/v2/leagues/nosuch/seasons")
    for resp in (_response(500, text="boom"), _response(text="<html>"), _response(text="")):
        transport({"/v2/leagues": resp})
        with pytest.raises(AssetFetchError):
            lg._get(f"{V2}/v2/leagues")


def test_a_dead_connection_is_an_asset_fetch_error_that_never_leaks_the_query(monkeypatch):
    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", lambda s: None)
    monkeypatch.setattr(requests.Session, "request", Router({"/v2/leagues": requests.ConnectionError("down")}))
    with pytest.raises(AssetFetchError):
        lg._get(f"{V2}/v2/leagues")


def test_a_foreign_url_is_a_value_error():
    with pytest.raises(ValueError, match="not a VTB"):
        lg._get(f"{V2}/v2/other")
    with pytest.raises(ValueError, match="not a VTB"):
        ib._get(f"{IB}/Widget/Nope/1")


# --- the v2 league API ---------------------------------------------------------------------------------------------------


def _env(name: str = "captures/v2__leagues.json") -> Dict[str, Any]:
    return copy.deepcopy(raw("vtb_league", name))


def test_the_v2_envelope_is_returned_with_the_request_stamped(transport):
    t = transport({"/v2/leagues/vtb/seasons": _response(body=_env("captures/v2__leagues__league__seasons__vtb.json"))})
    out = lg._get(f"{V2}/v2/leagues/vtb/seasons")
    assert out["data"] and out["_request"] == {"league_slug": "vtb", "route": "/v2/leagues/{league_slug}/seasons"}
    assert t.calls[0]["params"] == {}


@pytest.mark.parametrize(
    "mutate",
    [
        lambda b: b["meta"].update(status="error"),
        lambda b: b["meta"].update(code=500),
        lambda b: b["meta"].update(count=99),
        lambda b: b["meta"].update(limit=None),
        lambda b: b["meta"].update(limit=3),  # count reaches the limit: possibly truncated
        lambda b: b.pop("meta"),
        lambda b: b.update(data="x"),
    ],
)
def test_an_envelope_that_is_not_a_complete_success_is_refused(mutate, transport):
    body = _env()
    mutate(body)
    transport({"/v2/leagues": _response(body=body)})
    with pytest.raises(AssetFetchError):
        lg._get(f"{V2}/v2/leagues")


def test_the_v2_matches_of_another_league_or_season_are_refused(transport):
    body = _env("full/matches_vtb_2026.json.gz")
    transport({"/v2/leagues/vtb/seasons/2026/matches": _response(body=body), "/v2/leagues/vtb/seasons/2025/matches": _response(body=body),
               "/v2/leagues/wbc/seasons/2026/matches": _response(body=body)})  # fmt: skip
    assert lg._get(f"{V2}/v2/leagues/vtb/seasons/2026/matches", {"limit": 1000})["_request"]["season"] == 2026
    with pytest.raises(AssetFetchError, match="season"):
        lg._get(f"{V2}/v2/leagues/vtb/seasons/2025/matches", {"limit": 1000})
    with pytest.raises(AssetFetchError, match="league"):
        lg._get(f"{V2}/v2/leagues/wbc/seasons/2026/matches", {"limit": 1000})


def test_a_v2_season_with_no_rows_is_no_data(transport):
    body = _env("full/matches_vtb_2026.json.gz")
    body["data"], body["meta"]["count"] = [], 0
    transport({"/v2/leagues/vtb/seasons/2024/matches": _response(body=body)})
    with pytest.raises(NoDataError):
        lg._get(f"{V2}/v2/leagues/vtb/seasons/2024/matches", {"limit": 1000})


# --- Infobasket error vocabulary and scope --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "answer",
    [
        {"MessageType": 2, "Header": "Error", "Message": "Not Found"},
        {"MessageType": 4, "Header": "Ошибка", "Message": "Турнир - не плейофф"},
        "BadRequest",
        7,
        None,
    ],
)
def test_an_error_envelope_a_bare_string_or_scalar_is_an_asset_fetch_error(answer, transport):
    transport({"/Widget/GetOnline/999": _response(body=answer)})
    with pytest.raises(AssetFetchError):
        ib._get(f"{IB}/Widget/GetOnline/999", {"format": "json"})


def test_an_empty_list_is_no_data_and_a_non_list_is_refused(transport):
    transport({"/Widget/CrossTable/50720": _response(body=[]), "/Widget/PlayOff/54281": _response(body={"a": 1})})
    with pytest.raises(NoDataError):
        ib._get(f"{IB}/Widget/CrossTable/50720", {"format": "json"})
    with pytest.raises(AssetFetchError):
        ib._get(f"{IB}/Widget/PlayOff/54281", {"format": "json"})


def test_a_game_body_for_another_game_is_refused(transport):
    transport({"/Widget/GetOnline/921391": _online(), "/Widget/GetOnline/5": _online()})
    assert ib._get(f"{IB}/Widget/GetOnline/921391", {"format": "json"})["Online"]["GameID"] == 921391
    with pytest.raises(AssetFetchError, match="another game"):
        ib._get(f"{IB}/Widget/GetOnline/5", {"format": "json"})


@pytest.mark.parametrize("status,online", [(0, False), (2, True), (1, True)])
def test_a_game_that_is_not_final_is_refused(status, online, transport):
    def mutate(d):
        d["GameStatus"], d["IsOnline"] = status, online

    transport({"/Widget/GetOnline/921391": _online(mutate)})
    with pytest.raises(AssetFetchError, match="not final"):
        ib._get(f"{IB}/Widget/GetOnline/921391", {"format": "json"})


def test_a_stub_game_without_two_teams_is_refused(transport):
    transport({"/Widget/GetOnline/921391": _online(lambda d: d.update(GameTeams=[]))})
    with pytest.raises(AssetFetchError, match="stub"):
        ib._get(f"{IB}/Widget/GetOnline/921391", {"format": "json"})


def test_the_game_page_needs_its_id_and_a_final_status(transport):
    page = raw("vtb_infobasket", "captures/Widget__GamePage__gameId__2026.json")
    live = {**page, "GameStatus": 2}
    transport(
        {
            "/Widget/GamePage/921391": [_response(body=page), _response(body=live)],
            "/Widget/GamePage/7": _response(body=page),
        }
    )
    assert ib._get(f"{IB}/Widget/GamePage/921391")["GameID"] == 921391
    with pytest.raises(AssetFetchError, match="not final"):
        ib._get(f"{IB}/Widget/GamePage/921391")
    with pytest.raises(AssetFetchError, match="asked for game 7"):
        ib._get(f"{IB}/Widget/GamePage/7")


@pytest.mark.parametrize(
    "route,cap,other",
    [
        ("/Widget/PlayerPage/423011", "Widget__PlayerPage__personId", "/Widget/PlayerPage/1"),
        ("/Widget/CoachPage/131047", "Widget__CoachPage__personId", "/Widget/CoachPage/1"),
        ("/Widget/TeamPage/2650", "Widget__TeamPage__teamId", "/Widget/TeamPage/1"),
        ("/Widget/TeamStats/2650", "Widget__TeamStats__teamId__2026", "/Widget/TeamStats/1"),
    ],
)
def test_a_person_or_team_body_must_echo_the_id_asked_for(route, cap, other, transport):
    body = raw("vtb_infobasket", f"captures/{cap}.json")
    transport({route: _response(body=body), other: _response(body=body)})
    assert ib._get(f"{IB}{route}", {"format": "json"})["_request"]
    with pytest.raises(AssetFetchError, match="asked for"):
        ib._get(f"{IB}{other}", {"format": "json"})


def test_a_calendar_of_another_season_is_refused(transport):
    cal = raw("vtb_infobasket", "captures/Comp__GetCalendar__2023.json")  # 2022-23 games
    transport({"/Comp/GetSeasonsForTag": _seasons(), "/Comp/GetCalendar/": _response(body=cal)})
    with pytest.raises(AssetFetchError, match="another season"):
        ib._get(f"{IB}/Comp/GetCalendar/", {"season": 2026})
    assert ib._get(f"{IB}/Comp/GetCalendar/", {"season": 2023})["items"]


def test_team_games_of_another_team_and_team_comps_of_another_season_are_refused(transport):
    games = raw("vtb_infobasket", "captures/Widget__TeamGames__teamId__2026.json")
    comps = raw("vtb_infobasket", "captures/Widget__GetTeamComps__teamId__2026.json")
    transport({"/Comp/GetSeasonsForTag": _seasons(), "/Widget/TeamGames/2650": _response(body=games),
               "/Widget/TeamGames/1": _response(body=games), "/Widget/GetTeamComps/2650": _response(body=comps)})  # fmt: skip
    assert ib._get(f"{IB}/Widget/TeamGames/2650", {"season": 2026})["items"]
    with pytest.raises(AssetFetchError, match="another team"):
        ib._get(f"{IB}/Widget/TeamGames/1", {"season": 2026})
    with pytest.raises(AssetFetchError, match="another season"):
        ib._get(f"{IB}/Widget/GetTeamComps/2650", {"season": 2023})


def test_a_player_game_log_must_be_for_the_requested_team(transport):
    log = raw("vtb_infobasket", "captures/Widget__PlayerStats__personId__2026.json")
    transport({"/Comp/GetSeasonsForTag": _seasons(), "/Widget/PlayerStats/423011": _response(body=log)})
    ok = ib._get(f"{IB}/Widget/PlayerStats/423011", {"season": 2026, "team": 2650, "filter": 1, "tag": "vtb"})
    assert ok["_request"]["team_id"] == 2650
    with pytest.raises(AssetFetchError, match="another team"):
        ib._get(f"{IB}/Widget/PlayerStats/423011", {"season": 2026, "team": 1390, "filter": 1, "tag": "vtb"})


def test_the_stage_tree_and_standings_must_be_the_ones_asked_for(transport):
    tree = raw("vtb_infobasket", "captures/Widget__CompIssue__compId__2026.json")
    st = raw("vtb_infobasket", "captures/Widget__CompTeamResults__compId__2026.json")
    transport({"/Comp/GetSeasonsForTag": _seasons(), "/Widget/CompIssue/50714": _response(body=tree),
               "/Widget/CompIssue/39799": _response(body=tree), "/Widget/CompTeamResults/50720": _response(body=st),
               "/Widget/CompTeamResults/1": _response(body=st)})  # fmt: skip
    assert ib._get(f"{IB}/Widget/CompIssue/2026")["_request"]["season_comp_id"] == 50714
    with pytest.raises(AssetFetchError, match="stage tree"):
        ib._get(f"{IB}/Widget/CompIssue/2023")
    assert ib._get(f"{IB}/Widget/CompTeamResults/50720")["_request"]["comp_id"] == 50720
    with pytest.raises(AssetFetchError, match="another stage"):
        ib._get(f"{IB}/Widget/CompTeamResults/1")


def test_a_partial_team_stats_page_is_refused_and_an_empty_one_is_no_data(transport):
    full = {"Count": 2, "Page": 0, "Pages": 1, "Stats": [{"TeamID": 1}, {"TeamID": 2}]}
    paged = {**full, "Pages": 3}
    short = {**full, "Stats": [{"TeamID": 1}]}
    empty = {"Count": 0, "Page": 0, "Pages": 1, "Stats": []}
    transport({"/Comp/GetSeasonsForTag": _seasons(), "/Comp/GetTeamStats/50714": [_response(body=full), _response(body=paged),
               _response(body=short), _response(body=empty)]})  # fmt: skip
    assert ib._get(f"{IB}/Comp/GetTeamStats/2026", {"page": 0})["Stats"]
    for _ in range(2):
        with pytest.raises(AssetFetchError, match="partial page"):
            ib._get(f"{IB}/Comp/GetTeamStats/2026", {"page": 0})
    with pytest.raises(NoDataError):
        ib._get(f"{IB}/Comp/GetTeamStats/2026", {"page": 0})


# --- personal data ------------------------------------------------------------------------------------------------------


def test_the_scrub_removes_staff_birth_data_licences_and_contacts_but_keeps_player_bio():
    dirty = {
        "OnlineStarts": [
            {"StartType": 1, "PersonBirth": "22.05.1992", "LicenceNumber": "L1", "PersonName1": "Miller"},
            {"StartType": 3, "PersonBirth": "26.03.1982", "PersonBirthday": "/Date(1)/", "Age": 43},
            {"StartType": 4, "PersonBirthYear": 1981, "PersonContacts": [{"x": 1}], "PosID": 101},
        ],
        "GameTeams": [{"Coach": {"PersonBirth": "1.1.1980", "Age": 40, "FoulC": 1}, "Players": [{"PersonBirth": "21.03.2004", "Age": 22}]}],
        "SysUser": "operator", "bet": {"odds": 1.5},
    }  # fmt: skip
    clean = ib._scrub(dirty)
    starts = clean["OnlineStarts"]
    assert starts[0] == {"StartType": 1, "PersonBirth": "22.05.1992", "PersonName1": "Miller"}
    assert starts[1] == {"StartType": 3} and starts[2] == {"StartType": 4, "PosID": 101}
    assert clean["GameTeams"][0]["Coach"] == {"FoulC": 1}
    assert clean["GameTeams"][0]["Players"][0] == {"PersonBirth": "21.03.2004", "Age": 22}
    assert "SysUser" not in clean and "bet" not in clean
    assert ib._scrub({"Age": 50, "PersonBirth": "x", "PersonID": 1}, whole_body_is_coach=True) == {"PersonID": 1}


def test_get_scrubs_the_raw_body_too(transport):
    def dirty(d):
        d["OnlineStarts"][0].update(StartType=4, PersonBirth="01.01.1980", LicenceNumber="L9")

    transport({"/Widget/GetOnline/921391": _online(dirty)})
    out = ib._get(f"{IB}/Widget/GetOnline/921391", {"format": "json"})
    assert "PersonBirth" not in out["OnlineStarts"][0] and "LicenceNumber" not in out["OnlineStarts"][0]
