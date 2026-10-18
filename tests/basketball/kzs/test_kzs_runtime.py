"""The KZS runtime: season mapping, politeness per attempt, Retry-After, the HTTP-400 vocabulary and every refusal.

The transport is mocked and the bodies are the committed fixtures (``tests/fixtures/kzs``); nothing reads the
sdv-internal-refs checkout.
"""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pytest
import requests

from sportsdataverse.basketball import basketball_runtime as bb
from sportsdataverse.basketball.kzs import kzs_runtime as rt
from sportsdataverse.errors import AssetFetchError, NoDataError

FIX = Path(__file__).resolve().parents[2] / "fixtures" / "kzs"
API = "https://api.kzs.si/api/v1/public"


def _env(name: str) -> Dict[str, Any]:
    """A fixture as the API sends it: the ``{status, data}`` envelope."""
    path = FIX / name
    text = gzip.open(path, "rt", encoding="utf-8").read() if name.endswith(".gz") else path.read_text(encoding="utf-8")
    return json.loads(text)


def _response(status: int = 200, body: Any = None, *, text: Optional[str] = None, headers: Optional[dict] = None):
    resp = requests.Response()
    resp.status_code = status
    resp._content = (text if text is not None else json.dumps(body)).encode()
    resp.headers.update(headers or {"Content-Type": "application/json"})
    resp.url = API
    return resp


def _ok(name: str, mutate: Optional[Callable[[Any], None]] = None) -> requests.Response:
    body = copy.deepcopy(_env(name))
    if mutate:
        mutate(body["data"])
    return _response(body=body)


class Router:
    """Replaces ``requests.Session.request``: answers by route (the path after ``/public``) and records every attempt."""

    def __init__(self, routes: Dict[str, Any]):
        self.routes = routes
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, method, url, *args, **kwargs):
        route = url.split("/api/v1/public", 1)[1]
        self.calls.append({"route": route, "params": dict(kwargs.get("params") or {})})
        answer = self.routes[route]
        if isinstance(answer, list):
            answer = answer.pop(0) if len(answer) > 1 else answer[0]
        if isinstance(answer, Exception):
            raise answer
        return answer


@pytest.fixture
def transport(monkeypatch):
    """Install a router under the real session; no polite wait, no retry sleep. Dependency cache starts empty."""

    def install(routes: Dict[str, Any]) -> Router:
        t = Router(routes)
        monkeypatch.setattr(requests.Session, "request", t)
        return t

    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", lambda s: None)
    rt.clear_cache()
    yield install
    rt.clear_cache()


def _get(route: str, params: Optional[dict] = None):
    return rt._get(f"{API}{route}", params)


# --- seasons --------------------------------------------------------------------------------------------------------


def test_the_season_table_is_the_live_seasons_list():
    items = _env("full/seasons.json")["data"]["items"]
    assert len(items) == 39 and len(rt.SEASON_IDS) == 39
    for s in items:
        assert rt.SEASON_IDS[int(s["name"].split("/")[1])] == s["id"], s


@pytest.mark.parametrize("end,sid", [(2027, 27), (2026, 26), (2001, 1), (2030, 30), (2000, 39), (1992, 31)])
def test_end_year_maps_to_the_opaque_season_id(end, sid):
    assert rt.season_id(end) == sid and rt.season_id(str(end)) == sid


@pytest.mark.parametrize("bad", [True, "abc", "2027.5", 1991, 2031, 0, 26.5])
def test_a_season_that_is_not_an_end_year_is_a_value_error_before_any_request(bad, transport):
    t = transport({})
    with pytest.raises(ValueError, match="END year"):
        _get("/competitions/", {"season": bad, "limit": 10000})
    assert t.calls == []


def test_the_season_param_is_sent_as_season_id_and_stamped_as_both(transport):
    t = transport({"/competitions/": _ok("full/competitions__season26.json")})
    out = _get("/competitions/", {"season": 2026, "limit": 10000})
    assert t.calls[0]["params"] == {"seasonId": 26, "limit": 10000}
    assert out["_request"] == {"season_id": 26, "season": 2026}
    assert len(out["items"]) == 44


def test_an_explicit_season_id_passes_through_untouched(transport):
    t = transport({"/competitions/": _ok("full/competitions__season26.json")})
    out = _get("/competitions/", {"seasonId": 26, "limit": 10000})
    assert t.calls[0]["params"]["seasonId"] == 26 and out["_request"]["season"] == 2026


def test_no_season_means_no_season_stamp(transport):
    transport({"/seasons/": _ok("full/seasons.json")})
    assert "season" not in _get("/seasons/", {"limit": 1000})["_request"]


# --- envelope, politeness, errors -------------------------------------------------------------------------------------


def test_the_envelope_is_unwrapped_and_a_bare_list_is_wrapped(transport):
    transport(
        {
            "/matches/579/playingDays": _ok("captures/matches__competitionId__playingDays__579.json"),
            "/competitions/579": _ok("captures/competitions__competitionId__579.json"),
        }
    )
    out = _get("/matches/579/playingDays")
    assert set(out) == {"items", "_request"} and out["_request"] == {"competition_id": 579}
    assert out["items"][0]["date"] and "status" not in out


def test_every_attempt_waits_its_turn_retries_included(monkeypatch):
    waits: List[str] = []
    monkeypatch.setattr(bb, "_polite_wait", lambda url, interval=bb.MIN_INTERVAL: waits.append(url))
    monkeypatch.setattr("time.sleep", lambda s: None)
    t = Router({"/seasons/": [_response(503, text="busy"), _response(503, text="busy"), _ok("full/seasons.json")]})
    monkeypatch.setattr(requests.Session, "request", t)
    _get("/seasons/", {"limit": 1000})
    assert len(t.calls) == 3 and len(waits) == 3  # one wait per attempt, not one per call
    assert rt._bb.SESSION is bb.SESSION and bb.MIN_INTERVAL >= 1.0


def test_a_dependency_request_waits_too(monkeypatch, transport):
    waits: List[str] = []
    transport(
        {
            "/matches/101839": _ok("full/match__101839.json"),
            "/matches/101839/playByPlay": _ok("full/playByPlay__101839.json.gz"),
        }
    )
    monkeypatch.setattr(bb, "_polite_wait", lambda url, interval=bb.MIN_INTERVAL: waits.append(url))
    _get("/matches/101839/playByPlay")
    assert len(waits) == 2  # the pbp and the header it is checked against


def test_retry_after_is_honoured_on_429_and_503(monkeypatch):
    slept: List[float] = []
    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", lambda s: slept.append(s))
    for status in (429, 503):
        slept.clear()
        t = Router(
            {"/seasons/": [_response(status, text="slow", headers={"Retry-After": "7"}), _ok("full/seasons.json")]}
        )
        monkeypatch.setattr(requests.Session, "request", t)
        _get("/seasons/", {"limit": 1000})
        assert 7.0 in slept, (status, slept)


def test_the_session_spaces_two_requests_by_the_interval(monkeypatch):
    state = {"now": 1000.0, "slept": []}
    monkeypatch.setattr(bb.time, "monotonic", lambda: state["now"])
    monkeypatch.setattr(
        bb.time, "sleep", lambda s: (state["slept"].append(s), state.__setitem__("now", state["now"] + s))
    )
    monkeypatch.setattr(bb, "_last", {})
    monkeypatch.setattr(requests.Session, "request", Router({"/a": _response(200, {}), "/b": _response(200, {})}))
    bb.SESSION.request("GET", f"{API}/a")
    bb.SESSION.request("GET", f"{API}/b")
    assert state["slept"] == [pytest.approx(bb.MIN_INTERVAL)]


@pytest.mark.parametrize(
    "body",
    [
        {"status": "INVALID_REQUEST", "errorMessage": "Match with id 99999999 does not exist"},
        {
            "status": "VALIDATION_ERROR",
            "validationErrorDetails": {"fieldErrors": {"statsCategory": "must not be null"}},
        },
    ],
)
def test_http_400_is_an_asset_fetch_error_never_a_value_error(body, transport):
    t = transport({"/matches/99999999": _response(400, body)})
    with pytest.raises(AssetFetchError, match="400") as exc:
        _get("/matches/99999999")
    assert not isinstance(exc.value, ValueError)
    assert len(t.calls) == 1  # a 400 is not retried
    assert body["status"] in str(exc.value) or "does not exist" in str(exc.value) or "fieldErrors" in str(exc.value)


def test_a_400_carries_the_apis_message_and_never_the_query(transport):
    transport(
        {
            "/matches/99999999": _response(
                400, {"status": "INVALID_REQUEST", "errorMessage": "Match with id 99999999 does not exist"}
            )
        }
    )
    with pytest.raises(AssetFetchError) as exc:
        _get("/matches/99999999")
    assert "does not exist" in str(exc.value) and "?" not in str(exc.value)


def test_404_is_no_data_and_5xx_non_json_and_empty_are_asset_fetch_errors(transport):
    transport({"/teams/1": _response(404, text="no")})
    with pytest.raises(NoDataError):
        _get("/teams/1")
    for answer in (_response(502, text="bad gateway"), _response(200, text="<html>"), _response(200, text="")):
        transport({"/teams/1": answer})
        with pytest.raises(AssetFetchError):
            _get("/teams/1")


def test_a_dead_connection_is_an_asset_fetch_error(monkeypatch):
    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", lambda s: None)
    monkeypatch.setattr(requests.Session, "request", Router({"/teams/1": requests.ConnectionError("reset")}))
    with pytest.raises(AssetFetchError):
        _get("/teams/1")


@pytest.mark.parametrize(
    "body",
    [
        {"status": "INVALID_REQUEST", "errorMessage": "x", "data": {"id": 1}},
        {"status": "OK"},
        {"data": {"id": 1}},
        {"status": True, "data": {"id": 1}},
        [{"id": 1}],
    ],
)
def test_an_envelope_that_is_not_ok_is_refused(body, transport):
    transport({"/teams/1": _response(200, body)})
    with pytest.raises(AssetFetchError):
        _get("/teams/1")


def test_an_empty_answer_is_no_data(transport):
    transport({"/teams/": _response(200, {"status": "OK", "data": {"items": [], "count": 0, "offset": 0, "limit": 0}})})
    with pytest.raises(NoDataError):
        _get("/teams/", {"competitionId": 579, "limit": 1000})
    transport({"/teams/1": _response(200, {"status": "OK", "data": {}})})
    with pytest.raises(NoDataError):
        _get("/teams/1")


def test_a_non_integer_id_is_a_value_error(transport):
    transport({})
    with pytest.raises(ValueError, match="competitionId"):
        _get("/teams/", {"competitionId": "five", "limit": 1000})
    with pytest.raises(ValueError, match="not a KZS route"):
        _get("/clubs/")


# --- truncated pages --------------------------------------------------------------------------------------------------


def test_a_full_page_is_cut_and_refused_and_the_default_cap_is_150(transport):
    transport({"/matches/": _ok("captures/matches__30.json")})
    with pytest.raises(AssetFetchError, match="truncated page"):
        _get("/matches/", {"competitionId": 30, "limit": 3})  # 3 rows at limit 3
    rows = [{"id": i, "competitions": [{"competitionId": 30}]} for i in range(150)]
    transport({"/matches/": _response(200, {"status": "OK", "data": {"items": rows}})})
    with pytest.raises(AssetFetchError, match="limit=150"):
        _get("/matches/", {"competitionId": 30})  # no limit asked: the API's silent 150


def test_a_page_under_the_limit_passes(transport):
    transport({"/matches/": _ok("captures/matches__30.json")})
    assert len(_get("/matches/", {"competitionId": 30, "limit": 1000})["items"]) == 3


# --- scope: the body must be the one asked for ------------------------------------------------------------------------


def _set(path: str, value: Any) -> Callable[[Any], None]:
    def mutate(data: Any) -> None:
        node = data
        parts = path.split(".")
        for p in parts[:-1]:
            node = node[int(p)] if isinstance(node, list) else node[p]
        if isinstance(node, list):
            node[int(parts[-1])] = value
        else:
            node[parts[-1]] = value

    return mutate


SCOPE = [
    # route, params, fixture, mutation, dependencies {route: fixture}, needle
    (
        "/competitions/579",
        None,
        "captures/competitions__competitionId__579.json",
        _set("id", 30),
        {},
        "asked for competition 579",
    ),
    (
        "/competitions/579",
        None,
        "captures/competitions__competitionId__579.json",
        _set("id", True),
        {},
        "competition 579",
    ),
    ("/matches/101839", None, "full/match__101839.json", _set("id", 62586), {}, "asked for match 101839"),
    ("/teams/17551", None, "captures/teams__teamId__17551.json", _set("id", 1), {}, "asked for team 17551"),
    ("/players/106285", None, "captures/players__playerId__106285.json", _set("id", 2), {}, "asked for player 106285"),
    (
        "/competitions/",
        {"seasonId": 26, "limit": 10000},
        "full/competitions__season26.json",
        _set("items.3.season.id", 25),
        {},
        "seasonId=26",
    ),
    (
        "/matches/",
        {"competitionId": 579, "limit": 1000},
        "captures/matches__579_2025-10.json",
        _set("items.1.competitions.0.competitionId", 584),
        {},
        "not in competition 579",
    ),
    (
        "/matches/",
        {"competitionId": 579, "from": "2025-10-01T00:00:00.000Z", "to": "2025-10-31T23:59:59.000Z", "limit": 1000},
        "captures/matches__579_2025-10.json",
        _set("items.0.dateTime", "2025-11-02T18:00:00Z"),
        {},
        "outside",
    ),
    (
        "/matches/",
        {"competitionId": 579, "from": "2025-10-01T00:00:00.000Z", "limit": 1000},
        "captures/matches__579_2025-10.json",
        _set("items.0.dateTime", None),
        {},
        "no dateTime",
    ),
    (
        "/matches/standings",
        {"competitionId": 579},
        "captures/matches__standings__579.json",
        _set("competition.id", 30),
        {},
        "competitionId=579",
    ),
    (
        "/teams/",
        {"competitionId": 579, "limit": 1000},
        "captures/teams__579.json",
        _set("items.0.id", 1),
        {"/matches/standings": "captures/matches__standings__579.json"},
        "not in competition 579",
    ),
    (
        "/competitions/579/rounds",
        None,
        "captures/competitions__competitionId__rounds__579.json",
        _set("phases.0.id", 9),
        {"/competitions/579": "captures/competitions__competitionId__579.json"},
        "not the competition's",
    ),
    (
        "/matches/579/playingDays",
        None,
        "captures/matches__competitionId__playingDays__579.json",
        _set("0.date", "2031-01-01"),
        {"/competitions/579": "captures/competitions__competitionId__579.json"},
        "outside competition 579",
    ),
    (
        "/players/106285/stats",
        {"seasonId": 26},
        "captures/players__playerId__stats__106285_26.json",
        _set("competitionStats.0.competitionId", 1),
        {"/competitions/": "full/competitions__season26.json"},
        "not in season 26",
    ),
]


@pytest.mark.parametrize(
    "route,params,fixture,mutate,deps,needle", SCOPE, ids=[f"{s[0]}-{i}" for i, s in enumerate(SCOPE)]
)
def test_a_body_for_another_competition_match_team_player_or_season_is_refused(
    route, params, fixture, mutate, deps, needle, transport
):
    routes = {route: _ok(fixture, mutate), **{r: _ok(f) for r, f in deps.items()}}
    transport(routes)
    with pytest.raises(AssetFetchError, match=needle):
        _get(route, params)


def test_a_leaderboard_must_be_the_competitions_and_in_the_order_asked(transport):
    params = {"statsCategory": "PTS", "orderType": "AVERAGE", "sortBy": "avg", "sort": "DESC", "limit": 1000}
    deps = {"/matches/standings": _ok("full/standings__579.json")}
    transport({"/competitions/579/playerStats": _ok("full/playerStats__579.json"), **deps})
    out = _get("/competitions/579/playerStats", params)
    assert out["_request"] == {"competition_id": 579, "stats_category": "PTS"}

    transport({"/competitions/579/playerStats": _ok("full/playerStats__579.json", _set("items.0.teamId", 1)), **deps})
    with pytest.raises(AssetFetchError, match="not in competition 579"):
        _get("/competitions/579/playerStats", params)

    transport({"/competitions/579/playerStats": _ok("full/playerStats__579.json"), **deps})
    with pytest.raises(AssetFetchError, match="not ordered ASC by average"):
        _get("/competitions/579/playerStats", {**params, "sort": "ASC"})


def test_the_leaderboard_query_carries_the_competition_id_from_the_path(transport):
    t = transport(
        {
            "/competitions/579/teamStats": _ok("full/teamStats__579.json"),
            "/matches/standings": _ok("full/standings__579.json"),
        }
    )
    _get("/competitions/579/teamStats", {"statsCategory": "PTS", "limit": 1000})
    assert t.calls[0]["params"]["competitionId"] == 579
    # without sortBy and sort the order cannot be checked, and is not
    transport(
        {
            "/competitions/579/teamStats": _ok("full/teamStats__579.json", lambda d: d["items"].reverse()),
            "/matches/standings": _ok("full/standings__579.json"),
        }
    )
    _get("/competitions/579/teamStats", {"statsCategory": "PTS", "limit": 1000})


def test_standings_without_rows_are_no_data(transport):
    def empty(data):
        for ph in data["phases"]:
            ph["standings"] = []
            ph["groups"] = []

    transport({"/matches/standings": _ok("captures/matches__standings__579.json", empty)})
    with pytest.raises(NoDataError, match="no standings"):
        _get("/matches/standings", {"competitionId": 579})


# --- play-by-play and box score: the game must be final and the right one ---------------------------------------------

PBP = "/matches/101839/playByPlay"
BOX = "/matches/101839/stats"
HEAD = "/matches/101839"


def test_a_game_that_is_not_final_is_refused_on_the_stat_routes_but_not_the_header(transport):
    live = _ok("full/match__101839.json", _set("status", "LIVE"))
    transport({HEAD: live, PBP: _ok("full/playByPlay__101839.json.gz"), BOX: _ok("full/stats__101839.json")})
    for route in (PBP, BOX):
        with pytest.raises(AssetFetchError, match="not final"):
            _get(route)
    assert _get(HEAD)["status"] == "LIVE"


def test_the_pbp_and_box_checks_pass_on_the_full_bodies_and_cache_the_header(transport):
    t = transport(
        {
            HEAD: _ok("full/match__101839.json"),
            PBP: _ok("full/playByPlay__101839.json.gz"),
            BOX: _ok("full/stats__101839.json"),
        }
    )
    pbp, box = _get(PBP), _get(BOX)
    assert pbp["_request"] == {"match_id": 101839} and box["_request"] == {"match_id": 101839}
    assert [c["route"] for c in t.calls] == [PBP, HEAD, BOX]  # the header was fetched once for both


def test_pbp_with_no_actions_is_no_data(transport):
    transport({HEAD: _ok("full/match__101839.json"), PBP: _ok("full/playByPlay__101839.json.gz", _set("actions", []))})
    with pytest.raises(NoDataError, match="coverage gap"):
        _get(PBP)


def test_pbp_of_another_match_or_in_the_wrong_home_away_order_is_refused(transport):
    head = _ok("full/match__101839.json")
    transport({HEAD: head, PBP: _ok("full/playByPlay__101839.json.gz", _set("actions.0.matchTeamId", 1))})
    with pytest.raises(AssetFetchError, match="is not match 101839's"):
        _get(PBP)

    def swap(data):
        data["firstTeamPlayers"], data["secondTeamPlayers"] = data["secondTeamPlayers"], data["firstTeamPlayers"]

    rt.clear_cache()
    transport({HEAD: head, PBP: _ok("full/playByPlay__101839.json.gz", swap)})
    with pytest.raises(AssetFetchError, match="home/away order"):
        _get(PBP)

    def one_team(data):
        first = next(a["matchTeamId"] for a in data["actions"] if "matchTeamId" in a)
        data["actions"] = [a for a in data["actions"] if a.get("matchTeamId", first) == first]  # drop the other team

    rt.clear_cache()
    transport({HEAD: head, PBP: _ok("full/playByPlay__101839.json.gz", one_team)})
    with pytest.raises(AssetFetchError, match="not both"):
        _get(PBP)


def test_box_score_of_another_match_or_with_swapped_teams_is_refused(transport):
    head = _ok("full/match__101839.json")
    transport({HEAD: head, BOX: _ok("full/stats__101839.json", _set("matchId", 62586))})
    with pytest.raises(AssetFetchError, match="asked for match 101839"):
        _get(BOX)

    def swap(data):
        data["firstTeam"]["teamId"], data["secondTeam"]["teamId"] = (
            data["secondTeam"]["teamId"],
            data["firstTeam"]["teamId"],
        )

    rt.clear_cache()
    transport({HEAD: head, BOX: _ok("full/stats__101839.json", swap)})
    with pytest.raises(AssetFetchError, match="home/away order"):
        _get(BOX)


def test_a_box_score_without_player_lines_is_no_data(transport):
    def bare(data):
        data["firstTeam"]["playerStats"] = []

    transport({HEAD: _ok("full/match__101839.json"), BOX: _ok("full/stats__101839.json", bare)})
    with pytest.raises(NoDataError, match="no box score"):
        _get(BOX)


def test_the_full_fixtures_pass_their_own_checks(transport):
    transport(
        {
            "/seasons/": _ok("full/seasons.json"),
            "/matches/": _ok("full/matches__579_2025-10.json"),
            "/matches/standings": _ok("full/standings__579.json"),
            "/competitions/579/playerStats": _ok("full/playerStats__579.json"),
            "/competitions/579/teamStats": _ok("full/teamStats__579.json"),
            "/players/106285/stats": _ok("full/player_stats__106285_26.json"),
            "/competitions/": _ok("full/competitions__season26.json"),
        }
    )
    assert len(_get("/seasons/", {"limit": 1000})["items"]) == 39
    window = {"competitionId": 579, "from": "2025-10-01T00:00:00.000Z", "to": "2025-10-31T23:59:59.000Z", "limit": 1000}
    assert len(_get("/matches/", window)["items"]) == 21
    board = {"statsCategory": "PTS", "orderType": "AVERAGE", "sortBy": "avg", "sort": "DESC", "limit": 1000}
    assert _get("/competitions/579/playerStats", board)["items"]
    assert _get("/competitions/579/teamStats", board)["items"]
    assert _get("/matches/standings", {"competitionId": 579})["phases"]
    assert _get("/players/106285/stats", {"season": 2026})["_request"] == {
        "player_id": 106285,
        "season_id": 26,
        "season": 2026,
    }
