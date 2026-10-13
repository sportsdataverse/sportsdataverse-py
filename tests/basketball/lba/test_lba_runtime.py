"""The Lega Basket runtime: season mapping, politeness per attempt, Retry-After, and every refusal (mocked transport)."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
import requests

from sportsdataverse.basketball import basketball_runtime as bb
from sportsdataverse.basketball.lba import lba_runtime as rt
from sportsdataverse.errors import AssetFetchError, NoDataError

FIX = Path(__file__).resolve().parents[2] / "fixtures" / "legabasket"
API = "https://www.legabasket.it/api"


def _load(name: str) -> Any:
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def _response(status: int = 200, body: Any = None, *, text: Optional[str] = None, headers: Optional[dict] = None):
    resp = requests.Response()
    resp.status_code = status
    resp._content = (text if text is not None else json.dumps(body)).encode()
    resp.headers.update(headers or {"Content-Type": "application/json"})
    resp.url = API
    return resp


class Transport:
    """Replaces ``requests.Session.request``: answers from a script and records every attempt."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, session, method, url, *args, **kwargs):
        self.calls.append({"url": url, "params": dict(kwargs.get("params") or {})})
        answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(answer, Exception):
            raise answer
        return answer


@pytest.fixture
def transport(monkeypatch):
    """Install a scripted transport under the real session; no polite wait, no retry sleep."""

    def install(*answers):
        t = Transport(*answers)
        monkeypatch.setattr(requests.Session, "request", t)
        return t

    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", lambda s: None)
    return install


def _get(route: str, params: Optional[dict] = None):
    return rt._get(f"{API}{route}", params)


# --- seasons -------------------------------------------------------------------------------------------------------


def test_end_year_is_sent_as_the_start_year_and_stamped_on_the_body(transport):
    body = _load("full/teams__2026.json")
    t = transport(_response(body=body))
    out = _get("/teams/get-teams", {"year": 2027, "items": 50})
    assert t.calls[0]["params"]["year"] == 2026 and t.calls[0]["params"]["items"] == 50
    assert out["_request"] == {"season": 2027}


def test_s_param_maps_too_and_other_params_pass_through(transport):
    body = _load("full/players_statistics__points.json")
    t = transport(_response(body=body))
    _get("/statistics/get-players-statistics", {"s": 2027, "cs_id": 1, "cat": "points"})
    assert t.calls[0]["params"] == {"s": 2026, "cs_id": 1, "cat": "points"}


@pytest.mark.parametrize("bad", ["abc", 1800, 2300, True, 2026.5, []])
def test_a_season_that_is_not_an_end_year_is_a_value_error_before_any_request(bad, transport):
    t = transport(_response(body={}))
    with pytest.raises(ValueError, match="END year"):
        _get("/teams/get-teams", {"year": bad})
    assert t.calls == []


def test_no_season_means_no_stamp(transport):
    transport(_response(body=_load("full/calendar__602__d1.json")))
    assert "_request" not in _get("/championships/get-championships-calendar-by-id", {"id": 602, "d": 1})


# --- politeness -----------------------------------------------------------------------------------------------------


def test_every_attempt_waits_its_turn_retries_included(monkeypatch):
    waits: List[tuple] = []
    monkeypatch.setattr(bb, "_polite_wait", lambda url, interval=bb.MIN_INTERVAL: waits.append((url, interval)))
    monkeypatch.setattr("time.sleep", lambda s: None)
    t = Transport(
        _response(503, text="busy", headers={}),
        _response(503, text="busy", headers={}),
        _response(200, _load("full/teams__2026.json")),
    )
    monkeypatch.setattr(requests.Session, "request", t)
    _get("/teams/get-teams", {"year": 2027})
    assert len(t.calls) == 3 and len(waits) == 3  # one wait per attempt, not one per call
    assert {w[1] for w in waits} == {rt.INTERVAL} and rt.INTERVAL == 2.0  # robots.txt Crawl-delay: 2


def test_retry_after_is_honoured_on_429_and_503(monkeypatch):
    slept: List[float] = []
    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", slept.append)
    for status in (429, 503):
        slept.clear()
        t = Transport(
            _response(status, text="slow", headers={"Retry-After": "7"}), _response(200, _load("full/teams__2026.json"))
        )
        monkeypatch.setattr(requests.Session, "request", t)
        _get("/teams/get-teams", {"year": 2027})
        assert 7.0 in slept, (status, slept)


def test_the_session_spaces_two_requests_by_the_interval(monkeypatch):
    state = {"now": 1000.0, "slept": []}
    monkeypatch.setattr(bb.time, "monotonic", lambda: state["now"])
    monkeypatch.setattr(
        bb.time, "sleep", lambda s: (state["slept"].append(s), state.__setitem__("now", state["now"] + s))
    )
    monkeypatch.setattr(bb, "_last", {})
    monkeypatch.setattr(requests.Session, "request", Transport(_response(200, {})))
    rt.SESSION.request("GET", f"{API}/a")
    rt.SESSION.request("GET", f"{API}/b")
    assert state["slept"] == [pytest.approx(rt.INTERVAL)]


# --- the HTTP error vocabulary -----------------------------------------------------------------------------------------


def test_an_unknown_id_is_a_500_that_is_not_retried_and_is_an_asset_fetch_error(transport):
    t = transport(_response(500, text="Internal Server Error", headers={"Content-Type": "text/plain"}))
    with pytest.raises(AssetFetchError, match="500"):
        _get("/championships/get-championships-matches-by-id", {"id": 1})
    assert len(t.calls) == 1


def test_404_is_no_data_and_400_is_a_value_error(transport):
    transport(_response(404, text="nope", headers={}))
    with pytest.raises(NoDataError):
        _get("/teams/get-teams", {"year": 2027})
    transport(_response(422, text="bad", headers={}))
    with pytest.raises(ValueError):
        _get("/teams/get-teams", {"year": 2027})


def test_a_non_json_or_empty_200_is_an_asset_fetch_error(transport):
    transport(_response(200, text="<html>maintenance</html>", headers={"Content-Type": "text/html"}))
    with pytest.raises(AssetFetchError):
        _get("/teams/get-teams", {"year": 2027})
    transport(_response(200, text="", headers={}))
    with pytest.raises(AssetFetchError):
        _get("/teams/get-teams", {"year": 2027})


def test_a_dead_connection_is_an_asset_fetch_error(monkeypatch):
    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", lambda s: None)
    monkeypatch.setattr(requests.Session, "request", Transport(requests.ConnectionError("boom")))
    with pytest.raises(AssetFetchError):
        rt._get(f"{API}/teams/get-teams", {"year": 2027}, num_retries=1)


@pytest.mark.parametrize("envelope", [{"error": "x"}, {"message": "Internal server error", "statusCode": 500}, [1, 2]])
def test_an_error_envelope_or_a_non_object_is_an_asset_fetch_error(envelope, transport):
    transport(_response(200, envelope))
    with pytest.raises(AssetFetchError):
        _get("/teams/get-teams", {"year": 2027})


# --- nothing here: NoDataError ----------------------------------------------------------------------------------------------


def _pbp() -> Dict[str, Any]:
    return _load("full/pbp__25602.json")


def test_old_seasons_answer_empty_and_that_is_no_data(transport):
    body = _pbp()
    body["pbp"]["actions"] = []
    transport(_response(body=body))
    with pytest.raises(NoDataError, match="pbp.actions"):
        _get("/championships/get-championships-matches-play-by-play", {"id": 25602, "info": "true"})
    rank = _load("full/ranking__602__d3.json")
    rank["standings"], rank["teams_matches_series"] = None, None
    transport(_response(body=rank))
    with pytest.raises(NoDataError, match="standings"):
        _get("/championships/get-championship-ranking", {"id": 602})
    transport(_response(body={"competitions": [], "pagination": {"count": 0}}))
    with pytest.raises(NoDataError):
        _get("/championships/get-championships", {"s": 2099, "cs_id": 1, "items": 1000})


# --- wrong scope and partial answers: AssetFetchError ------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("route", "params", "mutate", "needle"),
    [
        (
            "/championships/get-championships-matches-play-by-play",
            {"id": 25602, "info": "true"},
            lambda b: b["match"].update(id=25603),
            "match.id",
        ),
        (
            "/championships/get-championships-matches-scores-by-id",
            {"id": 25602, "info": "true"},
            lambda b: b["match"].clear(),
            "match.id",
        ),
        (
            "/matches/get-championship-matches-shots-by-id",
            {"id": 25602, "info": 1},
            lambda b: b["match"].update(id=1),
            "match.id",
        ),
    ],
)
def test_a_body_for_another_game_is_refused_not_returned_empty(route, params, mutate, needle, transport):
    body = (
        _load("full/matches__25602.json")
        if "shots" not in route
        else _load("captures/2026/matches__get-championship-matches-shots-by-id.json")
    )
    if "play-by-play" in route:
        body = _pbp()
    mutate(body)
    transport(_response(body=body))
    with pytest.raises(AssetFetchError, match=needle):
        _get(route, params)


def test_a_game_that_is_not_final_is_refused_on_the_stat_routes_but_not_the_header(transport):
    for route, params, body in (
        (
            "/championships/get-championships-matches-scores-by-id",
            {"id": 25602, "info": "true"},
            _load("full/matches__25602.json"),
        ),
        ("/championships/get-championships-matches-play-by-play", {"id": 25602, "info": "true"}, _pbp()),
        (
            "/matches/get-championship-matches-shots-by-id",
            {"id": 25602, "info": 1},
            _load("captures/2026/matches__get-championship-matches-shots-by-id.json"),
        ),
    ):
        body["match"]["game_status"] = "1"
        transport(_response(body=body))
        with pytest.raises(AssetFetchError, match="not final"):
            _get(route, params)
    header = _load("full/matches__25602.json")
    header["match"]["game_status"] = "0"
    transport(_response(body=header))
    assert _get("/championships/get-championships-matches-by-id", {"id": 25602, "sc": 1})["match"]["game_status"] == "0"


def test_wrong_season_championship_round_team_player_and_category_are_refused(transport):
    teams = _load("full/teams__2026.json")
    transport(_response(body=teams))
    with pytest.raises(AssetFetchError, match="year"):
        _get("/teams/get-teams", {"year": 2026 + 2, "items": 50})  # asked for 2027-28 = START 2027, body says 2026
    cal = _load("full/calendar__602__d1.json")
    transport(_response(body=cal))
    with pytest.raises(AssetFetchError, match="competition.id"):
        _get("/championships/get-championships-calendar-by-id", {"id": 603, "d": 1})
    transport(_response(body=cal))
    with pytest.raises(AssetFetchError, match="matches"):
        _get("/championships/get-championships-calendar-by-id", {"id": 602, "d": 2})
    roster = _load("captures/2026/teams__get-team-roster.json")
    transport(_response(body=roster))
    with pytest.raises(AssetFetchError, match="team.id"):
        _get("/teams/get-team-roster", {"id": 1})
    transport(_response(body=_load("full/player_matches__969.json")))
    with pytest.raises(AssetFetchError, match="player.id"):
        _get("/players/get-player-matches", {"id": 970, "s": 2027, "cs_id": 1})
    transport(_response(body=_load("full/player_matches__969.json")))
    with pytest.raises(AssetFetchError, match="season"):
        _get("/players/get-player-matches", {"id": 969, "s": 2025, "cs_id": 1})  # START 2024; the game codes say 2026
    transport(_response(body=_load("full/players_statistics__points.json")))
    with pytest.raises(AssetFetchError, match="category"):
        _get("/statistics/get-players-statistics", {"s": 2027, "cs_id": 1, "cat": "assists"})


def test_standings_must_sit_on_the_round_asked_for(transport):
    rank = _load("full/ranking__602__d3.json")
    transport(_response(body=rank))
    assert _get("/championships/get-championship-ranking", {"id": 602, "d": 3})["standings"]
    transport(_response(body=rank))
    with pytest.raises(AssetFetchError, match="past round 2"):
        _get("/championships/get-championship-ranking", {"id": 602, "d": 2})
    mixed = copy.deepcopy(rank)
    mixed["standings"][0]["championships_day_id"] = 1
    transport(_response(body=mixed))
    with pytest.raises(AssetFetchError, match="several rounds"):
        _get("/championships/get-championship-ranking", {"id": 602, "d": 3})


def test_a_truncated_list_page_is_refused_and_a_requested_page_is_not(transport):
    body = _load("full/teams__2026.json")
    body["pagination"]["count"] = 40
    transport(_response(body=body))
    with pytest.raises(AssetFetchError, match="truncated"):
        _get("/teams/get-teams", {"year": 2027, "items": 50})
    transport(_response(body=body))
    assert _get("/teams/get-teams", {"year": 2027, "items": 50, "page": 1})["teams"]


def test_the_full_fixtures_pass_their_own_checks(transport):
    cases = {
        "/championships/get-championships": ("full/championships__s2026.json", {"s": 2027, "cs_id": 1, "items": 1000}),
        "/championships/get-championships-calendar-by-id": ("full/calendar__602__d1.json", {"id": 602, "d": 1}),
        "/championships/get-championships-matches-by-id": ("full/matches__25602.json", {"id": 25602, "sc": 1}),
        "/championships/get-championships-matches-play-by-play": (
            "full/pbp__25602.json",
            {"id": 25602, "info": "true"},
        ),
        "/championships/get-championship-ranking": ("full/ranking__602__d3.json", {"id": 602, "d": 3}),
        "/teams/get-teams": ("full/teams__2026.json", {"year": 2027}),
        "/teams/get-team-players-stats": ("full/team_players_stats__1764.json", {"id": 1764, "s": 2027, "cs_id": 1}),
        "/players/get-player-matches": ("full/player_matches__969.json", {"id": 969, "s": 2027, "cs_id": 1}),
        "/statistics/get-players-statistics": (
            "full/players_statistics__points.json",
            {"s": 2027, "cs_id": 1, "cat": "points"},
        ),
        "/statistics/get-teams-statistics-by-type": (
            "full/teams_statistics__offensive.json",
            {"type": "teams_offensive", "s": 2027, "cs_id": 1},
        ),
    }
    for route, (fixture, params) in cases.items():
        transport(_response(body=_load(fixture)))
        assert _get(route, params), route
