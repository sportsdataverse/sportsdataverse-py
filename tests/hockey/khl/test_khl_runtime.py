"""The two KHL runtimes: the khl.ru session POST client and the webcaster GET client.

Every request goes through a fake transport (no network, no sleeping): the refusal cases are the
ones ``sdv-internal-refs/khl/tools/capture.py`` refuses to bank (error envelopes, empty answers,
answers for another game / player / stage, non-200s, cut-off bodies). Answers for something other
than what was asked are failed fetches (``AssetFetchError``); empty answers are ``NoDataError``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime
from sportsdataverse.hockey.khl import khl_rest as K
from sportsdataverse.hockey.khl import khl_rest_parsers as P
from sportsdataverse.hockey.khl import khl_runtime as R
from sportsdataverse.hockey.khl import khl_webcaster_runtime as W

FIXTURES = Path(__file__).parents[2] / "fixtures"
SID, SID2 = "0123456789abcdef0123456789abcdef", "fedcba9876543210fedcba9876543210"
REST = "https://www.khl.ru/rest"
WC = W.BASE


def _rest(name: str) -> Any:
    return json.loads((FIXTURES / "khl_rest" / name).read_text(encoding="utf-8"))


def _wc(name: str) -> Any:
    return json.loads((FIXTURES / "khl_webcaster" / name).read_text(encoding="utf-8"))


class _Resp:
    def __init__(
        self, status: int = 200, body: Any = None, text: str | None = None, headers: Dict[str, str] | None = None
    ) -> None:
        self.status_code = status
        self.text = text if text is not None else json.dumps(body)
        self.headers = headers or {}


class _Http:
    """A curl_cffi session stand-in: GET / answers a page with a sessid; POSTs pop scripted answers."""

    def __init__(self, posts: List[_Resp], homes: List[_Resp] | None = None) -> None:
        self.posts = list(posts)
        self.homes = list(homes or [_Resp(text=f'"bitrix_sessid":"{SID}"'), _Resp(text=f'"bitrix_sessid":"{SID2}"')])
        self.calls: List[tuple] = []
        self.query: Any = None

    def get(self, url: str, timeout: Any = None) -> _Resp:
        self.calls.append(("GET", url))
        home = self.homes.pop(0)
        if isinstance(home, BaseException):
            raise home
        return home

    def post(
        self,
        url: str,
        data: Any = None,
        headers: Any = None,
        allow_redirects: Any = None,
        timeout: Any = None,
        params: Any = None,
    ) -> _Resp:
        self.query = params
        self.calls.append(("POST", url, dict(data), dict(headers), allow_redirects))
        post = self.posts.pop(0)
        if isinstance(post, BaseException):
            raise post
        return post


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> List[str]:
    """No sleeping, a fresh session table, and a season table from the committed full ``data.json``."""
    waits: List[str] = []
    monkeypatch.setattr(euro_hockey_runtime, "_polite_wait", lambda url, *a: waits.append(url))
    monkeypatch.setattr(R.time, "sleep", lambda s: waits.append(f"sleep {s:g}"))
    monkeypatch.setattr(R, "_sessions", {})
    W.season_table.cache_clear()
    yield waits
    W.season_table.cache_clear()


def _http(monkeypatch: pytest.MonkeyPatch, *posts: _Resp, homes: List[_Resp] | None = None) -> _Http:
    http = _Http(list(posts), homes)
    monkeypatch.setattr(R, "_new_http", lambda: http)
    return http


def _table(monkeypatch: pytest.MonkeyPatch) -> List[str]:
    fetched: List[str] = []

    def fetch(url: str, params: Any = None, **kw: Any) -> Any:
        fetched.append(url)
        return _wc("full/data.json")

    monkeypatch.setattr(W, "_fetch", fetch)
    return fetched


HEADER = {"page": "resume", "values[gameid]": 898236, "values[tournament]": 1369}


# --- khl.ru session -------------------------------------------------------------------------


def test_post_sends_the_form_with_the_session_and_ajax_headers(monkeypatch, _offline):
    http = _http(monkeypatch, _Resp(body=_rest("game__header.json")), _Resp(body=_rest("game__header.json")))
    raw = R._get(f"{REST}/game/header/", {**HEADER, "season": None})
    assert raw["data"]["game"]["id"] == 898236
    method, url, form, headers, redirects = http.calls[1]
    assert (method, url, redirects) == ("POST", f"{REST}/game/header/", False)
    assert form == {"sessid": SID, "page": "resume", "values[gameid]": "898236", "values[tournament]": "1369"}
    assert headers["X-Requested-With"] == "XMLHttpRequest"
    assert headers["Origin"] == "https://www.khl.ru" and headers["Referer"] == "https://www.khl.ru/"
    R._get(f"{REST}/game/header/", HEADER, headers={"Accept-Language": "en"})
    assert [c[0] for c in http.calls] == ["GET", "POST", "POST"]  # primed once, then reused
    assert http.calls[2][3]["Accept-Language"] == "en"
    assert _offline == ["https://www.khl.ru/", f"{REST}/game/header/", f"{REST}/game/header/"]  # >= 1 s apart


def test_wrapper_params_go_to_the_query_string_not_the_form(monkeypatch):
    http = _http(monkeypatch, _Resp(body=_rest("game__header.json")), _Resp(body=_rest("game__header.json")))
    R._get(f"{REST}/game/header/", {"debug": "1", "unused": None}, data=HEADER)
    assert http.query == {"debug": "1"}
    assert "debug" not in http.calls[1][2]
    R._get(f"{REST}/game/header/", HEADER)  # a direct call: params ARE the form, no query string
    assert http.query is None and http.calls[2][2]["values[gameid]"] == "898236"


def test_season_and_stage_become_the_tournament_id(monkeypatch):
    fetched = _table(monkeypatch)
    http = _http(monkeypatch, _Resp(body=_rest("standings__playoff.json")), _Resp(body=_rest("calendar__list.json")))
    R._get(f"{REST}/standings/playoff/", {"values[type]": "playoff", "stage": "playoff", "season": 2026})
    assert http.calls[1][2]["values[tournament]"] == "1370"
    R._get(f"{REST}/calendar/list/", {"season": 2026, "stage": "regular"})
    assert http.calls[2][2] == {"sessid": SID, "values[tournament]": "1369"}  # season / stage never reach the wire
    assert fetched == [f"{WC}/data.json"]  # the season table is fetched once and cached


def test_an_explicit_tournament_wins_over_the_season(monkeypatch):
    http = _http(monkeypatch, _Resp(body=_rest("calendar__list.json")))
    R._get(f"{REST}/calendar/list/", {"season": 1990, "values[tournament]": 1369})  # no table lookup
    assert http.calls[1][2]["values[tournament]"] == "1369"


def test_a_game_route_without_a_tournament_is_a_value_error(monkeypatch):
    http = _http(monkeypatch)
    with pytest.raises(ValueError, match="needs a season"):
        R._get(f"{REST}/game/text/", {"values[gameid]": 898236})
    assert http.calls == []


def test_a_307_re_primes_once_then_retries(monkeypatch, _offline):
    http = _http(monkeypatch, _Resp(307, text=""), _Resp(body=_rest("game__header.json")))
    R._get(f"{REST}/game/header/", HEADER)
    assert [c[0] for c in http.calls] == ["GET", "POST", "GET", "POST"]
    assert http.calls[1][2]["sessid"] == SID and http.calls[3][2]["sessid"] == SID2
    assert _offline[2] == "https://www.khl.ru/"  # the re-prime GET is throttled after the 307


def test_a_second_307_is_a_failed_fetch(monkeypatch):
    _http(monkeypatch, _Resp(307, text=""), _Resp(307, text=""))
    with pytest.raises(AssetFetchError, match="307 again"):
        R._get(f"{REST}/game/header/", HEADER)


def test_an_error_envelope_is_a_failed_fetch(monkeypatch):
    body = {"status": "error", "data": None, "errors": [{"message": "Undefined index: gameid"}]}
    _http(monkeypatch, _Resp(body=body))
    with pytest.raises(AssetFetchError, match="Undefined index"):
        R._get(f"{REST}/game/header/", HEADER)


STALE = {"status": "error", "data": None, "errors": [{"code": "invalid_csrf", "message": "x"}]}


def test_a_stale_sessid_re_primes_a_fresh_session_and_retries_once(monkeypatch, _offline):
    http = _http(monkeypatch, _Resp(body=STALE), _Resp(body=_rest("game__header.json")))
    assert R._get(f"{REST}/game/header/", HEADER)["data"]["game"]["id"] == 898236
    assert [c[0] for c in http.calls] == ["GET", "POST", "GET", "POST"]
    assert http.calls[1][2]["sessid"] == SID and http.calls[3][2]["sessid"] == SID2  # a freshly primed sessid
    assert R._sessions["https://www.khl.ru"][1] == SID2
    assert _offline.count("https://www.khl.ru/") == 2 and _offline.count(f"{REST}/game/header/") == 2


def test_a_second_stale_sessid_raises_and_drops_the_session(monkeypatch):
    homes = [_Resp(text=f'"bitrix_sessid":"{sid}"') for sid in (SID, SID2, SID)]
    http = _http(monkeypatch, _Resp(body=STALE), _Resp(body=STALE), _Resp(body=_rest("game__header.json")), homes=homes)
    with pytest.raises(AssetFetchError, match="invalid_csrf"):
        R._get(f"{REST}/game/header/", HEADER)
    assert R._sessions == {}  # dropped: the next call primes afresh
    R._get(f"{REST}/game/header/", HEADER)
    assert [c[0] for c in http.calls] == ["GET", "POST", "GET", "POST", "GET", "POST"]


# --- retries ---------------------------------------------------------------------------------


def test_a_5xx_post_is_retried_with_a_polite_wait_before_each_attempt(monkeypatch, _offline):
    http = _http(monkeypatch, _Resp(503, text="busy"), _Resp(502, text="bad"), _Resp(body=_rest("game__header.json")))
    assert R._get(f"{REST}/game/header/", HEADER)["status"] == "success"
    assert [c[0] for c in http.calls] == ["GET", "POST", "POST", "POST"]
    post = f"{REST}/game/header/"
    assert _offline == ["https://www.khl.ru/", post, "sleep 2", post, "sleep 4", post]


def test_retry_after_is_honoured_and_capped(monkeypatch, _offline):
    _http(
        monkeypatch,
        _Resp(429, text="slow down", headers={"retry-after": "30"}),
        _Resp(429, text="slow down", headers={"retry-after": "9999"}),
        _Resp(body=_rest("game__header.json")),
    )
    R._get(f"{REST}/game/header/", HEADER)
    assert [w for w in _offline if w.startswith("sleep")] == ["sleep 30", f"sleep {R._MAX_RETRY_AFTER:g}"]


def test_retries_are_bounded(monkeypatch, _offline):
    http = _http(monkeypatch, *[_Resp(500, text="boom")] * R.ATTEMPTS)
    with pytest.raises(AssetFetchError, match="500"):
        R._get(f"{REST}/game/header/", HEADER)
    assert sum(c[0] == "POST" for c in http.calls) == R.ATTEMPTS


def test_a_dropped_connection_is_retried(monkeypatch, _offline):
    http = _http(monkeypatch, ConnectionResetError("reset"), _Resp(body=_rest("game__header.json")))
    assert R._get(f"{REST}/game/header/", HEADER)["status"] == "success"
    assert [c[0] for c in http.calls] == ["GET", "POST", "POST"] and "sleep 2" in _offline


def test_the_session_prime_is_retried(monkeypatch, _offline):
    homes = [_Resp(503, text="busy"), ConnectionResetError("reset"), _Resp(text=f'"bitrix_sessid":"{SID}"')]
    http = _http(monkeypatch, _Resp(body=_rest("game__header.json")), homes=homes)
    R._get(f"{REST}/game/header/", HEADER)
    assert [c[0] for c in http.calls] == ["GET", "GET", "GET", "POST"]
    assert _offline[:5] == ["https://www.khl.ru/", "sleep 2", "https://www.khl.ru/", "sleep 4", "https://www.khl.ru/"]


@pytest.mark.parametrize(
    ("route", "form", "body"),
    [
        ("/players/stats/", {"values[id]": 41928}, {"status": "success", "data": [], "errors": []}),
        ("/clubs/team/", {"values[club_id]": 7}, {"status": "success", "data": {"players": {}}, "errors": []}),
    ],
    ids=["no-data", "no-rows"],
)
def test_an_empty_success_is_no_data(monkeypatch, route, form, body):
    _http(monkeypatch, _Resp(body=body))
    with pytest.raises(NoDataError):
        R._get(f"{REST}{route}", form)


@pytest.mark.parametrize(
    ("route", "form", "fixture", "match"),
    [
        ("/game/header/", {**HEADER, "values[gameid]": 1}, "game__header.json", "asked for game 1"),
        ("/game/text/", {**HEADER, "values[tournament]": 1370}, "game__text.json", "of tournament 1370"),
        ("/players/stats/", {"values[id]": 1}, "players__stats.json", "asked for player 1"),
        ("/players/profile/", {"values[id]": 1}, "players__profile.json", "asked for player 1"),
        ("/calendar/list/", {"values[tournament]": 1423}, "calendar__list.json", "tournament 1423"),
    ],
    ids=["header", "text", "career", "profile", "schedule"],
)
def test_an_answer_for_something_else_is_a_failed_fetch(monkeypatch, route, form, fixture, match):
    _http(monkeypatch, _Resp(body=_rest(fixture)))
    with pytest.raises(AssetFetchError, match=match):
        R._get(f"{REST}{route}", form)


def test_a_schedule_without_games_is_no_data(monkeypatch):
    body = _rest("calendar__list.json")
    body["data"]["GAMES"] = [None, {"2026-01-01": None}]
    _http(monkeypatch, _Resp(body=body))
    with pytest.raises(NoDataError, match="no games"):
        R._get(f"{REST}/calendar/list/", {"values[tournament]": 1369})


def test_the_all_star_rows_do_not_fail_the_schedule_scope(monkeypatch):
    body = _rest("calendar__list.json")
    week = body["data"]["GAMES"][0]
    day = next(iter(week))
    week[day] = [*week[day], dict(week[day][0], id=901626, tnId=1423)]
    _http(monkeypatch, _Resp(body=body))
    assert R._get(f"{REST}/calendar/list/", {"values[tournament]": 1369})["status"] == "success"


@pytest.mark.parametrize(
    ("resp", "error"),
    [
        (_Resp(404, text="not found"), NoDataError),
        (_Resp(400, text="bad"), ValueError),
        ([_Resp(500, text="boom")] * R.ATTEMPTS, AssetFetchError),
        (_Resp(200, text='{"status": "success", "data": {"ga'), AssetFetchError),  # cut off
        (_Resp(200, text="<html>anti-bot</html>"), AssetFetchError),
        (_Resp(200, text=""), AssetFetchError),
    ],
    ids=["404", "400", "500", "truncated", "html", "empty"],
)
def test_status_and_body_follow_the_error_vocabulary(monkeypatch, resp, error):
    _http(monkeypatch, *(resp if isinstance(resp, list) else [resp]))
    with pytest.raises(error):
        R._get(f"{REST}/game/header/", HEADER)


@pytest.mark.parametrize(
    "home",
    [_Resp(text="<html>no token</html>"), _Resp(403, text="access_deny"), ConnectionResetError("reset")],
    ids=["no-sessid", "403", "dead"],
)
def test_a_session_that_cannot_be_primed_is_a_failed_fetch(monkeypatch, home):
    _http(monkeypatch, homes=[home] * R.ATTEMPTS)
    with pytest.raises(AssetFetchError):
        R._get(f"{REST}/game/header/", HEADER)


def test_a_dead_connection_is_a_failed_fetch(monkeypatch):
    http = _http(monkeypatch, *[ConnectionResetError("reset")] * R.ATTEMPTS)
    with pytest.raises(AssetFetchError, match="fetch failed after 3 attempts"):
        R._get(f"{REST}/game/header/", HEADER)
    assert sum(c[0] == "POST" for c in http.calls) == R.ATTEMPTS


def test_betting_fields_are_stripped(monkeypatch):
    body = _rest("game__text.json")
    body["data"]["game"]["fonbet"] = True
    _http(monkeypatch, _Resp(body=body))
    assert "fonbet" not in R._get(f"{REST}/game/text/", HEADER)["data"]["game"]


# --- webcaster ---------------------------------------------------------------------------------


def test_stage_for_reads_the_season_table(monkeypatch):
    fetched = _table(monkeypatch)
    assert W.stage_for(2026)["stage_id"] == 370 and W.stage_for(2026)["tournament_id"] == 1369
    assert (W.stage_for(2026, "playoff")["tournament_id"], W.stage_for(2026, "playoff")["stage_id"]) == (1370, 395)
    assert W.stage_for("2010")["tournament_id"] == 167  # odd: the parity pattern is not used
    assert fetched == [f"{WC}/data.json"]


@pytest.mark.parametrize(
    ("season", "stage", "error"),
    [
        (1990, "regular", NoDataError),
        (2027, "playoff", NoDataError),
        (2026, "final", ValueError),
        ("2025-26", "regular", ValueError),
    ],
)
def test_stage_for_refuses_what_the_table_lacks(monkeypatch, season, stage, error):
    _table(monkeypatch)
    with pytest.raises(error):
        W.stage_for(season, stage)


def _wc_get(monkeypatch: pytest.MonkeyPatch, body: Any) -> List[Dict[str, Any]]:
    calls: List[Dict[str, Any]] = []

    def fake(url: str, params: Any = None, **kw: Any) -> Any:
        calls.append({"url": url, **(params or {})})
        assert kw["session"] is euro_hockey_runtime.SESSION  # its request() waits before EVERY attempt
        return body

    monkeypatch.setattr(W._rt, "_get", fake)
    return calls


def test_webcaster_season_becomes_the_stage_id(monkeypatch):
    _table(monkeypatch)
    W.season_table()  # cached from the full data.json
    monkeypatch.undo()
    calls = _wc_get(monkeypatch, _wc("teams_v2.json"))
    W._get(f"{WC}/teams_v2.json", {"season": 2026, "stage": "regular", "locale": "en"})
    assert calls == [{"url": f"{WC}/teams_v2.json", "stage_id": 370, "locale": "en"}]


@pytest.mark.parametrize(
    ("body", "error"),
    [
        ({"error": {"code": 404, "message": "Not found"}}, NoDataError),
        ({"error": {"code": 500, "message": "x"}}, AssetFetchError),
        ({}, NoDataError),
    ],
    ids=["404-envelope", "500-envelope", "empty-object"],
)
def test_webcaster_error_envelopes(monkeypatch, body, error):
    _wc_get(monkeypatch, body)
    with pytest.raises(error):
        W._get(f"{WC}/event_v2.json", {"id": 2786714})


@pytest.mark.parametrize(
    ("route", "params", "fixture"),
    [
        ("event_v2.json", {"id": 1}, "event_v2.json"),
        ("team_v2.json", {"id": 1}, "team_v2.json"),
        ("events_v2.json", {"stage_id": 407}, "events_v2.json"),
        ("players_v2.json", {"q[id_in][]": [33405]}, "players_v2.json"),
    ],
    ids=["event", "team", "events-stage", "players"],
)
def test_webcaster_answer_for_something_else_is_a_failed_fetch(monkeypatch, route, params, fixture):
    _wc_get(monkeypatch, _wc(fixture))
    with pytest.raises(AssetFetchError, match="asked for"):
        W._get(f"{WC}/{route}", params)


def test_an_empty_events_page_ends_the_stage(monkeypatch):
    _wc_get(monkeypatch, [])
    assert W._get(f"{WC}/events_v2.json", {"stage_id": 370, "page": 48}) == []


def test_webcaster_redactions(monkeypatch):
    body = {"remote_ip": "203.0.113.9", "stages_v2": [{"id": 1, "bets": {"url": "https://bookmaker/?aff=1"}}]}
    _wc_get(monkeypatch, body)
    assert W._get(f"{WC}/data.json") == {"stages_v2": [{"id": 1}]}


def test_the_proxy_is_used_and_never_reported(monkeypatch):
    secret = "http://user:s3cret@proxy.example:8000"
    monkeypatch.setenv(W.PROXY_ENV, secret)
    seen: Dict[str, Any] = {}

    def fake(transport: Any, url: str, **kw: Any) -> Any:
        seen.update(kw)
        try:
            raise OSError(f"ProxyError: cannot connect to {secret}")
        except OSError as exc:
            raise AssetFetchError("khl.api.webcaster.pro: fetch failed after retries: OSError") from exc

    monkeypatch.setattr(W._rt, "_download_json", fake)
    with pytest.raises(AssetFetchError) as info:
        W._get(f"{WC}/tables_v2.json", {"locale": "en"})
    assert seen["proxy"] == {"http": secret, "https": secret} and seen["logger"] is W._QUIET
    assert seen["session"] is euro_hockey_runtime.SESSION
    assert "s3cret" not in str(info.value) and info.value.__cause__ is None and info.value.__suppress_context__


# --- goalies: every page, sorted by games played ---------------------------------------------


def _goalie_page(page: int, last: int, gps: List[Tuple[int, int, str]], nav: bool = True) -> _Resp:
    """A ``position=goalies`` page built from the committed fixture: one row per ``(gp, w, name)``."""
    body = _rest("stat__players_goalies.json")
    group = body["data"]["list"][0]
    template = group["list"][0]
    rows = []
    for gp, w, name in gps:
        row = json.loads(json.dumps(template))
        row["first"][1].update(val=name, link=f"/players/{int.from_bytes(name.encode(), 'big')}/")
        row["cols"].update(gp=gp, w=w)
        rows.append(row)
    group["list"] = rows
    if nav:
        group["nav"] = {"active": page, "start": 1, "end": last, "max": last}
    else:
        group.pop("nav", None)
    return _Resp(body=body)


GOALIES = {"values[tournament]": 1369, "values[amplua_full]": "goalies", "nav[count]": 3}


def test_goalies_fetch_every_page_and_sort_by_games_played_then_wins_then_name(monkeypatch):
    http = _http(
        monkeypatch,
        _goalie_page(1, 3, [(0, 0, "Bee"), (0, 0, "Abe"), (12, 5, "Cid")]),
        _goalie_page(2, 3, [(30, 18, "Dan"), (30, 20, "Eli"), (12, 5, "Ace")]),
        _goalie_page(3, 3, [(45, 25, "Fox")]),
    )
    raw = R._get(f"{REST}/stat/players/", GOALIES)
    assert [c[2]["nav[page]"] for c in http.calls if c[0] == "POST"] == ["1", "2", "3"]
    df = P.parse_khl_player_stats(raw)
    assert df["player_name"].to_list() == ["Fox", "Eli", "Dan", "Ace", "Cid", "Abe", "Bee"]
    assert df["gp"].to_list() == [45, 30, 30, 12, 12, 0, 0]


def test_goalies_through_the_wrapper_and_skaters_stay_one_page(monkeypatch):
    http = _http(
        monkeypatch,
        _goalie_page(1, 2, [(0, 0, "Abe"), (1, 0, "Bee"), (2, 1, "Cid")]),
        _goalie_page(2, 2, [(40, 22, "Dan")]),
        _Resp(body=_rest("stat__players.json")),
    )
    df = K.khl_player_stats(tournament_id=1369, position="goalies", per_page=3)
    assert df["gp"].to_list() == [40, 2, 1, 0] and df["group"].unique().to_list() == ["goalies"]
    K.khl_player_stats(tournament_id=1369, position="forwards", page=2)
    posts = [c[2] for c in http.calls if c[0] == "POST"]
    assert len(posts) == 3 and posts[2]["nav[page]"] == "2"  # skaters: the one page asked for


def test_a_missing_goalie_page_never_returns_a_partial_list(monkeypatch):
    empty = _rest("stat__players_goalies.json")
    empty["data"]["list"][0]["list"] = []
    _http(
        monkeypatch,
        _goalie_page(1, 3, [(0, 0, "Abe"), (1, 0, "Bee"), (2, 1, "Cid")]),
        _goalie_page(2, 3, [(3, 1, "Dan"), (4, 2, "Eli"), (5, 3, "Fox")]),
        _Resp(body=empty),  # page 3 of 3 comes back empty
    )
    with pytest.raises(AssetFetchError, match="do not fill 3 pages"):
        R._get(f"{REST}/stat/players/", GOALIES)


def test_goalie_paging_is_bounded_and_refuses_a_shifted_list(monkeypatch):
    _http(monkeypatch, _goalie_page(1, 999, [(0, 0, "Abe"), (1, 0, "Bee"), (2, 1, "Cid")]))
    with pytest.raises(AssetFetchError, match="page guard"):
        R._get(f"{REST}/stat/players/", GOALIES)

    monkeypatch.setattr(R, "MAX_PAGES", 2)
    monkeypatch.setattr(R, "_sessions", {})
    full = [(1, 0, "A"), (2, 0, "B"), (3, 0, "C")]
    _http(monkeypatch, *(_goalie_page(i, 0, [(g, w, f"{n}{i}") for g, w, n in full], nav=False) for i in (1, 2, 3)))
    with pytest.raises(AssetFetchError, match="still paging goalies after 2 pages"):
        R._get(f"{REST}/stat/players/", GOALIES)

    monkeypatch.setattr(R, "_sessions", {})
    _http(
        monkeypatch,
        _goalie_page(1, 2, [(0, 0, "Abe"), (1, 0, "Bee"), (2, 1, "Cid")]),
        _goalie_page(2, 2, [(2, 1, "Cid")]),  # Cid moved onto page 2 too
    )
    with pytest.raises(AssetFetchError, match="on two pages"):
        R._get(f"{REST}/stat/players/", GOALIES)
