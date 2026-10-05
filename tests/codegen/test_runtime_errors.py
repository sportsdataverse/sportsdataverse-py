"""A failed fetch raises; it never reaches a parser as data (CLAUDE.md "Error vocabulary").

Every getter here is driven through the REAL ``dl_utils.download`` (retry loop, ESPN
``code: 404`` probe) with only ``requests.Session.get`` replaced, serving the real error
bodies in ``tests/fixtures/runtime_errors/`` where one could be captured.

The vocabulary: 2xx JSON -> the body; 204/205 -> ``{}``; 404 (and ESPN's 200 with
``code: 404``) -> ``NoDataError``; 400/422 -> ``ValueError``; any other non-2xx after
retries, a connection failure after retries, an empty 200 or a non-JSON 2xx ->
``AssetFetchError``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, List, Tuple, Union

import pytest
import requests

from sportsdataverse._codegen_runtime import _get
from sportsdataverse.errors import AssetFetchError, NoDataError

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "runtime_errors"
FOX_KEY = "jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq"
Answer = Tuple[int, str, str]


def _body(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


def _route(monkeypatch, answer: Callable[[str], Union[Answer, Exception]]) -> List[str]:
    """Answer every ``requests.Session.get`` with ``answer(url)``; return the URLs hit."""
    import sportsdataverse.cache as _cache

    calls: List[str] = []

    def get(self, url, params=None, **kwargs):
        full = requests.Request("GET", url, params=params).prepare().url
        calls.append(full)
        out = answer(full)
        if isinstance(out, Exception):
            raise out
        status, body, ctype = out
        resp = requests.Response()
        resp.status_code = status
        resp._content = body.encode("utf-8")
        resp.encoding = "utf-8"
        resp.headers["Content-Type"] = ctype
        resp.url = full
        return resp

    monkeypatch.setattr(_cache, "get_cache_mode", lambda: "off")
    monkeypatch.setattr("sportsdataverse.dl_utils.time.sleep", lambda *a, **k: None)
    monkeypatch.setattr(requests.Session, "get", get)
    return calls


def _serve(monkeypatch, status: int, body: str = "", ctype: str = "application/json") -> List[str]:
    return _route(monkeypatch, lambda url: (status, body, ctype))


# --------------------------------------------------------------------------------------
# the shared getter, through generated wrappers of three families
# --------------------------------------------------------------------------------------


def test_2xx_json_is_returned_unchanged(monkeypatch):
    _serve(monkeypatch, 200, '{"team": {"id": "13"}, "athletes": []}')
    from sportsdataverse.nba import espn_nba_team_roster

    assert espn_nba_team_roster(team_id=13, return_parsed=False) == {"team": {"id": "13"}, "athletes": []}


def test_2xx_json_array_is_returned_unchanged(monkeypatch):
    _serve(monkeypatch, 200, "[1, 2]")
    assert _get("https://example.test/x") == [1, 2]


@pytest.mark.parametrize("status", [204, 205])
def test_no_content_is_a_deliberate_nothing(monkeypatch, status):
    _serve(monkeypatch, status, "")
    assert _get("https://example.test/x") == {}


def test_empty_200_is_a_failed_fetch(monkeypatch):
    # barttorvik's block, pro.nfl.com's rejected params and stats-host throttling all
    # answer a blank 200; that is not "nothing here".
    _serve(monkeypatch, 200, "  ")
    with pytest.raises(AssetFetchError, match="example.test/x answered HTTP 200 with an empty body"):
        _get("https://example.test/x")


@pytest.mark.parametrize(
    ("fixture", "ctype", "call"),
    [
        ("cbs_napi_404.json", "application/json", lambda: _get("https://api.cbssports.com/napi/resource/nope")),
        ("nhl_api_web_404.html", "text/html", lambda: __import__("sportsdataverse.nhl", fromlist=["x"]).nhl_web_pbp(1)),
        ("espn_core_event_404.json", "application/json", lambda: _get("https://sports.core.api.espn.com/v2/x")),
    ],
)
def test_404_is_no_data(monkeypatch, fixture, ctype, call):
    calls = _serve(monkeypatch, 404, _body(fixture), ctype)
    with pytest.raises(NoDataError):
        call()
    assert len(calls) == 1  # a definitive answer: never retried


def test_espn_200_with_code_404_body_is_no_data(monkeypatch):
    _serve(monkeypatch, 200, _body("espn_site_summary_404.json"))
    from sportsdataverse.nfl import espn_nfl_summary

    with pytest.raises(NoDataError):
        espn_nfl_summary(event_id=1)


def test_fox_401_error_body_is_a_failed_fetch(monkeypatch):
    # Before: {"fault": {"faultstring": "Invalid ApiKey", ...}} came back as the payload.
    calls = _serve(monkeypatch, 401, _body("fox_bifrost_401_invalid_apikey.json"))
    from sportsdataverse.fox import fox_api_scoreboard

    with pytest.raises(AssetFetchError) as ei:
        fox_api_scoreboard(sport="nfl", return_parsed=False)
    msg = str(ei.value)
    assert "api.foxsports.com/bifrost/v1/nfl/scoreboard/main" in msg
    assert "HTTP 401" in msg and "Invalid ApiKey" in msg
    assert FOX_KEY not in msg
    assert len(calls) == 1  # 401 is not retried
    assert FOX_KEY in calls[0]  # ...and the key really was on the request


@pytest.mark.parametrize("status", [400, 422])
def test_rejected_request_is_a_value_error(monkeypatch, status):
    # The request itself is wrong (real ESPN 400 body): no retry, not "no data".
    calls = _serve(monkeypatch, status, _body("espn_site_roster_400.json"))
    from sportsdataverse.nba import espn_nba_team_roster

    with pytest.raises(
        ValueError, match=rf"site\.api\.espn\.com/.*/teams/99999/roster rejected the request: HTTP {status}"
    ):
        espn_nba_team_roster(team_id=99999)
    assert len(calls) == 1


@pytest.mark.parametrize("status", [403, 429, 503])
def test_retryable_status_after_retries_is_a_failed_fetch(monkeypatch, status):
    calls = _serve(monkeypatch, status, '{"error": "rate limited", "token": "s3cr3t-value-123"}')
    from sportsdataverse.fox import fox_api_scoreboard

    with pytest.raises(AssetFetchError, match=f"HTTP {status}") as ei:
        fox_api_scoreboard(sport="nfl", num_retries=2)  # parsed path: no empty frame either
    assert len(calls) == 3  # retried, then surfaced
    assert "s3cr3t-value-123" not in str(ei.value)


def test_connection_failure_after_retries_is_a_failed_fetch(monkeypatch):
    # download() re-raises the last requests exception once its budget is spent.
    boom = requests.ConnectionError("Max retries exceeded with url: /x?apikey=" + FOX_KEY)
    calls = _route(monkeypatch, lambda url: boom)
    with pytest.raises(AssetFetchError, match="example.test/x: fetch failed after retries: ConnectionError") as ei:
        _get("https://example.test/x", params={"apikey": FOX_KEY}, num_retries=2)
    assert len(calls) == 3
    assert isinstance(ei.value.__cause__, requests.ConnectionError)
    assert FOX_KEY not in str(ei.value) and FOX_KEY not in str(ei.value.__cause__)


def test_non_json_2xx_is_a_failed_fetch_without_the_decode_error(monkeypatch):
    _serve(monkeypatch, 200, "<html>Service Unavailable</html>", "text/html")
    with pytest.raises(AssetFetchError, match="example.test/x answered HTTP 200 with a non-JSON body") as ei:
        _get("https://example.test/x", params={"apikey": FOX_KEY})
    # Not chained at all: a JSONDecodeError's ``.doc`` is the whole body.
    assert ei.value.__context__ is None and ei.value.__cause__ is None
    assert FOX_KEY not in str(ei.value)


def test_message_never_quotes_a_query_string_the_url_carries(monkeypatch):
    # ESPN $ref links arrive with their own query string; a key may ride in it.
    url = (
        f"http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/1?lang=en&region=us&apikey={FOX_KEY}"
    )
    _serve(monkeypatch, 503, "{}")
    with pytest.raises(AssetFetchError) as ei:
        _get(url, num_retries=0)
    msg = str(ei.value)
    assert msg.startswith("sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/1 answered HTTP 503")
    assert "lang=en" not in msg and "region" not in msg and FOX_KEY not in msg


def test_failed_fetch_is_never_an_empty_frame(monkeypatch):
    _serve(monkeypatch, 503, '{"message": "Service Unavailable"}')
    from sportsdataverse.nhl import nhl_web_pbp

    with pytest.raises(AssetFetchError):  # before: a zero-row frame indistinguishable from "no plays"
        nhl_web_pbp(2023030417, num_retries=0)


# --------------------------------------------------------------------------------------
# every other runtime getter that had the bug
# --------------------------------------------------------------------------------------

_JSON = ["mlb_api_extra", "mls", "nwsl", "yahoo", "on3", "nfl_api", "nhl_scoreboard", "nhl_records"]
_TEXT = ["statcast", "torvik", "kenpom", "statcast_player"]


def _getter(name: str) -> Callable[[], Any]:
    from sportsdataverse import _subscription_http
    from sportsdataverse.cfb import on3_runtime
    from sportsdataverse.mbb import kenpom_runtime, torvik_runtime
    from sportsdataverse.mlb import mlb_api_extra, mlb_statcast_extra, mlb_statcast_runtime
    from sportsdataverse.nfl import nfl_api_runtime
    from sportsdataverse.nhl import nhl_api_web_extra, nhl_records_extra
    from sportsdataverse.soccer.mls import mls_api_runtime
    from sportsdataverse.soccer.nwsl import nwsl_api_runtime
    from sportsdataverse.yahoo import yahoo_shangrila_runtime

    return {
        "mlb_api_extra": lambda: mlb_api_extra._get("/api/v1/teams", num_retries=0),
        "mls": lambda: mls_api_runtime._get("https://stats-api.mlssoccer.com/competitions", num_retries=0),
        "nwsl": lambda: nwsl_api_runtime._get("https://api-sdp.nwslsoccer.com/v1/x", num_retries=0),
        "yahoo": lambda: yahoo_shangrila_runtime._get(
            "https://graphite-secure.sports.yahoo.com/v1/query/x", num_retries=0
        ),
        "on3": lambda: on3_runtime._get("https://api.on3.com/public/rdb/v1/x", num_retries=0),
        "nfl_api": lambda: nfl_api_runtime._get("https://api.nfl.com/football/v2/x", headers={}, num_retries=0),
        "nhl_scoreboard": lambda: nhl_api_web_extra.nhl_scoreboard(return_parsed=False, num_retries=0),
        "nhl_records": lambda: nhl_records_extra._fetch("/fastest-goals", num_retries=0),
        "statcast": lambda: mlb_statcast_runtime._get("https://baseballsavant.mlb.com/leaderboard/x", num_retries=0),
        "torvik": lambda: torvik_runtime._get("https://barttorvik.com/x.csv", num_retries=0),
        "kenpom": lambda: _subscription_http.get_html(
            kenpom_runtime.KENPOM, "https://kenpom.com/index.php", session=requests.Session(), num_retries=0
        ),
        "statcast_player": lambda: mlb_statcast_extra._player_page_html(545361, num_retries=0),
    }[name]


@pytest.mark.parametrize("name", _JSON + _TEXT)
@pytest.mark.parametrize("status", [401, 503])
def test_other_getters_raise_on_a_failed_fetch(monkeypatch, name, status):
    _serve(monkeypatch, status, '{"errors": [{"message": "nope"}]}')
    with pytest.raises(AssetFetchError, match=f"HTTP {status}"):
        _getter(name)()


@pytest.mark.parametrize("name", _JSON + _TEXT)
@pytest.mark.parametrize("status", [400, 422])
def test_other_getters_raise_value_error_on_a_rejected_request(monkeypatch, name, status):
    # Yahoo answers a bad persisted query with HTTP 400 {"errors": [...]}: not data.
    _serve(monkeypatch, status, '{"errors": [{"message": "PersistedQueryNotFound"}]}')
    with pytest.raises(ValueError, match=f"rejected the request: HTTP {status}"):
        _getter(name)()


@pytest.mark.parametrize("name", _JSON + _TEXT)
def test_other_getters_404_is_no_data(monkeypatch, name):
    # on3 and yahoo used to swallow this into ``{}``.
    _serve(monkeypatch, 404, "")
    with pytest.raises(NoDataError):
        _getter(name)()


@pytest.mark.parametrize("name", _JSON + _TEXT)
def test_other_getters_raise_on_an_empty_200(monkeypatch, name):
    _serve(monkeypatch, 200, "", "text/html")
    with pytest.raises(AssetFetchError, match="empty body"):
        _getter(name)()


@pytest.mark.parametrize("name", _JSON + _TEXT)
def test_other_getters_raise_on_a_connection_failure(monkeypatch, name):
    _route(monkeypatch, lambda url: requests.ReadTimeout("read timed out"))
    with pytest.raises(AssetFetchError, match="fetch failed after retries: ReadTimeout") as ei:
        _getter(name)()
    assert isinstance(ei.value.__cause__, requests.ReadTimeout)


@pytest.mark.parametrize("name", _JSON)
def test_other_json_getters_raise_on_a_non_json_2xx(monkeypatch, name):
    _serve(monkeypatch, 200, "<html>Access Denied</html>", "text/html")
    with pytest.raises(AssetFetchError, match="non-JSON body") as ei:
        _getter(name)()
    assert ei.value.__context__ is None


@pytest.mark.parametrize("name", ["statcast", "torvik"])
def test_text_getters_raise_on_a_json_labelled_body_that_will_not_decode(monkeypatch, name):
    # The decode failure used to fall through to the text branch, which parsed to an empty frame.
    _serve(monkeypatch, 200, "<html>Service Unavailable</html>", "application/json")
    with pytest.raises(AssetFetchError, match="non-JSON body") as ei:
        _getter(name)()
    assert ei.value.__context__ is None


@pytest.mark.parametrize("name", ["statcast", "torvik"])
def test_text_getters_keep_csv_text_and_json(monkeypatch, name):
    _serve(monkeypatch, 200, "team,conf\nDuke,ACC\n", "text/csv")
    assert _getter(name)() == "team,conf\nDuke,ACC\n"
    _serve(monkeypatch, 200, '{"data": [1]}', "application/json; charset=utf-8")
    assert _getter(name)() == {"data": [1]}


# --------------------------------------------------------------------------------------
# on3's Next.js data route: a 404 first means "buildId rotated", then means "absent"
# --------------------------------------------------------------------------------------

_ON3 = "https://www.on3.com/rivals/rankings/player/football/2026.json"


def _on3(monkeypatch, page: Callable[[int], Answer], data: Callable[[str], Answer]) -> List[str]:
    from sportsdataverse.cfb import on3_runtime

    monkeypatch.setattr(on3_runtime, "_build_id", None)
    pages = iter(range(100))

    def answer(url: str) -> Answer:
        return page(next(pages)) if "/db/rankings/" in url else data(url)

    return _route(monkeypatch, answer)


def _page(build_id: str) -> Answer:
    return 200, f'<script id="__NEXT_DATA__">{{"buildId":"{build_id}"}}</script>', "text/html"


def test_on3_refreshes_a_rotated_build_id_then_reads(monkeypatch):
    from sportsdataverse.cfb import on3_runtime

    _on3(
        monkeypatch,
        lambda n: _page("old" if n == 0 else "new"),
        lambda url: (200, '{"pageProps": {"ok": 1}}', "application/json") if "/new/" in url else (404, "", "text/html"),
    )
    assert on3_runtime._scrape_get(_ON3, num_retries=0) == {"pageProps": {"ok": 1}}


def test_on3_second_404_after_a_refresh_is_no_data(monkeypatch):
    from sportsdataverse.cfb import on3_runtime

    calls = _on3(monkeypatch, lambda n: _page("old" if n == 0 else "new"), lambda url: (404, "", "text/html"))
    with pytest.raises(NoDataError):
        on3_runtime._scrape_get(_ON3, num_retries=0)
    assert sum("/_next/data/" in c for c in calls) == 2


def test_on3_404_with_an_unchanged_build_id_is_no_data(monkeypatch):
    from sportsdataverse.cfb import on3_runtime

    calls = _on3(monkeypatch, lambda n: _page("same"), lambda url: (404, "", "text/html"))
    with pytest.raises(NoDataError):
        on3_runtime._scrape_get(_ON3, num_retries=0)
    assert sum("/_next/data/" in c for c in calls) == 1


def test_on3_challenge_page_without_a_build_id_is_a_failed_fetch(monkeypatch):
    from sportsdataverse.cfb import on3_runtime

    _on3(monkeypatch, lambda n: (200, "<html>Just a moment...</html>", "text/html"), lambda url: (500, "", "text/html"))
    with pytest.raises(AssetFetchError, match="no Next.js buildId"):
        on3_runtime._scrape_get(_ON3, num_retries=0)


def test_on3_page_failure_is_a_failed_fetch(monkeypatch):
    from sportsdataverse.cfb import on3_runtime

    _on3(monkeypatch, lambda n: (403, "<html>Forbidden</html>", "text/html"), lambda url: (500, "", "text/html"))
    with pytest.raises(AssetFetchError, match="HTTP 403"):
        on3_runtime._scrape_get(_ON3, num_retries=0)


# --------------------------------------------------------------------------------------
# (status, text) transports: 247Sports (curl_cffi) and the LEGACY PFF premium runtime
# --------------------------------------------------------------------------------------


def _tuple_transport(status: int, text: str) -> Callable[..., Tuple[int, str]]:
    return lambda *a, **k: (status, text)


def _tuple_calls(t: Callable[..., Any]) -> List[Callable[[], Any]]:
    from sportsdataverse.cfb import sports247_runtime, sports247_site_pages_runtime
    from sportsdataverse.nfl import pff_runtime

    return [
        lambda: sports247_runtime._get("https://ipa.247sports.com/rdb/v1/x/", transport=t, auth=False),
        lambda: sports247_site_pages_runtime._get("https://247sports.com/Institution/1.json", transport=t),
        lambda: pff_runtime._get("https://premium.pff.com/api/v1/x", {}, cookies={"_premium_key": "PK"}, transport=t),
    ]


@pytest.mark.parametrize(
    ("status", "text", "err"),
    [
        (404, "", NoDataError),
        (400, '{"message": "bad"}', ValueError),
        (422, "", ValueError),
        (403, "Forbidden", AssetFetchError),
        (500, "{}", AssetFetchError),
        (200, "<html>", AssetFetchError),
        (200, "", AssetFetchError),
    ],
)
def test_tuple_transport_getters(status, text, err):
    for call in _tuple_calls(_tuple_transport(status, text)):
        with pytest.raises(err):
            call()


def test_tuple_transport_failure_is_a_failed_fetch():
    def down(*a: Any, **k: Any) -> Tuple[int, str]:
        raise TimeoutError("curl: (28) Operation timed out")

    for call in _tuple_calls(down):
        with pytest.raises(AssetFetchError, match="fetch failed after retries: TimeoutError"):
            call()


def test_tuple_transport_getters_keep_json_and_no_content():
    for call in _tuple_calls(_tuple_transport(200, '{"Key": 1}')):
        assert call() == {"Key": 1}
    for call in _tuple_calls(_tuple_transport(204, "")):
        assert call() == {}


def test_247_refused_after_a_failed_remint_is_a_failed_fetch(monkeypatch):
    from sportsdataverse.cfb import sports247_runtime as rt

    monkeypatch.setattr(rt, "_jwt", "expired")
    monkeypatch.setattr(rt, "_mint_guest_jwt", lambda *a, **k: None)
    with pytest.raises(AssetFetchError, match="HTTP 401"):
        rt._get("https://ipa.247sports.com/rdb/v1/x/", transport=_tuple_transport(401, '{"message": "expired"}'))


def test_the_two_errors_stay_distinct():
    assert not issubclass(NoDataError, AssetFetchError)
    assert not issubclass(AssetFetchError, NoDataError)
