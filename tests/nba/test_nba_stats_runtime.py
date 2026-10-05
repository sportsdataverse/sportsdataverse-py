import sys
import warnings
from pathlib import Path

import pytest

from sportsdataverse.errors import AssetFetchError, EmptyResponseWarning, NoDataError
from sportsdataverse.nba.nba_stats_runtime import _get, stats_headers
from sportsdataverse.wnba.wnba_stats_runtime import _get as wnba_get

# Real stats.nba.com error bodies; provenance in the README beside them.
FIX = Path(__file__).resolve().parents[1] / "fixtures" / "runtime_errors"
GETTERS = pytest.mark.parametrize("get", [_get, wnba_get], ids=["nba", "wnba"])


@pytest.fixture(autouse=True)
def _single_shot(monkeypatch):
    """One attempt unless a test opts in, and no real backoff sleeps."""
    monkeypatch.delenv("SDV_PY_NBA_STATS_RETRIES", raising=False)
    monkeypatch.setenv("SDV_PY_NBA_STATS_BACKOFF", "0")


def _answers(*responses):
    """Transport yielding *responses* in order (a (status, text) tuple or an exception), repeating the last."""
    calls = {"n": 0}

    def transport(url, params, headers, proxy_url):
        calls["n"] += 1
        item = responses[min(calls["n"], len(responses)) - 1]
        if isinstance(item, BaseException):
            raise item
        return item

    return transport, calls


def test_stats_headers_token_and_host():
    h = stats_headers("stats.nba.com")
    assert h["x-nba-stats-token"] == "true" and h["Host"] == "stats.nba.com"
    assert "nba.com" in h["Referer"]
    # curl_cffi's impersonation supplies a User-Agent that matches its client hints.
    assert "User-Agent" not in h


def test_get_builds_url_strips_none_and_pads_gameid():
    captured = {}

    def transport(url, params, headers, proxy_url):
        captured["url"] = url
        captured["params"] = params
        return 200, '{"resultSets": [{"name": "X", "headers": ["A"], "rowSet": [[1]]}]}'

    out = _get(
        "leaguedashplayerstats",
        {"LeagueID": "00", "Empty": None, "GameID": "61"},
        transport=transport,
    )
    assert captured["url"] == "https://stats.nba.com/stats/leaguedashplayerstats"
    assert "Empty" not in captured["params"]
    assert captured["params"]["GameID"] == "0000000061"
    assert out["resultSets"][0]["name"] == "X"


# --- the error vocabulary: every answer, both runtimes ---


@GETTERS
def test_200_json_is_returned_without_a_warning(get, recwarn):
    assert get("x", {}, transport=lambda *a: (200, '{"resultSets": []}')) == {"resultSets": []}
    assert not [w for w in recwarn if issubclass(w.category, EmptyResponseWarning)]


@GETTERS
@pytest.mark.parametrize("status", [204, 205])
def test_no_content_is_empty_and_warns(get, status):
    with pytest.warns(EmptyResponseWarning, match=f"HTTP {status} with an empty body"):
        assert get("x", {}, transport=lambda *a: (status, "")) == {}


@GETTERS
def test_a_bare_empty_object_is_empty_and_warns(get):
    with pytest.warns(EmptyResponseWarning, match="HTTP 200 with an empty object"):
        assert get("x", {}, transport=lambda *a: (200, "{}")) == {}


@GETTERS
def test_404_is_no_data(get):
    body = (FIX / "nba_stats_404_unknown_endpoint.html").read_text(encoding="utf-8")
    with pytest.raises(NoDataError, match=r"/stats/notarealendpoint answered HTTP 404"):
        get("notarealendpoint", {"LeagueID": "00"}, transport=lambda *a: (404, body))


@GETTERS
@pytest.mark.parametrize("status", [400, 422])
def test_400_and_422_are_value_errors(get, status):
    body = (FIX / "nba_stats_400_bad_measuretype.json").read_text(encoding="utf-8")
    with pytest.raises(ValueError, match=rf"rejected the request: HTTP {status}: .*Parameter must be valid") as ei:
        get("leaguedashplayerstats", {"MeasureType": "Bogus"}, transport=lambda *a: (status, body))
    assert not isinstance(ei.value, AssetFetchError)


@GETTERS
@pytest.mark.parametrize("status", [401, 403, 429, 500, 502, 503])
def test_a_failed_status_is_asset_fetch_error(get, status):
    with pytest.raises(AssetFetchError, match=rf"/stats/x answered HTTP {status}: <html>Access Denied") as ei:
        get("x", {"Season": "2024-25"}, transport=lambda *a: (status, "<html>Access Denied</html>"))
    assert "Season" not in str(ei.value)  # host + path only, never the query string


@GETTERS
def test_the_missing_season_empty_500_is_asset_fetch_error(get):
    # stats.nba.com answers a request without a required Season with an EMPTY 500
    # (captured: 0 bytes, see the README). A failed fetch, never {} plus a warning.
    host = "stats.wnba.com" if get is wnba_get else "stats.nba.com"
    with warnings.catch_warnings():
        warnings.simplefilter("error", EmptyResponseWarning)
        with pytest.raises(AssetFetchError, match=rf"{host}/stats/leaguedashplayerstats answered HTTP 500"):
            get("leaguedashplayerstats", {"LeagueID": "00"}, transport=lambda *a: (500, ""))


def test_a_wrapper_raises_instead_of_parsing_a_failed_fetch_to_an_empty_frame():
    from sportsdataverse.nba.nba_stats import nba_stats_leaguedashplayerstats

    with pytest.raises(AssetFetchError, match="leaguedashplayerstats answered HTTP 500"):
        nba_stats_leaguedashplayerstats(season="2024-25", transport=lambda *a: (500, ""))


@GETTERS
@pytest.mark.parametrize(
    ("body", "why"), [("   ", "with an empty body"), ("<html>blocked</html>", "with a non-JSON body")]
)
def test_an_unusable_200_is_asset_fetch_error(get, body, why):
    with pytest.raises(AssetFetchError, match=f"answered HTTP 200 {why}") as ei:
        get("x", {}, transport=lambda *a: (200, body))
    assert ei.value.__context__ is None  # the JSONDecodeError (whole body) is never chained


@GETTERS
def test_a_connection_error_is_asset_fetch_error(get):
    transport, _ = _answers(ConnectionResetError("reset by peer"))
    with pytest.raises(AssetFetchError, match="/stats/x: fetch failed after retries: ConnectionResetError") as ei:
        get("x", {}, transport=transport)
    assert isinstance(ei.value.__cause__, ConnectionResetError)


def test_a_curl_cffi_error_is_asset_fetch_error():
    curl_exc = pytest.importorskip("curl_cffi.requests.exceptions")
    transport, _ = _answers(curl_exc.Timeout("curl: (28) Operation timed out"))
    with pytest.raises(AssetFetchError, match="fetch failed after retries: Timeout"):
        _get("x", {}, transport=transport)


def test_a_missing_curl_cffi_is_neither_masked_nor_retried(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "3")
    transport, calls = _answers(ImportError("Live stats.nba.com calls require curl_cffi"))
    with pytest.raises(ImportError, match="curl_cffi"):
        _get("x", {}, transport=transport)
    assert calls["n"] == 1


# --- host and caller in the EmptyResponseWarning ---


def test_a_full_url_names_its_own_host():
    seen = {}

    def transport(url, params, headers, proxy_url):
        seen.update(headers)
        return 200, "{}"

    # the host= keyword keeps its stats.nba.com default; the URL is stats.wnba.com
    with pytest.warns(EmptyResponseWarning, match="stats.wnba.com answers this way"):
        _get("https://stats.wnba.com/stats/x", {}, transport=transport)
    assert seen["Host"] == "stats.wnba.com"


def test_wnba_warning_names_its_host_and_points_at_the_caller():
    def wrapper():  # stands in for a generated wnba_stats_* wrapper
        return wnba_get("x", {}, transport=lambda *a: (200, "{}"))

    with pytest.warns(EmptyResponseWarning, match="stats.wnba.com answers this way") as rec:
        line = sys._getframe().f_lineno + 1
        wrapper()
    # The warning points at the wrapper's caller, past the WNBA shim's extra frame.
    assert (rec[0].filename, rec[0].lineno) == (__file__, line)


def test_get_passes_full_url_through():
    captured = {}

    def transport(url, params, headers, proxy_url):
        captured["url"] = url
        return 200, '{"resultSets": []}'

    _get("https://stats.nba.com/stats/leaguedashplayerstats", {}, transport=transport)
    assert captured["url"] == "https://stats.nba.com/stats/leaguedashplayerstats"


def test_wnba_runtime_fixes_host():
    captured = {}

    def transport(url, params, headers, proxy_url):
        captured["url"] = url
        captured["host_header"] = headers.get("Host")
        return 200, '{"resultSets": []}'

    wnba_get("leaguedashplayerstats", {}, transport=transport)
    assert captured["url"] == "https://stats.wnba.com/stats/leaguedashplayerstats"
    assert captured["host_header"] == "stats.wnba.com"


# --- retry / timeout tunables (SDV_PY_NBA_STATS_RETRIES / _TIMEOUT / _BACKOFF) ---


def test_get_no_retry_by_default():
    transport, calls = _answers((500, ""))
    with pytest.raises(AssetFetchError):
        _get("gamerotation", {}, transport=transport)
    assert calls["n"] == 1  # single shot, no retry


def test_get_retries_on_empty_then_succeeds(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "3")
    transport, _ = _answers((200, "{}"), (200, "{}"), (200, '{"resultSets": [1]}'))
    assert _get("gamerotation", {}, transport=transport) == {"resultSets": [1]}  # the 3rd attempt


@pytest.mark.parametrize("failure", [(503, ""), (429, "slow down"), (200, ""), TimeoutError("curl 28")])
def test_get_retries_a_failed_fetch_then_succeeds(monkeypatch, failure):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "2")
    transport, calls = _answers(failure, (200, '{"resultSets": [1]}'))
    assert _get("gamerotation", {}, transport=transport) == {"resultSets": [1]}
    assert calls["n"] == 2


@pytest.mark.parametrize(("status", "exc"), [(404, NoDataError), (400, ValueError), (422, ValueError)])
def test_an_answer_is_never_retried(monkeypatch, status, exc):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "3")
    transport, calls = _answers((status, ""))
    with pytest.raises(exc):
        _get("gamerotation", {}, transport=transport)
    assert calls["n"] == 1


def test_get_exhausted_failure_raises_after_every_attempt(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "2")
    transport, calls = _answers((500, ""))
    with pytest.raises(AssetFetchError, match="answered HTTP 500"):
        _get("gamerotation", {}, transport=transport)
    assert calls["n"] == 3  # 1 + 2 retries, then gives up


def test_get_exhausted_exception_raises_asset_fetch_error(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "2")
    transport, calls = _answers(TimeoutError("always hangs"))
    with pytest.raises(AssetFetchError, match="TimeoutError") as ei:
        _get("gamerotation", {}, transport=transport)
    assert isinstance(ei.value.__cause__, TimeoutError)
    assert calls["n"] == 3


def test_get_exhausted_empty_returns_blank(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "2")
    transport, calls = _answers((200, "{}"))
    with pytest.warns(EmptyResponseWarning):
        assert _get("gamerotation", {}, transport=transport) == {}
    assert calls["n"] == 3  # 1 + 2 retries, then gives up
