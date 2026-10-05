import sys

import pytest

from sportsdataverse.errors import EmptyResponseWarning
from sportsdataverse.nba.nba_stats_runtime import _get, stats_headers


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


def test_get_returns_empty_dict_on_non_200():
    with pytest.warns(EmptyResponseWarning, match=r"stats/x answered HTTP 500.*Season"):
        out = _get("x", {}, transport=lambda *a: (500, "boom"))
    assert out == {}


def test_get_returns_empty_dict_on_blank_body():
    with pytest.warns(EmptyResponseWarning, match="HTTP 200 with an empty body"):
        out = _get("x", {}, transport=lambda *a: (200, "   "))
    assert out == {}


def test_get_names_a_non_json_body():
    with pytest.warns(EmptyResponseWarning, match="HTTP 200 with a body that is not JSON"):
        assert _get("x", {}, transport=lambda *a: (200, "<html>blocked</html>")) == {}


def test_a_full_url_names_its_own_host():
    seen = {}

    def transport(url, params, headers, proxy_url):
        seen.update(headers)
        return 500, ""

    # the host= keyword keeps its stats.nba.com default; the URL is stats.wnba.com
    with pytest.warns(EmptyResponseWarning, match="stats.wnba.com answers this way"):
        _get("https://stats.wnba.com/stats/x", {}, transport=transport)
    assert seen["Host"] == "stats.wnba.com"


def test_wnba_warning_names_its_host_and_points_at_the_caller():
    from sportsdataverse.wnba.wnba_stats_runtime import _get as wnba_get

    def wrapper():  # stands in for a generated wnba_stats_* wrapper
        return wnba_get("x", {}, transport=lambda *a: (500, ""))

    with pytest.warns(EmptyResponseWarning, match="stats.wnba.com answers this way") as rec:
        line = sys._getframe().f_lineno + 1
        wrapper()
    # The warning points at the wrapper's caller, past the WNBA shim's extra frame.
    assert (rec[0].filename, rec[0].lineno) == (__file__, line)


def test_get_does_not_warn_on_data(recwarn):
    _get("x", {}, transport=lambda *a: (200, '{"resultSets": []}'))
    assert not [w for w in recwarn if issubclass(w.category, EmptyResponseWarning)]


def test_get_passes_full_url_through():
    captured = {}

    def transport(url, params, headers, proxy_url):
        captured["url"] = url
        return 200, '{"resultSets": []}'

    _get("https://stats.nba.com/stats/leaguedashplayerstats", {}, transport=transport)
    assert captured["url"] == "https://stats.nba.com/stats/leaguedashplayerstats"


def test_wnba_runtime_fixes_host():
    from sportsdataverse.wnba.wnba_stats_runtime import _get as wget

    captured = {}

    def transport(url, params, headers, proxy_url):
        captured["url"] = url
        captured["host_header"] = headers.get("Host")
        return 200, '{"resultSets": []}'

    wget("leaguedashplayerstats", {}, transport=transport)
    assert captured["url"] == "https://stats.wnba.com/stats/leaguedashplayerstats"
    assert captured["host_header"] == "stats.wnba.com"


# --- retry / timeout tunables (SDV_PY_NBA_STATS_RETRIES / _TIMEOUT / _BACKOFF) ---


def test_get_no_retry_by_default(monkeypatch):
    monkeypatch.delenv("SDV_PY_NBA_STATS_RETRIES", raising=False)
    calls = {"n": 0}

    def transport(url, params, headers, proxy_url):
        calls["n"] += 1
        return 200, "{}"  # blank envelope

    with pytest.warns(EmptyResponseWarning):
        assert _get("gamerotation", {}, transport=transport) == {}
    assert calls["n"] == 1  # single shot, no retry


def test_get_retries_on_empty_then_succeeds(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "3")
    monkeypatch.setenv("SDV_PY_NBA_STATS_BACKOFF", "0")
    seq = iter([(200, "{}"), (200, "{}"), (200, '{"resultSets": [1]}')])

    def transport(url, params, headers, proxy_url):
        return next(seq)

    out = _get("gamerotation", {}, transport=transport)
    assert out == {"resultSets": [1]}  # recovered on the 3rd attempt


def test_get_retries_on_exception_then_succeeds(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "2")
    monkeypatch.setenv("SDV_PY_NBA_STATS_BACKOFF", "0")
    calls = {"n": 0}

    def transport(url, params, headers, proxy_url):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("curl 28")
        return 200, '{"resultSets": [1]}'

    assert _get("gamerotation", {}, transport=transport) == {"resultSets": [1]}
    assert calls["n"] == 2


def test_get_exhausted_exception_reraises(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "2")
    monkeypatch.setenv("SDV_PY_NBA_STATS_BACKOFF", "0")

    def transport(url, params, headers, proxy_url):
        raise TimeoutError("always hangs")

    import pytest

    with pytest.raises(TimeoutError):
        _get("gamerotation", {}, transport=transport)


def test_get_exhausted_empty_returns_blank(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "2")
    monkeypatch.setenv("SDV_PY_NBA_STATS_BACKOFF", "0")
    calls = {"n": 0}

    def transport(url, params, headers, proxy_url):
        calls["n"] += 1
        return 200, "{}"

    with pytest.warns(EmptyResponseWarning):
        assert _get("gamerotation", {}, transport=transport) == {}
    assert calls["n"] == 3  # 1 + 2 retries, then gives up
