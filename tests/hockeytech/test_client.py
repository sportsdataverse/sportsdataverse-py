from __future__ import annotations

import pathlib
import traceback

import pytest
import requests

from sportsdataverse.errors import AssetFetchError, NoDataError


def test_strip_jsonp_angular_callback():
    from sportsdataverse.hockeytech._client import _strip_jsonp

    assert _strip_jsonp('angular.callbacks._8([{"a":1}])') == '[{"a":1}]'


def test_strip_jsonp_bare_parens():
    from sportsdataverse.hockeytech._client import _strip_jsonp

    assert _strip_jsonp('({"a":1})') == '{"a":1}'


def test_strip_jsonp_passthrough_plain_json():
    from sportsdataverse.hockeytech._client import _strip_jsonp

    assert _strip_jsonp('{"a":1}') == '{"a":1}'


def test_build_url_includes_key_client_code_and_feed():
    from sportsdataverse.hockeytech._client import _build_url

    url = _build_url("pwhl", feed="modulekit", view="seasons", params={"site_id": "0"})
    assert url.startswith("https://lscluster.hockeytech.com/feed/index.php?")
    assert "feed=modulekit" in url and "view=seasons" in url
    assert "key=446521baf8c38984" in url and "client_code=pwhl" in url


def test_build_url_gc_feed_uses_tab_not_view():
    from sportsdataverse.hockeytech._client import _build_url

    url = _build_url("pwhl", feed="gc", view="gamesummary", params={"game_id": 42})
    assert "tab=gamesummary" in url
    assert "view=" not in url
    assert "feed=gc" in url


# ---------------------------------------------------------------------------
# Issue #238: HockeyTech reports an unknown view with HTTP 200 + error-in-body,
# so a sentinel response is otherwise indistinguishable from "no data".
# Payload shapes below are the real captures committed under
# sdv-internal-refs/hockeytech/captures/samples/pwhl/{streaks,svf_streaks}.json
# ---------------------------------------------------------------------------

_MODULEKIT_STREAKS_SENTINEL = {
    "SiteKit": {
        "Parameters": {"feed": "modulekit", "client_code": "pwhl", "view": "streaks"},
        "Undefined": "Undefined Tab streaks",
    }
}
_STATVIEWFEED_STREAKS_SENTINEL = {"error": "InvalidView error: streaks"}


def test_invalid_view_reason_detects_both_sentinel_shapes():
    from sportsdataverse.hockeytech._client import _invalid_view_reason

    assert _invalid_view_reason(_MODULEKIT_STREAKS_SENTINEL) == "Undefined Tab streaks"
    assert _invalid_view_reason(_STATVIEWFEED_STREAKS_SENTINEL) == "InvalidView error: streaks"
    # gc feed nests under "GC"
    assert _invalid_view_reason({"GC": {"Undefined": "Undefined Tab bogus"}}) == "Undefined Tab bogus"


def test_invalid_view_reason_passes_healthy_payloads():
    from sportsdataverse.hockeytech._client import _invalid_view_reason

    assert _invalid_view_reason({"SiteKit": {"Seasons": [{"season_id": "1"}]}}) is None
    assert _invalid_view_reason({"SiteKit": {"Streaks": []}}) is None  # genuinely empty != sentinel
    assert _invalid_view_reason([]) is None
    assert _invalid_view_reason(None) is None
    assert _invalid_view_reason({"error": ""}) is None  # empty string is not a reason


def test_invalid_view_reason_any_top_level_error_string():
    """Like sportsdataverse-js: any non-empty top-level ``error`` is a sentinel, not just InvalidView."""
    from sportsdataverse.hockeytech._client import _invalid_view_reason

    assert _invalid_view_reason({"error": "Invalid season"}) == "Invalid season"


# ---------------------------------------------------------------------------
# Error vocabulary (PY-4): NoDataError = the fetch succeeded and nothing is
# there; AssetFetchError = the fetch failed, answer unknown. Each branch is
# driven through the real ``dl_utils.download`` with the HTTP session faked,
# serving recorded bodies from tests/fixtures/hockeytech/.
# ---------------------------------------------------------------------------

_FIX = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "hockeytech"


def _serve(monkeypatch, status, body):
    """Answer every HTTP GET with ``status`` + ``body``; return the list of URLs hit."""
    from sportsdataverse import dl_utils
    from sportsdataverse.hockeytech import _client as client

    calls = []

    def fake_get(url, **_k):
        calls.append(url)
        r = requests.Response()
        r.status_code = status
        r._content = body.encode("utf-8") if isinstance(body, str) else body
        r.encoding = "utf-8"
        r.url = url
        return r

    monkeypatch.setattr(dl_utils._SHARED_SESSION, "get", fake_get)
    monkeypatch.setattr(dl_utils.time, "sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(client.time, "sleep", lambda *_a, **_k: None)
    return calls


def _call(league="pwhl", feed="modulekit", view="seasons", params=None):
    from sportsdataverse.hockeytech import _client as client

    return client.hockeytech_api(league, feed, view, params or {}, max_retries=1)


def test_healthy_jsonp_body_is_parsed(monkeypatch):
    body = "angular.callbacks._0(" + (_FIX / "pwhl_seasons.json").read_text(encoding="utf-8") + ")"
    _serve(monkeypatch, 200, body)
    out = _call()
    assert out["SiteKit"]["Seasons"][0]["season_name"] == "2026-27 Pre-Season"


def test_access_denied_reply_is_an_empty_payload(monkeypatch):
    """MJHL's public key has no gamecenter access: documented graceful empty."""
    _serve(monkeypatch, 200, (_FIX / "mjhl_gamesummary_7301_access_denied.txt").read_bytes())
    assert _call("mjhl", "gc", "gamesummary", {"game_id": 7301}) == {}


@pytest.mark.parametrize(
    "fixture, needle",
    [
        ("pwhl_streaks_undefined_tab.json", "Undefined Tab streaks"),
        ("pwhl_svf_streaks_invalidview.json", "InvalidView error: streaks"),
    ],
)
def test_error_sentinel_is_a_failed_fetch(monkeypatch, fixture, needle):
    _serve(monkeypatch, 200, (_FIX / fixture).read_bytes())
    with pytest.raises(AssetFetchError, match=needle):
        _call(view="streaks")


@pytest.mark.parametrize("body", ["", "   ", "<html><body>Bad Gateway</body></html>"])
def test_empty_or_unparseable_body_is_a_failed_fetch(monkeypatch, body):
    _serve(monkeypatch, 200, body)
    with pytest.raises(AssetFetchError, match="unparseable"):
        _call()


@pytest.mark.parametrize("status", [403, 429, 500, 503])
def test_failed_status_is_a_failed_fetch_not_none(monkeypatch, status):
    calls = _serve(monkeypatch, status, "Service Unavailable")
    with pytest.raises(AssetFetchError, match=f"HTTP {status}"):
        _call()
    assert len(calls) == 2  # retried once (max_retries=1), then surfaced


def test_404_is_no_data(monkeypatch):
    _serve(monkeypatch, 404, "Not Found")
    with pytest.raises(NoDataError):
        _call()


def test_transport_error_is_a_failed_fetch(monkeypatch):
    from sportsdataverse import dl_utils

    _serve(monkeypatch, 200, "")

    def boom(url, **_k):
        raise requests.exceptions.ConnectTimeout("timed out")

    monkeypatch.setattr(dl_utils._SHARED_SESSION, "get", boom)
    with pytest.raises(AssetFetchError, match="timed out"):
        _call()


def test_unknown_league_raises_before_any_request(monkeypatch):
    calls = _serve(monkeypatch, 200, "{}")
    with pytest.raises(ValueError, match="Unknown HockeyTech league"):
        _call(league="nhl")
    assert calls == []


def test_error_messages_do_not_carry_the_key(monkeypatch):
    _serve(monkeypatch, 503, "x")
    with pytest.raises(AssetFetchError) as ei:
        _call()
    assert "446521baf8c38984" not in str(ei.value)


def _rendered(err):
    """Everything a traceback would print for ``err``, causes and contexts included."""
    return "".join(traceback.format_exception(type(err), err, err.__traceback__))


def test_transport_error_text_never_carries_the_key(monkeypatch):
    """requests quotes the URL (and so the key) in a ConnectionError; it must not reach the caller."""
    from sportsdataverse import dl_utils

    secret = "SECRETKEY12345678"
    monkeypatch.setenv("SDV_PWHL_API_KEY", secret)
    _serve(monkeypatch, 200, "")

    def refuse(url, **_k):
        path = url.split("lscluster.hockeytech.com", 1)[1]
        raise requests.exceptions.ConnectionError(
            "HTTPSConnectionPool(host='lscluster.hockeytech.com', port=443): Max retries exceeded "
            f"with url: {path} (Caused by NewConnectionError('Failed to establish a new connection'))"
        )

    monkeypatch.setattr(dl_utils._SHARED_SESSION, "get", refuse)
    with pytest.raises(AssetFetchError) as ei:
        _call()
    err = ei.value
    assert "Max retries exceeded" in str(err) and "key=REDACTED" in str(err)
    assert secret not in _rendered(err)
    assert err.__cause__ is None and err.__context__ is None


def test_404_text_never_carries_the_key(monkeypatch):
    secret = "SECRETKEY12345678"
    monkeypatch.setenv("SDV_PWHL_API_KEY", secret)
    _serve(monkeypatch, 404, "Not Found")
    with pytest.raises(NoDataError) as ei:
        _call()
    assert secret not in _rendered(ei.value)
    assert ei.value.__context__ is None
