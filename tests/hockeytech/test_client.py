from __future__ import annotations


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


def test_hockeytech_api_bad_status_returns_none_and_warns(monkeypatch):
    import sportsdataverse.hockeytech._client as client

    # Since Fix 4, hockeytech_api routes HTTP through dl_utils.download.
    # Monkeypatch download to raise so the except-branch fires and cli_warn is called.
    def _failing_download(*a, **k):
        raise RuntimeError("HTTP 500")

    monkeypatch.setattr(client, "download", _failing_download)
    monkeypatch.setattr(client.time, "sleep", lambda *_a, **_k: None)  # no real waiting
    warned = {}
    import sportsdataverse._codegen_runtime as rt

    monkeypatch.setattr(rt, "cli_warn", lambda msg: warned.setdefault("msg", msg))
    out = client.hockeytech_api("pwhl", "modulekit", "seasons", {}, max_retries=2)
    assert out is None
    assert "HTTP 500" in warned.get("msg", "")


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


def test_hockeytech_api_warns_on_invalid_view_sentinel(monkeypatch):
    """A sentinel must warn -- silently returning an empty frame hides a dead view."""
    import json

    from sportsdataverse.hockeytech import _client as client

    class _Resp:
        text = json.dumps(_MODULEKIT_STREAKS_SENTINEL)

    monkeypatch.setattr(client, "download", lambda *a, **k: _Resp())
    monkeypatch.setattr(client.time, "sleep", lambda *_a, **_k: None)
    warned = {}
    import sportsdataverse._codegen_runtime as rt

    monkeypatch.setattr(rt, "cli_warn", lambda msg: warned.setdefault("msg", msg))

    out = client.hockeytech_api("pwhl", "modulekit", "streaks", {"league_id": 1})
    # payload still returned unchanged (parsers keep yielding a zero-row frame)
    assert out == _MODULEKIT_STREAKS_SENTINEL
    msg = warned.get("msg", "")
    assert "Undefined Tab streaks" in msg
    assert "NOT an empty result" in msg


def test_hockeytech_api_silent_on_healthy_payload(monkeypatch):
    import json

    from sportsdataverse.hockeytech import _client as client

    class _Resp:
        text = json.dumps({"SiteKit": {"Seasons": [{"season_id": "1"}]}})

    monkeypatch.setattr(client, "download", lambda *a, **k: _Resp())
    monkeypatch.setattr(client.time, "sleep", lambda *_a, **_k: None)
    warned = {}
    import sportsdataverse._codegen_runtime as rt

    monkeypatch.setattr(rt, "cli_warn", lambda msg: warned.setdefault("msg", msg))

    client.hockeytech_api("pwhl", "modulekit", "seasons", {})
    assert not warned, f"healthy payload must not warn, got {warned}"
