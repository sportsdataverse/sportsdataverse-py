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

    class _Resp:
        status_code = 500
        text = ""

    monkeypatch.setattr(client.requests, "get", lambda *a, **k: _Resp())
    monkeypatch.setattr(client.time, "sleep", lambda *_a, **_k: None)  # no real waiting
    warned = {}
    import sportsdataverse._codegen_runtime as rt

    monkeypatch.setattr(rt, "cli_warn", lambda msg: warned.setdefault("msg", msg))
    out = client.hockeytech_api("pwhl", "modulekit", "seasons", {}, max_retries=2)
    assert out is None
    assert "HTTP 500" in warned.get("msg", "")
