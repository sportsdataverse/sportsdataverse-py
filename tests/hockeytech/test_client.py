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
