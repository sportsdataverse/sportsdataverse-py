"""The family runtime picks the body contract by host.

``api-live.euroleague.net`` answers XML unless asked for JSON, so the runtime must always
ask; ``live.euroleague.net/api`` answers JSON by default and its empty 200 body means "no
such game", which must come back as ``{}`` -- never the shared getter's ``AssetFetchError``.
"""

from __future__ import annotations

import json
from typing import Any, Dict

import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.euroleague import euroleague_runtime as rt


class _Resp:
    """A minimal ``requests.Response`` stand-in for the transport."""

    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code

    def json(self) -> Any:
        return json.loads(self.text)


def test_runtime_get_sends_accept_json(monkeypatch):
    seen: dict = {}

    def fake(url, params=None, **kw):
        seen.update(kw)
        return {}

    monkeypatch.setattr(rt._rt, "_get", fake)
    rt._get("https://api-live.euroleague.net/v2/competitions")
    assert seen["headers"]["Accept"] == "application/json"


def test_v3_host_also_gets_accept_json(monkeypatch):
    seen: dict = {}
    monkeypatch.setattr(rt._rt, "_get", lambda u, p=None, **kw: seen.update(kw))
    rt._get("https://api-live.euroleague.net/v3/competitions/E/seasons/E2025/rounds/1/basicstandings")
    assert seen["headers"] == {"Accept": "application/json"}


def test_caller_headers_merge_without_losing_accept(monkeypatch):
    seen: dict = {}
    monkeypatch.setattr(rt._rt, "_get", lambda u, p=None, **kw: seen.update(kw))
    rt._get("u", headers={"X-Test": "1"})
    assert seen["headers"] == {"Accept": "application/json", "X-Test": "1"}


def _live(monkeypatch: pytest.MonkeyPatch, resp: _Resp) -> Dict[str, Any]:
    """Route the live host through a fake transport; return what it was called with."""
    seen: Dict[str, Any] = {}

    def download(url, **kw):
        seen["url"] = url
        seen.update(kw)
        return resp

    monkeypatch.setattr(rt._rt, "download", download)
    return seen


def test_live_host_sends_no_accept_header_and_strips_none_params(monkeypatch):
    seen = _live(monkeypatch, _Resp('{"Rows": []}'))
    out = rt._get(f"{rt.LIVE_HOST}/Points", {"gamecode": 1, "seasoncode": "E2025", "x": None})
    assert out == {"Rows": []}
    assert seen["url"] == "https://live.euroleague.net/api/Points"
    assert seen["params"] == {"gamecode": 1, "seasoncode": "E2025"}
    assert "headers" not in seen


def test_live_empty_200_body_is_the_no_such_game_sentinel(monkeypatch):
    """``/Points?gamecode=9999`` answers 200 with an empty body -> ``{}``, not ``AssetFetchError``."""
    _live(monkeypatch, _Resp(""))
    assert rt._get(f"{rt.LIVE_HOST}/Points", {"gamecode": 9999, "seasoncode": "E2025"}) == {}


def test_live_host_keeps_the_error_vocabulary(monkeypatch):
    _live(monkeypatch, _Resp("", 404))
    with pytest.raises(NoDataError):
        rt._get(f"{rt.LIVE_HOST}/Header", {"gamecode": 1, "seasoncode": "E2025"})
    _live(monkeypatch, _Resp("<html>blocked</html>", 403))
    with pytest.raises(AssetFetchError):
        rt._get(f"{rt.LIVE_HOST}/Header", {"gamecode": 1, "seasoncode": "E2025"})
    _live(monkeypatch, _Resp("<html>not json</html>", 200))
    with pytest.raises(AssetFetchError):
        rt._get(f"{rt.LIVE_HOST}/Header", {"gamecode": 1, "seasoncode": "E2025"})


def test_api_live_empty_200_body_still_raises(monkeypatch):
    """Only the live host softens an empty 200; on api-live it stays a failed fetch."""
    monkeypatch.setattr(rt._rt, "download", lambda url, **kw: _Resp(""))
    with pytest.raises(AssetFetchError):
        rt._get("https://api-live.euroleague.net/v2/competitions")
