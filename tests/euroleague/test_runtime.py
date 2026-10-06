"""The EuroLeague API answers XML unless asked for JSON; the family runtime must always ask."""

from __future__ import annotations

from sportsdataverse.euroleague import euroleague_runtime as rt


def test_runtime_get_sends_accept_json(monkeypatch):
    seen: dict = {}

    def fake(url, params=None, **kw):
        seen.update(kw)
        return {}

    monkeypatch.setattr(rt._rt, "_get", fake)
    rt._get("https://api-live.euroleague.net/v2/competitions")
    assert seen["headers"]["Accept"] == "application/json"


def test_caller_headers_merge_without_losing_accept(monkeypatch):
    seen: dict = {}
    monkeypatch.setattr(rt._rt, "_get", lambda u, p=None, **kw: seen.update(kw))
    rt._get("u", headers={"X-Test": "1"})
    assert seen["headers"] == {"Accept": "application/json", "X-Test": "1"}
