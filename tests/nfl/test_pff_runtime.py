"""Offline tests for the PFF runtime storage_state auth tier.

Every test runs with no Playwright install and no live session: the headless
browser refresh is injected (``storage_state_refresher=`` / a monkeypatched
``_playwright_refresh``) and the HTTP call via ``transport=``.
"""

import sys

import pytest

from sportsdataverse.nfl import pff_runtime


@pytest.fixture(autouse=True)
def _clean_pff_env(monkeypatch):
    """Strip ambient PFF env + reset the refresh cache around each test."""
    for var in (
        "SDV_PY_PFF_PREMIUM_KEY",
        "SDV_PY_PFF_SESSION",
        "SDV_PY_PFF_COOKIES",
        "SDV_PY_PFF_STORAGE_STATE",
        "SDV_PY_PFF_STORAGE_STATE_TTL",
    ):
        monkeypatch.delenv(var, raising=False)
    pff_runtime._reset_storage_state_cache()
    yield
    pff_runtime._reset_storage_state_cache()


def _boom(_path):
    pytest.fail("storage_state refresher should not have been consulted")


def test_explicit_cookies_win_over_storage_state(monkeypatch):
    monkeypatch.setenv("SDV_PY_PFF_STORAGE_STATE", "state.json")
    monkeypatch.setattr(pff_runtime, "_playwright_refresh", _boom)
    assert pff_runtime._resolve_cookies({"_premium_key": "PK"}) == {"_premium_key": "PK"}


def test_env_cookies_win_over_storage_state(monkeypatch):
    monkeypatch.setenv("SDV_PY_PFF_PREMIUM_KEY", "PK")
    monkeypatch.setenv("SDV_PY_PFF_STORAGE_STATE", "state.json")
    monkeypatch.setattr(pff_runtime, "_playwright_refresh", _boom)
    assert pff_runtime._resolve_cookies(None) == {"_premium_key": "PK"}


def test_storage_state_tier_refreshes_when_no_other_cookies(monkeypatch):
    monkeypatch.setenv("SDV_PY_PFF_STORAGE_STATE", "state.json")
    fresh = {"_premium_key": "PK2", "__session": "JWT"}
    out = pff_runtime._resolve_cookies(None, storage_state_refresher=lambda _p: dict(fresh))
    assert out == fresh


def test_storage_state_cache_one_refresh_within_ttl():
    calls = {"n": 0}

    def refresher(_path):
        calls["n"] += 1
        return {"_premium_key": f"PK{calls['n']}"}

    clock = lambda: 1000.0  # noqa: E731 - fixed clock keeps both calls inside the TTL
    c1 = pff_runtime._cookies_from_storage_state("s.json", refresher=refresher, _clock=clock)
    c2 = pff_runtime._cookies_from_storage_state("s.json", refresher=refresher, _clock=clock)
    assert calls["n"] == 1  # one browser launch, second served from cache
    assert c1 == c2 == {"_premium_key": "PK1"}


def test_storage_state_cache_expires_after_ttl(monkeypatch):
    monkeypatch.setenv("SDV_PY_PFF_STORAGE_STATE_TTL", "60")
    calls = {"n": 0}
    now = {"t": 0.0}

    def refresher(_path):
        calls["n"] += 1
        return {"_premium_key": f"PK{calls['n']}"}

    pff_runtime._cookies_from_storage_state("s.json", refresher=refresher, _clock=lambda: now["t"])
    now["t"] = 61.0  # past the 60s TTL
    c2 = pff_runtime._cookies_from_storage_state("s.json", refresher=refresher, _clock=lambda: now["t"])
    assert calls["n"] == 2
    assert c2 == {"_premium_key": "PK2"}


def test_empty_refresh_falls_through_to_runtimeerror(monkeypatch):
    monkeypatch.setenv("SDV_PY_PFF_STORAGE_STATE", "state.json")
    with pytest.raises(RuntimeError, match="SDV_PY_PFF_STORAGE_STATE"):
        pff_runtime._resolve_cookies(None, storage_state_refresher=lambda _p: {})


def test_no_auth_at_all_raises_runtimeerror():
    with pytest.raises(RuntimeError, match="paywalled"):
        pff_runtime._resolve_cookies(None)


def test_missing_playwright_raises_clear_importerror(monkeypatch):
    # Force `from playwright.sync_api import sync_playwright` to raise ImportError,
    # regardless of whether playwright is installed in the test env.
    monkeypatch.setitem(sys.modules, "playwright", None)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", None)
    with pytest.raises(ImportError, match=r"sportsdataverse\[pff\]"):
        pff_runtime._playwright_refresh("state.json")


def test_get_uses_storage_state_cookies_end_to_end(monkeypatch):
    import json

    monkeypatch.setenv("SDV_PY_PFF_STORAGE_STATE", "state.json")
    monkeypatch.setattr(pff_runtime, "_playwright_refresh", lambda _p: {"_premium_key": "PK", "__session": "JWT"})
    seen = {}

    def fake_transport(url, params, headers, cookies):
        seen["cookies"] = cookies
        return 200, json.dumps({"ok": True})

    body = pff_runtime._get("https://premium.pff.com/api/v1/leagues", {}, transport=fake_transport)
    assert body == {"ok": True}
    assert seen["cookies"] == {"_premium_key": "PK", "__session": "JWT"}
