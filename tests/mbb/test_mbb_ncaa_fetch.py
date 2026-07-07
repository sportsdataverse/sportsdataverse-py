"""Tests for the NCAA cache-first proxy fetch layer (Phase 5f, Task 5f.1).

Fully offline -- every test injects a fake transport (or an explicit
``proxy_pool``) and never touches the network. The two binding directives
(proxy-only, cache-first) are asserted structurally, not merely assumed:
see :func:`test_no_proxy_configured_raises` and
:func:`test_cache_hit_makes_zero_transport_calls`.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Optional

import pytest

from sportsdataverse.mbb.mbb_ncaa_fetch import (
    NcaaFetchConfig,
    NcaaFetcher,
    cached_path,
    get_config,
    is_cached,
    load_proxybonanza_pool,
    playwright_transport,
    reset_config,
    update_config,
)


class FakeTransport:
    """Records every ``(url, proxies, headers)`` call; replays scripted responses."""

    def __init__(self, responses: "list[tuple[int, str]]") -> None:
        self.responses = list(responses)
        self.calls: "list[tuple[str, dict, dict]]" = []

    def __call__(self, url: str, proxies: dict, headers: dict) -> "tuple[int, str]":
        self.calls.append((url, dict(proxies), dict(headers)))
        if not self.responses:
            raise AssertionError("FakeTransport exhausted -- unexpected extra call")
        return self.responses.pop(0)


def _cfg(
    tmp_path: Path, transport: Optional[FakeTransport] = None, proxy_url: Optional[str] = "http://u:p@1.1.1.1:1"
) -> NcaaFetchConfig:
    return NcaaFetchConfig(cache_dir=tmp_path, proxy_url=proxy_url, transport=transport)


# --- binding directive 1: no direct-fetch mode -----------------------------


def test_no_proxy_configured_raises(tmp_path: Path) -> None:
    cfg = NcaaFetchConfig(cache_dir=tmp_path)  # no proxy_url, no proxybonanza pair
    fetcher = NcaaFetcher(cfg)
    with pytest.raises(RuntimeError, match="proxy"):
        fetcher.fetch_html("contests/1/play_by_play")


# --- binding directive 2: cache-first / fetch-once --------------------------


def test_cache_hit_makes_zero_transport_calls(tmp_path: Path) -> None:
    transport = FakeTransport([(200, "<html>hi</html>")])
    fetcher = NcaaFetcher(_cfg(tmp_path, transport))

    first = fetcher.fetch_html("contests/123/play_by_play")
    assert first == "<html>hi</html>"
    assert len(transport.calls) == 1
    assert cached_path("contests/123/play_by_play", cache_dir=tmp_path).exists()

    second = fetcher.fetch_html("contests/123/play_by_play")
    assert second == first
    assert len(transport.calls) == 1  # no new transport call on cache hit


def test_force_refetch_overwrites_cache(tmp_path: Path) -> None:
    transport = FakeTransport([(200, "v1"), (200, "v2")])
    fetcher = NcaaFetcher(_cfg(tmp_path, transport))

    assert fetcher.fetch_html("contests/1/play_by_play") == "v1"
    assert fetcher.fetch_html("contests/1/play_by_play", force=True) == "v2"
    assert len(transport.calls) == 2
    assert cached_path("contests/1/play_by_play", cache_dir=tmp_path).read_text(encoding="utf-8") == "v2"


def test_is_cached_helper(tmp_path: Path) -> None:
    assert not is_cached("contests/1/play_by_play", cache_dir=tmp_path)
    transport = FakeTransport([(200, "x")])
    NcaaFetcher(_cfg(tmp_path, transport)).fetch_html("contests/1/play_by_play")
    assert is_cached("contests/1/play_by_play", cache_dir=tmp_path)


# --- host guard --------------------------------------------------------------


def test_other_host_url_rejected(tmp_path: Path) -> None:
    fetcher = NcaaFetcher(_cfg(tmp_path))
    with pytest.raises(ValueError, match="stats.ncaa.org"):
        fetcher.fetch_html("https://example.com/contests/1/play_by_play")


# --- ban-marker rotation -----------------------------------------------------


def test_ban_marker_response_rotates_proxy(tmp_path: Path) -> None:
    transport = FakeTransport([(200, "Access Denied - captcha"), (200, "<html>ok</html>")])
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport)
    fetcher = NcaaFetcher(cfg, proxy_pool=["http://u:p@1.1.1.1:1", "http://u:p@2.2.2.2:2"])

    text = fetcher.fetch_html("contests/1/play_by_play")

    assert text == "<html>ok</html>"
    assert len(transport.calls) == 2
    proxies_seen = {c[1]["http"] for c in transport.calls}
    assert proxies_seen == {"http://u:p@1.1.1.1:1", "http://u:p@2.2.2.2:2"}
    # the ban-page response is never the one written to cache
    assert cached_path("contests/1/play_by_play", cache_dir=tmp_path).read_text(encoding="utf-8") == "<html>ok</html>"


def test_all_proxies_exhausted_raises(tmp_path: Path) -> None:
    transport = FakeTransport([(403, "forbidden"), (403, "forbidden"), (403, "forbidden")])
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, max_retries=0)
    fetcher = NcaaFetcher(cfg, proxy_pool=["http://u:p@1.1.1.1:1"])
    with pytest.raises(RuntimeError, match="rotating proxies"):
        fetcher.fetch_html("contests/1/play_by_play")


# --- redaction ---------------------------------------------------------------


def test_repr_redacts_secrets() -> None:
    cfg = NcaaFetchConfig(
        proxy_url="http://secretlogin:secretpass@5.6.7.8:8080",
        proxybonanza_key="SUPERSECRETKEY",
        proxybonanza_pkg="pkg123",
    )
    r = repr(cfg)
    assert "secretlogin" not in r
    assert "secretpass" not in r
    assert "SUPERSECRETKEY" not in r
    assert str(cfg) == r
    assert "pkg123" in r  # package id is not a secret


# --- pool loader ---------------------------------------------------------------


def test_load_proxybonanza_pool_parses_canned_response() -> None:
    canned = json.dumps(
        {
            "data": {
                "login": "fakeuser",
                "password": "fakepass",
                "ippacks": [
                    {"ip": "9.9.9.1", "port_http": 8080},
                    {"ip": "9.9.9.2", "port_http": 8081},
                ],
            }
        }
    )

    def transport(url: str, headers: dict) -> "tuple[int, str]":
        assert url == "https://api.proxybonanza.com/v1/userpackages/pkg123.json"
        assert headers["Authorization"] == "fake-api-key"
        return 200, canned

    pool = load_proxybonanza_pool("fake-api-key", "pkg123", transport=transport)

    assert pool == [
        "http://fakeuser:fakepass@9.9.9.1:8080",
        "http://fakeuser:fakepass@9.9.9.2:8081",
    ]


def test_load_proxybonanza_pool_raises_on_error_status() -> None:
    def transport(url: str, headers: dict) -> "tuple[int, str]":
        return 401, "unauthorized"

    with pytest.raises(RuntimeError, match="status=401"):
        load_proxybonanza_pool("bad-key", "pkg123", transport=transport)


# --- path builders -------------------------------------------------------------


def test_path_builders_modern_and_legacy(tmp_path: Path) -> None:
    transport = FakeTransport([(200, "a")] * 6)
    fetcher = NcaaFetcher(_cfg(tmp_path, transport))

    fetcher.fetch_game_pbp("4690813")
    fetcher.fetch_game_pbp("4690813", legacy=True)
    fetcher.fetch_game_box("4690813", period=2)
    fetcher.fetch_game_box("4690813", period=2, legacy=True)
    fetcher.fetch_team_roster("391", "2024")
    fetcher.fetch_team_schedule("391")

    urls = [c[0] for c in transport.calls]
    assert urls == [
        "https://stats.ncaa.org/contests/4690813/play_by_play",
        "https://stats.ncaa.org/game/play_by_play/4690813",
        "https://stats.ncaa.org/contests/4690813/box_score?period_no=2",
        "https://stats.ncaa.org/game/box_score/4690813?period_no=2",
        "https://stats.ncaa.org/teams/391/roster/2024",
        "https://stats.ncaa.org/teams/391/game_by_game",
    ]


# --- cache-key query-string sanitization ---------------------------------------


def test_query_string_cache_key_sanitization(tmp_path: Path) -> None:
    p1 = cached_path("contests/1/box_score?period_no=1", cache_dir=tmp_path)
    p2 = cached_path("contests/1/box_score?period_no=2", cache_dir=tmp_path)

    assert p1 != p2
    assert p1.parent == p2.parent
    assert p1.name == "box_score__period_no=1.html"
    assert p2.name == "box_score__period_no=2.html"
    assert cached_path("contests/1/play_by_play", cache_dir=tmp_path).name == "play_by_play.html"


def test_query_string_unsafe_chars_sanitized(tmp_path: Path) -> None:
    p = cached_path("contests/1/box_score?a=1&b=hi there", cache_dir=tmp_path)
    assert p.name == "box_score__a=1&b=hi_there.html"


# --- UTF-8 round-trip ------------------------------------------------------


def test_utf8_roundtrip(tmp_path: Path) -> None:
    text = "Über Team – café"
    transport = FakeTransport([(200, text)])
    fetcher = NcaaFetcher(_cfg(tmp_path, transport))

    out = fetcher.fetch_html("contests/1/play_by_play")

    assert out == text
    assert cached_path("contests/1/play_by_play", cache_dir=tmp_path).read_text(encoding="utf-8") == text


# --- config surface ----------------------------------------------------------


def test_update_config_then_reset() -> None:
    reset_config()
    default_timeout = get_config().timeout
    update_config(timeout=999)
    assert get_config().timeout == 999
    reset_config()
    assert get_config().timeout == default_timeout


def test_update_config_unknown_key_raises() -> None:
    with pytest.raises(ValueError, match="Unknown config key"):
        update_config(not_a_real_field=1)
    reset_config()


def test_env_vars_populate_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SDV_PY_NCAA_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("SDV_PY_NCAA_PROXY_URL", "http://envuser:envpass@1.2.3.4:8080")
    monkeypatch.setenv("SDV_PY_PROXYBONANZA_KEY", "envkey")
    monkeypatch.setenv("SDV_PY_PROXYBONANZA_PKG", "envpkg")

    cfg = reset_config()

    assert cfg.cache_dir == tmp_path
    assert cfg.proxy_url == "http://envuser:envpass@1.2.3.4:8080"
    assert cfg.proxybonanza_key == "envkey"
    assert cfg.proxybonanza_pkg == "envpkg"

    monkeypatch.undo()
    reset_config()


# --- browser transport (suggested game-detail scraping method) --------------


def test_custom_transport_needs_no_proxy_pool(tmp_path: Path) -> None:
    """The no-direct-fetch guard is scoped to the default curl transport; an
    injected (browser / unblocker) transport runs pool-less and gets ``{}``
    for proxies."""
    transport = FakeTransport([(200, "<html>ok</html>")])
    fetcher = NcaaFetcher(NcaaFetchConfig(cache_dir=tmp_path, transport=transport))  # no proxy
    assert fetcher.fetch_html("contests/1/play_by_play") == "<html>ok</html>"
    assert len(transport.calls) == 1
    assert transport.calls[0][1] == {}  # empty pool -> direct, no proxy dict


def test_playwright_transport_shape() -> None:
    """The factory returns a callable, closeable, context-managed transport --
    all without importing Playwright (lazy until first fetch)."""
    t = playwright_transport()
    assert callable(t)
    with t as t2:
        assert t2 is t
    t.close()  # idempotent, browser never launched


def test_playwright_transport_missing_dep_raises() -> None:
    """Playwright is not a hard dep; first use without it raises a clear hint."""
    if importlib.util.find_spec("playwright") is not None:
        pytest.skip("playwright installed -- ImportError path not exercised")
    t = playwright_transport()
    with pytest.raises(ImportError, match="[Pp]laywright"):
        t("https://stats.ncaa.org/contests/1/play_by_play", {}, {})


def test_with_browser_wires_transport_pool_less(tmp_path: Path) -> None:
    fetcher = NcaaFetcher.with_browser(NcaaFetchConfig(cache_dir=tmp_path))
    assert callable(fetcher.config.transport)
    assert hasattr(fetcher.config.transport, "close")
    assert fetcher._pool == []  # browser path needs no proxy pool
    fetcher.__exit__()  # closes the (never-launched) transport without error


def test_ban_check_ignores_blocked_shot_in_real_content(tmp_path: Path) -> None:
    """A real game page says "layup blocked" (blocked shots); the WAF-specific
    ban markers must NOT false-positive on it (regression: bare "blocked")."""
    real = "<html><body><table><tr><td>Bezhanishvili 2pt layup blocked missed</td></tr></table></body></html>"
    transport = FakeTransport([(200, real)])
    fetcher = NcaaFetcher(_cfg(tmp_path, transport))
    assert fetcher.fetch_html("contests/1613299/play_by_play") == real
    assert len(transport.calls) == 1  # accepted first response, no rotation
