"""Offline tests for the tiered TTL response cache."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

# ---------------------------------------------------------------------------
# Tier picker
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://api-web.nhle.com/v1/scoreboard/now", "LIVE"),
        ("https://api-web.nhle.com/v1/standings/now", "LIVE"),
        ("https://api-web.nhle.com/v1/gamecenter/2023030417/play-by-play", "IMMUTABLE"),
        ("https://api-web.nhle.com/v1/gamecenter/2023030417/boxscore", "IMMUTABLE"),
        ("https://api.nhle.com/stats/rest/en/glossary", "IMMUTABLE"),
        ("https://records.nhl.com/site/api/player", "IMMUTABLE"),
        ("https://api-web.nhle.com/v1/draft/picks/2024/1", "REFERENCE"),
        ("https://sports.core.api.espn.com/v2/.../leagues/nba/venues", "REFERENCE"),
        ("https://site.api.espn.com/apis/site/v2/sports/.../news", "FAST"),
        ("https://site.api.espn.com/apis/site/v2/sports/.../injuries", "FAST"),
        ("https://site.api.espn.com/apis/site/v2/sports/.../teams/13/roster", "SLOW"),
        ("https://api-web.nhle.com/v1/player/8478402/landing", "SLOW"),
        ("https://api.nhle.com/stats/rest/en/leaders/skaters/points", "MODERATE"),
    ],
)
def test_pick_ttl_classifies_by_url(url, expected):
    from sportsdataverse import cache

    tier_map = {
        "LIVE": cache.LIVE,
        "IMMUTABLE": cache.IMMUTABLE,
        "REFERENCE": cache.REFERENCE,
        "SLOW": cache.SLOW,
        "MODERATE": cache.MODERATE,
        "FAST": cache.FAST,
    }
    assert cache.pick_ttl(url) == tier_map[expected], f"{url} -> expected {expected}, got {cache.pick_ttl(url)}"


def test_pick_ttl_past_scoreboard_is_immutable():
    """A scoreboard URL with dates=YYYYMMDD for a past date should be
    cacheable forever (game results don't change)."""
    from sportsdataverse import cache

    # Date guaranteed in the past
    url = "https://site.api.espn.com/.../basketball/nba/scoreboard?dates=20200101"
    # Pass a fixed "today" so the test is deterministic
    today = datetime(2026, 5, 26, tzinfo=timezone.utc)
    assert cache.pick_ttl(url, today=today) == cache.IMMUTABLE


def test_pick_ttl_future_scoreboard_is_live():
    """A scoreboard URL with a future date — no point caching it, the
    games haven't been played yet."""
    from sportsdataverse import cache

    url = "https://site.api.espn.com/.../basketball/nba/scoreboard?dates=20990101"
    today = datetime(2026, 5, 26, tzinfo=timezone.utc)
    assert cache.pick_ttl(url, today=today) == cache.LIVE


# ---------------------------------------------------------------------------
# Mode + read/write
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_cache_dir(monkeypatch, tmp_path):
    """Point the filesystem cache at a temp dir."""
    monkeypatch.setenv("SDV_PY_CACHE_DIR", str(tmp_path))
    yield tmp_path


@pytest.fixture
def reset_cache():
    """Reset cache state before AND after each test."""
    from sportsdataverse import cache

    original_mode = cache.get_cache_mode()
    cache._MEMORY_CACHE.clear()
    cache.set_cache_mode("off")
    yield
    cache.set_cache_mode("off")
    cache._MEMORY_CACHE.clear()
    cache.set_cache_mode(original_mode)


def test_set_cache_mode_rejects_invalid(reset_cache):
    from sportsdataverse import cache

    with pytest.raises(ValueError, match="Invalid cache mode"):
        cache.set_cache_mode("disk")


def test_memory_cache_round_trip(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    cache.cache_set("https://x.example/a", None, {"hello": "world"})
    assert cache.cache_get("https://x.example/a") == {"hello": "world"}
    assert cache.cache_get("https://x.example/b") is None  # miss


def test_filesystem_cache_round_trip(tmp_cache_dir, reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("filesystem")
    cache.cache_set("https://x.example/a", {"q": 1}, {"data": [1, 2, 3]})
    out = cache.cache_get("https://x.example/a", {"q": 1})
    assert out == {"data": [1, 2, 3]}


def test_cache_params_are_order_insensitive(reset_cache):
    """The cache key should be the same for ?a=1&b=2 and ?b=2&a=1."""
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    cache.cache_set("https://x.example/a", {"a": 1, "b": 2}, {"key": "ab"})
    assert cache.cache_get("https://x.example/a", {"b": 2, "a": 1}) == {"key": "ab"}


def test_cache_off_mode_short_circuits(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("off")
    cache.cache_set("https://x.example/a", None, {"value": 1})
    assert cache.cache_get("https://x.example/a") is None
    # And the memory dict stayed empty
    assert not cache._MEMORY_CACHE


def test_live_tier_bypasses_cache(reset_cache):
    """URLs that pick_ttl classifies as LIVE should never be persisted."""
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    cache.cache_set(
        "https://api-web.nhle.com/v1/scoreboard/now",
        None,
        {"games": []},
    )
    assert (
        cache.cache_get(
            "https://api-web.nhle.com/v1/scoreboard/now",
        )
        is None
    )


def test_per_call_ttl_override_wins(reset_cache):
    """A caller-supplied ttl= overrides the tier picker."""
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    # This URL would normally be LIVE (no cache); force it to 1h
    cache.cache_set(
        "https://api-web.nhle.com/v1/scoreboard/now",
        None,
        {"forced": True},
        ttl=timedelta(hours=1),
    )
    out = cache.cache_get(
        "https://api-web.nhle.com/v1/scoreboard/now",
        ttl=timedelta(hours=1),
    )
    assert out == {"forced": True}


def test_clear_cache_by_url(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    cache.cache_set("https://x.example/a", None, {"a": 1})
    cache.cache_set("https://x.example/b", None, {"b": 2})
    n = cache.clear_cache(url="https://x.example/a")
    assert n == 1
    assert cache.cache_get("https://x.example/a") is None
    assert cache.cache_get("https://x.example/b") == {"b": 2}


def test_clear_cache_all(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    for i in range(5):
        cache.cache_set(f"https://x.example/{i}", None, {"i": i})
    n = cache.clear_cache()
    assert n == 5
    assert cache.cache_stats()["entries"] == 0


def test_cache_stats_reflects_mode(reset_cache):
    from sportsdataverse import cache

    cache.set_cache_mode("memory")
    stats = cache.cache_stats()
    assert stats["mode"] == "memory"
    assert stats["entries"] == 0
    cache.cache_set("https://x.example/a", None, {"v": 1})
    assert cache.cache_stats()["entries"] == 1


# ---------------------------------------------------------------------------
# download() integration: cache hit returns CachedResponse
# ---------------------------------------------------------------------------


def test_download_returns_cached_response_on_hit(reset_cache):
    """When the cache has an entry for the URL, download() must return a
    CachedResponse shim WITHOUT making a network call."""
    from sportsdataverse import cache
    from sportsdataverse.dl_utils import download

    cache.set_cache_mode("memory")
    cache.cache_set(
        "https://api.example.com/teams",
        None,
        {"teams": [{"id": 1}]},
    )
    resp = download("https://api.example.com/teams")
    assert isinstance(resp, cache.CachedResponse)
    assert resp.from_cache is True
    assert resp.json() == {"teams": [{"id": 1}]}
    assert resp.status_code == 200


def test_download_offmode_does_not_consult_cache(monkeypatch):
    """With mode=off, the cache layer must never short-circuit."""
    from sportsdataverse import cache

    cache.set_cache_mode("off")
    cache._MEMORY_CACHE.clear()
    cache._MEMORY_CACHE["fake-key"] = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "url": "https://api.example.com/teams",
        "body": {"DO_NOT_RETURN": True},
    }
    # If the off-mode short-circuit fails, we'd hit the network — but with
    # the network mocked to raise, we'd see the exception
    import sportsdataverse.dl_utils as dl_utils

    called = {"hit": False}

    def fake_get(self, url, **kw):
        called["hit"] = True
        raise RuntimeError("would have hit network")

    monkeypatch.setattr("requests.Session.get", fake_get)
    with pytest.raises(RuntimeError, match="would have hit network"):
        dl_utils.download("https://api.example.com/teams", num_retries=0)
    assert called["hit"], "download should hit network when mode=off"
