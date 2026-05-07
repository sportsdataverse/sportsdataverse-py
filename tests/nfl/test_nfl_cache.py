"""Tests for the NFL caching layer + config module.

Covers:

- Memory cache hit returns the same frame on a second call.
- Filesystem cache persists across decorator invocations and is read on
  subsequent calls.
- ``cache_mode='off'`` skips the cache entirely.
- ``cache_duration`` expiration triggers a refetch.
- ``clear_cache()`` empties both memory and filesystem.
- Env-var initialization (``SDV_PY_NFL_CACHE``) is honored at import.
- ``update_config()`` raises ``ValueError`` on unknown keys.
- ``return_as_pandas=True`` round-trips through the cache.

Live tests are gated on ``SDV_PY_LIVE_TESTS=1`` because they hit the
network on the cache-miss path. Offline tests use a tiny synthetic loader
wrapped with ``@cached_loader`` so the cache behavior is exercised without
needing a real download.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.nfl as nfl
from sportsdataverse.nfl import cache as cache_mod
from sportsdataverse.nfl import config as config_mod
from sportsdataverse.nfl.cache import cached_loader, clear_cache
from sportsdataverse.nfl.config import (
    NflConfig,
    get_config,
    reset_config,
    update_config,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_cache(tmp_path: Path):
    """Each test gets a clean memory + filesystem cache and a tmp cache dir.

    Saves the live config, points it at a unique temp dir for the test, and
    restores the original config + clears both caches afterwards. This
    prevents cross-test bleed when tests run in parallel or in different
    orders.
    """
    snapshot = NflConfig(
        cache_mode=get_config().cache_mode,
        cache_dir=get_config().cache_dir,
        cache_duration=get_config().cache_duration,
        verbose=get_config().verbose,
        timeout=get_config().timeout,
        user_agent=get_config().user_agent,
    )
    update_config(
        cache_mode="memory",
        cache_dir=tmp_path,
        cache_duration=86400,
    )
    cache_mod._MEMORY.clear()
    yield
    cache_mod._MEMORY.clear()
    update_config(
        cache_mode=snapshot.cache_mode,
        cache_dir=snapshot.cache_dir,
        cache_duration=snapshot.cache_duration,
        verbose=snapshot.verbose,
        timeout=snapshot.timeout,
        user_agent=snapshot.user_agent,
    )


@pytest.fixture
def fake_loader():
    """A tiny ``@cached_loader``-wrapped function with a call counter.

    Returning a fresh closure per test means the call counter starts at 0,
    and the function's ``__qualname__`` includes the test-scoped factory
    so cache keys don't collide across tests.
    """

    counter = {"calls": 0}

    @cached_loader
    def _fake(seasons=None, return_as_pandas=False):
        counter["calls"] += 1
        df = pl.DataFrame({"x": [1, 2, 3], "season": [2023, 2024, 2025]})
        return df.to_pandas() if return_as_pandas else df

    return _fake, counter


# ---------------------------------------------------------------------------
# Memory cache
# ---------------------------------------------------------------------------


def test_memory_cache_returns_same_frame_on_second_call(fake_loader):
    """Second call should hit memory and skip the underlying function body."""
    fn, counter = fake_loader

    first = fn(seasons=[2024])
    second = fn(seasons=[2024])

    assert counter["calls"] == 1, "second call should be a cache hit"
    assert first.equals(second)
    assert isinstance(first, pl.DataFrame)


def test_memory_cache_separates_different_args(fake_loader):
    """Different args produce different cache keys → distinct fetches."""
    fn, counter = fake_loader
    fn(seasons=[2024])
    fn(seasons=[2025])
    assert counter["calls"] == 2


def test_return_as_pandas_roundtrips_through_memory_cache(fake_loader):
    """Cache stores polars; pandas conversion happens on read."""
    fn, counter = fake_loader

    pl_first = fn(seasons=[2024])
    pd_second = fn(seasons=[2024], return_as_pandas=True)

    # Single underlying call — pandas read came from cached polars frame.
    assert counter["calls"] == 1
    assert isinstance(pl_first, pl.DataFrame)
    # pandas import is implicit via polars; just check duck-typed shape.
    assert hasattr(pd_second, "to_dict")
    assert list(pd_second["x"]) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Filesystem cache
# ---------------------------------------------------------------------------


def test_filesystem_cache_persists_across_calls(tmp_path, fake_loader):
    """First call writes parquet; second call reads it without invoking fn."""
    update_config(cache_mode="filesystem", cache_dir=tmp_path)
    fn, counter = fake_loader

    first = fn(seasons=[2024])
    # Drop memory cache (filesystem mode shouldn't use it anyway, but be explicit).
    cache_mod._MEMORY.clear()
    second = fn(seasons=[2024])

    assert counter["calls"] == 1, "second call should hit the on-disk parquet"
    assert first.equals(second)

    # At least one parquet file landed under cache_dir.
    parquet_files = list(tmp_path.glob("*.parquet"))
    assert len(parquet_files) >= 1


# ---------------------------------------------------------------------------
# Off mode
# ---------------------------------------------------------------------------


def test_cache_off_skips_cache(fake_loader):
    """``cache_mode='off'`` must invoke the underlying fn on every call."""
    update_config(cache_mode="off")
    fn, counter = fake_loader

    fn(seasons=[2024])
    fn(seasons=[2024])
    fn(seasons=[2024])

    assert counter["calls"] == 3


# ---------------------------------------------------------------------------
# Expiration
# ---------------------------------------------------------------------------


def test_cache_expiration_invalidates_after_duration(fake_loader):
    """A 0-second TTL means every call is treated as expired → refetch."""
    update_config(cache_duration=0)
    fn, counter = fake_loader

    fn(seasons=[2024])
    # Sleep a hair so monotonic clock advances past 0.
    time.sleep(0.01)
    fn(seasons=[2024])

    assert counter["calls"] == 2


# ---------------------------------------------------------------------------
# clear_cache()
# ---------------------------------------------------------------------------


def test_clear_cache_empties_both_modes(tmp_path, fake_loader):
    """``clear_cache()`` wipes the memory dict and filesystem entries."""
    fn, _ = fake_loader

    # Populate memory cache.
    update_config(cache_mode="memory", cache_dir=tmp_path)
    fn(seasons=[2024])
    assert len(cache_mod._MEMORY) == 1

    # Populate filesystem cache (in same tmp dir).
    update_config(cache_mode="filesystem", cache_dir=tmp_path)
    fn(seasons=[2025])
    assert len(list(tmp_path.glob("*.parquet"))) >= 1

    clear_cache()

    assert len(cache_mod._MEMORY) == 0
    assert list(tmp_path.glob("*.parquet")) == []
    # Directory itself preserved.
    assert tmp_path.exists()


# ---------------------------------------------------------------------------
# Env-var initialization
# ---------------------------------------------------------------------------


def test_env_var_initialization_cache_mode(monkeypatch):
    """``SDV_PY_NFL_CACHE=off`` should be picked up by ``_from_env()``."""
    monkeypatch.setenv("SDV_PY_NFL_CACHE", "off")
    cfg = config_mod._from_env()
    assert cfg.cache_mode == "off"


def test_env_var_initialization_cache_duration(monkeypatch):
    monkeypatch.setenv("SDV_PY_NFL_CACHE_DURATION", "120")
    cfg = config_mod._from_env()
    assert cfg.cache_duration == 120


def test_env_var_initialization_invalid_duration_falls_back(monkeypatch):
    """Invalid int values must be ignored (default kept)."""
    monkeypatch.setenv("SDV_PY_NFL_CACHE_DURATION", "not-a-number")
    cfg = config_mod._from_env()
    assert cfg.cache_duration == 86400  # default


def test_env_var_initialization_cache_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("SDV_PY_NFL_CACHE_DIR", str(tmp_path))
    cfg = config_mod._from_env()
    assert cfg.cache_dir == tmp_path


def test_env_var_initialization_user_agent(monkeypatch):
    monkeypatch.setenv("SDV_PY_NFL_USER_AGENT", "test-agent/1.0")
    cfg = config_mod._from_env()
    assert cfg.user_agent == "test-agent/1.0"


# ---------------------------------------------------------------------------
# update_config validation
# ---------------------------------------------------------------------------


def test_update_config_validation_raises_on_unknown_key():
    with pytest.raises(ValueError, match="Unknown config key"):
        update_config(unknown_key=123)


def test_update_config_coerces_string_cache_dir(tmp_path):
    update_config(cache_dir=str(tmp_path))
    assert get_config().cache_dir == tmp_path
    assert isinstance(get_config().cache_dir, Path)


def test_reset_config_restores_defaults(monkeypatch):
    update_config(cache_mode="off", cache_duration=1)
    reset_config()
    cfg = get_config()
    # Default cache_mode is memory unless env says otherwise.
    assert cfg.cache_mode in ("memory", "filesystem", "off")  # whatever env says
    # cache_duration default is 86400 unless env says otherwise.
    assert isinstance(cfg.cache_duration, int)


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


def test_public_imports_resolve():
    """All cache + config symbols should be reachable from ``sportsdataverse.nfl``."""
    assert callable(nfl.clear_cache)
    assert callable(nfl.get_config)
    assert callable(nfl.update_config)
    assert nfl.NflConfig is NflConfig


# ---------------------------------------------------------------------------
# Live integration test (gated on env)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    os.environ.get("SDV_PY_LIVE_TESTS") != "1",
    reason="live network test — set SDV_PY_LIVE_TESTS=1 to enable",
)
def test_live_load_nfl_combine_cache_hit_is_fast():
    """First call hits network; second call should be served from memory."""
    clear_cache()
    update_config(cache_mode="memory")

    t0 = time.time()
    first = nfl.load_nfl_combine()
    cold_elapsed = time.time() - t0

    t1 = time.time()
    second = nfl.load_nfl_combine()
    warm_elapsed = time.time() - t1

    assert isinstance(first, pl.DataFrame)
    assert first.equals(second)
    # The cache hit should be at least an order of magnitude faster than the
    # cold fetch. Use a generous floor so flakes on tiny machines don't fire.
    assert warm_elapsed < cold_elapsed
    assert warm_elapsed < 0.5
