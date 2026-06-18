"""Offline tests for sportsdataverse.nfl.nfl_build.

All tests are network-free: they monkeypatch ``nfl_build._build_game_espn``
so no ESPN requests are made.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Generator

import polars as pl
import pytest

import sportsdataverse.nfl.nfl_build as nfl_build_mod
from sportsdataverse.nfl import clear_cache, reset_config, update_config
from sportsdataverse.nfl.nfl_build import PIPELINE_VERSION, build_nfl_season


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FRAME_A = pl.DataFrame({"game_id": [1, 1], "play_id": [101, 102], "yards": [5, 10]})
_FRAME_B = pl.DataFrame({"game_id": [2, 2], "play_id": [201, 202], "epa": [0.1, -0.2]})


@pytest.fixture(autouse=True)
def _reset_cache() -> Generator[None, None, None]:
    """Reset config + clear cache before and after each test."""
    reset_config()
    clear_cache()
    yield
    reset_config()
    clear_cache()


# ---------------------------------------------------------------------------
# Test 1: compile + concat with diagonal_relaxed (schema union)
# ---------------------------------------------------------------------------


def test_compile_concat_diagonal_relaxed(monkeypatch: pytest.MonkeyPatch) -> None:
    """build_nfl_season merges games with different column sets via schema union."""
    call_map = {1: _FRAME_A, 2: _FRAME_B}

    def fake_build(game_id: int) -> pl.DataFrame:
        return call_map[game_id]

    monkeypatch.setattr(nfl_build_mod, "_build_game_espn", fake_build)
    update_config(cache_mode="off")

    result = build_nfl_season([1, 2])

    assert isinstance(result, pl.DataFrame)
    assert result.shape[0] == _FRAME_A.shape[0] + _FRAME_B.shape[0]
    # Columns must be the union of both frames
    assert "yards" in result.columns
    assert "epa" in result.columns
    assert "game_id" in result.columns
    # Missing cells are null, not erroring
    assert result.filter(pl.col("game_id") == 1)["epa"].is_null().all()
    assert result.filter(pl.col("game_id") == 2)["yards"].is_null().all()


# ---------------------------------------------------------------------------
# Test 2: cache hit skips builder (memory mode)
# ---------------------------------------------------------------------------


def test_cache_hit_memory_skips_builder(monkeypatch: pytest.MonkeyPatch) -> None:
    """Second call with cache_mode='memory' does not invoke _build_game_espn again."""
    call_count = {"n": 0}

    def counting_build(game_id: int) -> pl.DataFrame:
        call_count["n"] += 1
        return _FRAME_A

    monkeypatch.setattr(nfl_build_mod, "_build_game_espn", counting_build)
    update_config(cache_mode="memory", cache_duration=3600)

    build_nfl_season([1])
    assert call_count["n"] == 1, "builder should run once on first call"

    build_nfl_season([1])
    assert call_count["n"] == 1, "builder should NOT run on cache hit"


# ---------------------------------------------------------------------------
# Test 3: cache hit skips builder (filesystem mode)
# ---------------------------------------------------------------------------


def test_cache_hit_filesystem_skips_builder(monkeypatch: pytest.MonkeyPatch) -> None:
    """Second call with cache_mode='filesystem' does not invoke _build_game_espn again."""
    call_count = {"n": 0}

    def counting_build(game_id: int) -> pl.DataFrame:
        call_count["n"] += 1
        return _FRAME_A

    monkeypatch.setattr(nfl_build_mod, "_build_game_espn", counting_build)

    with tempfile.TemporaryDirectory() as tmp:
        update_config(cache_mode="filesystem", cache_dir=Path(tmp), cache_duration=3600)

        build_nfl_season([1])
        assert call_count["n"] == 1

        build_nfl_season([1])
        assert call_count["n"] == 1, "builder should NOT run on filesystem cache hit"


# ---------------------------------------------------------------------------
# Test 4: PIPELINE_VERSION bump forces recompute
# ---------------------------------------------------------------------------


def test_pipeline_version_bump_invalidates_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Changing PIPELINE_VERSION causes a cache miss, forcing the builder to run again."""
    call_count = {"n": 0}

    def counting_build(game_id: int) -> pl.DataFrame:
        call_count["n"] += 1
        return _FRAME_A

    monkeypatch.setattr(nfl_build_mod, "_build_game_espn", counting_build)
    update_config(cache_mode="memory", cache_duration=3600)

    # Prime the cache with the current version
    build_nfl_season([1])
    assert call_count["n"] == 1

    # Simulate a pipeline bump by patching PIPELINE_VERSION
    old_version = nfl_build_mod.PIPELINE_VERSION
    monkeypatch.setattr(nfl_build_mod, "PIPELINE_VERSION", old_version + 1)

    build_nfl_season([1])
    assert call_count["n"] == 2, "bumped PIPELINE_VERSION must invalidate cache"


# ---------------------------------------------------------------------------
# Test 5: source="shield" raises NotImplementedError
# ---------------------------------------------------------------------------


def test_shield_source_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        build_nfl_season([1], source="shield")


# ---------------------------------------------------------------------------
# Test 6: invalid source raises ValueError
# ---------------------------------------------------------------------------


def test_invalid_source_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Unknown source"):
        build_nfl_season([1], source="invalid_source")


# ---------------------------------------------------------------------------
# Test 7: failed game is skipped, result is partial
# ---------------------------------------------------------------------------


def test_failed_game_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    """A game that raises an exception is skipped; the rest still compile."""

    def flaky_build(game_id: int) -> pl.DataFrame:
        if game_id == 999:
            raise RuntimeError("ESPN 500")
        return _FRAME_A

    monkeypatch.setattr(nfl_build_mod, "_build_game_espn", flaky_build)
    update_config(cache_mode="off")

    with pytest.warns(UserWarning, match="skipping game_id=999"):
        result = build_nfl_season([1, 999])

    assert result.shape[0] == _FRAME_A.shape[0]


# ---------------------------------------------------------------------------
# Test 8: return_as_pandas=True returns a pandas DataFrame
# ---------------------------------------------------------------------------


def test_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    import pandas as pd

    monkeypatch.setattr(nfl_build_mod, "_build_game_espn", lambda gid: _FRAME_A)
    update_config(cache_mode="off")

    result = build_nfl_season([1], return_as_pandas=True)
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Test 9: empty game_ids returns empty DataFrame
# ---------------------------------------------------------------------------


def test_empty_game_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(nfl_build_mod, "_build_game_espn", lambda gid: _FRAME_A)
    update_config(cache_mode="off")

    result = build_nfl_season([])
    assert isinstance(result, pl.DataFrame)
    assert result.shape[0] == 0


# ---------------------------------------------------------------------------
# Test 10: PIPELINE_VERSION is a positive integer constant
# ---------------------------------------------------------------------------


def test_pipeline_version_is_positive_int() -> None:
    assert isinstance(PIPELINE_VERSION, int)
    assert PIPELINE_VERSION >= 1
