"""Shared offline fixtures: the real CLE @ JAX ESPN summary through the dispatch entry, no network."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sportsdataverse.football.sources.dispatch import _process_game

NFL_FIX = Path(__file__).parents[2] / "nfl" / "fixtures"
CFB_FIX = Path(__file__).parents[2] / "cfb" / "fixtures"
NFL_GAME_ID = 401872922  # CLE @ JAX, 2026 week 1 (final)
CFB_GAME_ID = 401628334  # 2024 week 1 (final)


@pytest.fixture(scope="session")
def nfl_summary() -> dict:
    return json.loads((NFL_FIX / f"summary_{NFL_GAME_ID}.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def cfb_summary() -> dict:
    return json.loads((CFB_FIX / f"summary_{CFB_GAME_ID}.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def no_network():
    """Any download on the processor modules is a test failure (the offline path must not reach ESPN)."""
    import sportsdataverse.cfb.cfb_pbp as cfb_mod
    import sportsdataverse.nfl.nfl_pbp as nfl_mod

    def _boom(*a, **k):
        raise AssertionError("network call on the offline path")

    saved = (nfl_mod.download, cfb_mod.download)
    nfl_mod.download = cfb_mod.download = _boom
    yield
    nfl_mod.download, cfb_mod.download = saved


@pytest.fixture(scope="session")
def nfl_processed(nfl_summary, no_network):
    """The ESPN path through ``_process_game`` on the stored summary (one ~8 s pipeline run per session)."""
    return _process_game("nfl", NFL_GAME_ID, payloads={"espn": nfl_summary})
