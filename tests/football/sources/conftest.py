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
    import sportsdataverse.nfl.cbs_pbp.game_id as cbs_id_mod
    import sportsdataverse.nfl.nfl_pbp as nfl_mod

    def _boom(*a, **k):
        raise AssertionError("network call on the offline path")

    saved = (nfl_mod.download, cfb_mod.download, cbs_id_mod.download)
    # the CBS adapter is the first registered alternate that FETCHES when it is reached with
    # no payload (a week scoreboard page, then four NAPI bodies), so the offline guard has to
    # cover its transport too, not just the processors'
    nfl_mod.download = cfb_mod.download = cbs_id_mod.download = _boom
    yield
    nfl_mod.download, cfb_mod.download, cbs_id_mod.download = saved


@pytest.fixture
def alternates_unavailable(monkeypatch):
    """Every registered non-ESPN adapter hands over, so a fall-through test stays offline."""
    from sportsdataverse.football.sources import dispatch

    def _down(source):
        def adapter(league, espn_id, ctx):
            raise dispatch.SourceUnavailable(f"{source} down")

        return adapter

    # Registered slots only, and including the ones not imported yet: an adapter module is
    # imported lazily by ``_adapter_for``, so a registered-but-unimported source is absent from
    # ``_ADAPTERS`` here and would otherwise reach its own fetch. A slot with no adapter at all
    # keeps its "not implemented" attempt.
    for league, source in set(dispatch._ADAPTERS) | set(dispatch._ADAPTER_MODULES):
        if source != "espn":
            monkeypatch.setitem(dispatch._ADAPTERS, (league, source), _down(source))


@pytest.fixture(scope="session")
def nfl_processed(nfl_summary, no_network):
    """The ESPN path through ``_process_game`` on the stored summary (one ~8 s pipeline run per session)."""
    return _process_game("nfl", NFL_GAME_ID, payloads={"espn": nfl_summary})
