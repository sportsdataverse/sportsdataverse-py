"""Offline tests for the ``ufl_pbp`` / ``xfl_pbp`` public shims (Task 1.4).

Monkeypatches the ``espn_{ufl,xfl}_summary`` fetch with the committed
fixtures so the shims are exercised end-to-end without network.
"""

from __future__ import annotations

import json
from pathlib import Path


from sportsdataverse.football.ufl import ufl_pbp
from sportsdataverse.football.xfl import xfl_pbp

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "league_ports"


def _load_fixture(name: str) -> dict:
    with open(FIXTURE_DIR / name, encoding="utf-8") as f:
        return json.load(f)


def test_xfl_pbp_routes_to_shared_core(monkeypatch):
    summary = _load_fixture("xfl_summary.json")
    monkeypatch.setattr("sportsdataverse.football.xfl.xfl_ep_wp.espn_xfl_summary", lambda *a, **kw: summary)

    out = xfl_pbp("401517780")
    assert out.height > 0
    assert "epa" in out.columns
    assert "wp" in out.columns


def test_ufl_pbp_routes_to_shared_core(monkeypatch):
    summary = _load_fixture("ufl_summary.json")
    monkeypatch.setattr("sportsdataverse.football.ufl.ufl_ep_wp.espn_ufl_summary", lambda *a, **kw: summary)

    # ESPN ships no play-by-play for the captured UFL game (see
    # FEASIBILITY.md) -- the shim correctly returns a zero-row frame rather
    # than raising.
    out = ufl_pbp("401638335")
    assert out.height == 0


def test_ufl_and_xfl_pbp_share_the_same_core(monkeypatch):
    # Feed the SAME (XFL, real-data) summary to both shims via monkeypatch
    # and confirm both dispatch through `build_spring_football_pbp` /
    # `enrich_spring_football_pbp` with the correct league slug -- i.e. they
    # aren't divergent reimplementations.
    summary = _load_fixture("xfl_summary.json")
    monkeypatch.setattr("sportsdataverse.football.xfl.xfl_ep_wp.espn_xfl_summary", lambda *a, **kw: summary)
    monkeypatch.setattr("sportsdataverse.football.ufl.ufl_ep_wp.espn_ufl_summary", lambda *a, **kw: summary)

    xfl_out = xfl_pbp("401517780")
    ufl_out = ufl_pbp("401517780")
    assert xfl_out.height == ufl_out.height > 0
    for col in ("ep", "epa", "wp", "wpa"):
        assert (xfl_out[col].fill_null(-999) - ufl_out[col].fill_null(-999)).abs().max() < 1e-9


def test_xfl_pbp_return_as_pandas(monkeypatch):
    import pandas as pd

    summary = _load_fixture("xfl_summary.json")
    monkeypatch.setattr("sportsdataverse.football.xfl.xfl_ep_wp.espn_xfl_summary", lambda *a, **kw: summary)

    out = xfl_pbp("401517780", return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
