"""Offline tests for the ``ufl_pbp`` / ``xfl_pbp`` public shims (Task 1.4).

Monkeypatches the ``espn_{ufl,xfl}_summary`` fetch with the committed
fixtures so the shims are exercised end-to-end without network.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sportsdataverse.football.ufl import ufl_pbp
from sportsdataverse.football.xfl import xfl_pbp

# UFL / XFL frames lack the xYAC and 4th-down inputs, and enrich_spring_football_pbp keeps that
# warning on purpose (see its comment); test_ufl_and_xfl_pbp_share_the_same_core asserts it.
pytestmark = pytest.mark.filterwarnings(r"ignore:enrich_nfl_pbp\x3a skipping:RuntimeWarning")

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

    with pytest.warns(RuntimeWarning, match=r"enrich_nfl_pbp: skipping"):
        xfl_out = xfl_pbp("401517780")
        ufl_out = ufl_pbp("401517780")
    assert xfl_out.height == ufl_out.height > 0
    # Same core, different league-season constants: the only rows allowed to differ are the
    # kickoffs, which the XFL (touchback at 65) and the UFL (its earliest ruleset, 2024: 75)
    # score from different touchback spots (rules/xfl.yaml xfl-2020-inaugural-ruleset,
    # rules/ufl.yaml ufl-2024-inaugural-ruleset).
    # Same kickoff definition as ep_wp._apply_feature_substitution (a kickoff with a penalty
    # carries play_type "kickoff" but kickoff_attempt 0).
    ko = (xfl_out["kickoff_attempt"] == 1) | (xfl_out["play_type"] == "kickoff")
    assert ko.sum() >= 5
    assert bool(((xfl_out["ep"] - ufl_out["ep"]).abs() > 1e-6).filter(ko).all()), "league touchback not threaded"
    scrimmage = xfl_out["down"].is_not_null()
    for col in ("ep", "wp"):
        d = (xfl_out[col].fill_null(-999) - ufl_out[col].fill_null(-999)).abs()
        assert d.filter(scrimmage).max() < 1e-9, col
    # epa / wpa lead into the next REAL play (a no-play row in between is skipped), so the
    # one or two rows before a kickoff may also move.
    ko_f = ko.fill_null(False)
    before_ko = ko_f.shift(-1, fill_value=False) | ko_f.shift(-2, fill_value=False)
    for col in ("epa", "wpa"):
        d = (xfl_out[col].fill_null(-999) - ufl_out[col].fill_null(-999)).abs()
        assert d.filter(scrimmage & ~before_ko).max() < 1e-9, col


def test_xfl_pbp_return_as_pandas(monkeypatch):
    import pandas as pd

    summary = _load_fixture("xfl_summary.json")
    monkeypatch.setattr("sportsdataverse.football.xfl.xfl_ep_wp.espn_xfl_summary", lambda *a, **kw: summary)

    out = xfl_pbp("401517780", return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
