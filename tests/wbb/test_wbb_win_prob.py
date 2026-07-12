"""Tests for the WBB season win-probability shim (``wbb_win_prob``)."""

from __future__ import annotations

import polars as pl

import sportsdataverse.mbb.mbb_win_prob as core
from sportsdataverse.wbb.wbb_win_prob import build_wbb_season_wp


def test_delegates_with_womens_league(monkeypatch):
    seen = {}

    def _fake(season, league):
        seen["league"] = league
        # empty pbp -> zero-row schema frame, exercises the full delegation path
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

    monkeypatch.setattr(core, "_load_league_frames", _fake)
    out = build_wbb_season_wp(2024)
    assert seen["league"] == "womens"
    assert out.columns == list(core._WP_SCHEMA)
