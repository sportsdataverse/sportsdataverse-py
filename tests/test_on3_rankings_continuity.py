"""Continuity tests for the 4 legacy On3 rankings names after the RDB retarget.

The names moved from the generated ``on3`` stem to the hand-written
``on3_rankings`` shim; they must keep returning frames (via the retained
``_next/data`` scrape) and emit a ``DeprecationWarning``."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "on3"


def _load(stem: str) -> dict:
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


def test_player_rankings_returns_frame_and_warns(monkeypatch):
    import sportsdataverse.cfb.on3_rankings as shim

    monkeypatch.setattr(shim, "_scrape_get", lambda *a, **k: _load("on3_player_rankings"))
    with pytest.warns(DeprecationWarning):
        df = shim.on3_player_rankings(year=2026)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 3
    assert "person_name" in df.columns


def test_team_rankings_returns_frame_and_warns(monkeypatch):
    import sportsdataverse.cfb.on3_rankings as shim

    monkeypatch.setattr(shim, "_scrape_get", lambda *a, **k: _load("on3_team_rankings"))
    with pytest.warns(DeprecationWarning):
        df = shim.on3_team_rankings(year=2026)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 3
    assert "organization_name" in df.columns


def test_raw_passthrough_and_warns(monkeypatch):
    import sportsdataverse.cfb.on3_rankings as shim

    payload = _load("on3_player_rankings")
    monkeypatch.setattr(shim, "_scrape_get", lambda *a, **k: payload)
    with pytest.warns(DeprecationWarning):
        raw = shim.on3_industry_player_rankings(year=2026, return_parsed=False)
    assert isinstance(raw, dict)
    assert "pageProps" in raw


def test_all_four_legacy_names_importable():
    from sportsdataverse.cfb import (
        on3_industry_player_rankings,
        on3_industry_team_rankings,
        on3_player_rankings,
        on3_team_rankings,
    )

    for fn in (on3_player_rankings, on3_industry_player_rankings, on3_team_rankings, on3_industry_team_rankings):
        assert callable(fn)
