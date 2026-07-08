"""Tests for the NFL-draft outcome loader + projection (T2.2 Phase 5)."""

from __future__ import annotations

import importlib

import polars as pl

_mod = importlib.import_module("sportsdataverse.cfb.cfb_draft_projection")
from sportsdataverse.cfb.cfb_draft_projection import load_draft_outcomes


def _fake_picks(**kwargs) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [2023, 2023, 2010],
            "round": [1, 1, 2],
            "pick": [1, 2, 40],
            "pfr_player_name": ["Bryce Young", "C.J. Stroud", "Old Guy"],
            "college": ["Alabama", "Ohio St.", "Nowhere"],
            "cfb_player_id": ["4685720", None, None],
            "position": ["QB", "QB", "RB"],
        }
    )


def test_draft_outcomes_contract(monkeypatch) -> None:
    monkeypatch.setattr(_mod, "load_nfl_draft_picks", _fake_picks)
    out = load_draft_outcomes(2023)
    assert out.height == 2  # 2010 filtered out
    assert out.schema["draft_year"] == pl.Int64
    assert out.schema["player_id"] == pl.Utf8
    assert out.schema["round"] == pl.Int64 and out.schema["pick"] == pl.Int64
    row = out.row(0, named=True)
    assert row["player_name"] == "Bryce Young" and row["college"] == "Alabama"
    assert row["player_id"] == "4685720"


def test_draft_outcomes_multi_year_and_empty(monkeypatch) -> None:
    monkeypatch.setattr(_mod, "load_nfl_draft_picks", _fake_picks)
    both = load_draft_outcomes([2010, 2023])
    assert set(both["draft_year"].unique().to_list()) == {2010, 2023}
    monkeypatch.setattr(_mod, "load_nfl_draft_picks", lambda **k: pl.DataFrame())
    empty = load_draft_outcomes(2023)
    assert empty.height == 0
    for col in ("draft_year", "college", "player_id", "player_name", "round", "pick"):
        assert col in empty.columns
