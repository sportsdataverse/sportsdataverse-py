"""Roster-talent tests (T2.2 Phase 1).

Task 1.1 pins the recruit-class loader's normalization of the 247 RDB recruit feed
(``sports247_recruits``) into per-recruit rows, monkeypatched to a synthetic payload
so no network is hit. The real column names are confirmed live during implementation.
"""

from __future__ import annotations

import sys

import polars as pl

from sportsdataverse.cfb.cfb_roster_talent import load_recruit_classes

_mod = sys.modules["sportsdataverse.cfb.cfb_roster_talent"]


def _fake_recruit_page(*, year: int, page: int | None = None, **kwargs: object) -> pl.DataFrame:
    """Two committed recruits + one uncommitted (dropped); empty after page 1 (ends paging)."""
    if page and page > 1:
        return pl.DataFrame(schema={"key": pl.Int64})
    return pl.DataFrame(
        {
            "key": [46112955, 46133902, 46128573],  # 247 recruit key (Int64)
            "composite_star_rating": [4.0, 5.0, 3.0],
            "composite_rating": [94.5, 98.1, 84.2],
            "primary_position": ["QB", "WR", "OT"],
            # committed team key is Float64 in the RDB -> must cast Int64->Utf8, not "71.0"
            "committed_institution_team_key": [71.0, 71.0, None],
            "committed_institution_full_name": ["Michigan Wolverines", "Michigan Wolverines", None],
        }
    )


def test_loader_normalizes_to_per_recruit_contract(monkeypatch) -> None:
    monkeypatch.setattr(_mod, "sports247_recruits", _fake_recruit_page)
    out = load_recruit_classes(2023, division="fbs")

    assert out.columns == ["season", "team_id", "team", "recruit_id", "stars", "grade", "position"]
    assert out.height == 2  # the uncommitted recruit (null team) is dropped
    assert out.schema["team_id"] == pl.Utf8
    assert out.schema["recruit_id"] == pl.Utf8
    assert out.schema["stars"] == pl.Int64
    assert out.schema["grade"] == pl.Float64
    # float team key -> clean integer string, never "71.0"
    assert out["team_id"].unique().to_list() == ["71"]
    assert out["recruit_id"].to_list() == ["46112955", "46133902"]
    assert out["season"].unique().to_list() == [2023]


def test_loader_multi_season_concats(monkeypatch) -> None:
    monkeypatch.setattr(_mod, "sports247_recruits", _fake_recruit_page)
    out = load_recruit_classes([2022, 2023], division="fbs")
    assert set(out["season"].unique().to_list()) == {2022, 2023}
    assert out.height == 4  # 2 committed recruits x 2 seasons


def test_loader_empty_returns_documented_schema(monkeypatch) -> None:
    monkeypatch.setattr(_mod, "sports247_recruits", lambda **k: pl.DataFrame(schema={"key": pl.Int64}))
    out = load_recruit_classes(2023)
    assert out.height == 0
    assert out.schema["team_id"] == pl.Utf8 and out.schema["stars"] == pl.Int64
