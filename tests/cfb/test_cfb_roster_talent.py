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


def test_blue_chip_window_rollup() -> None:
    from sportsdataverse.cfb.cfb_roster_talent import blue_chip_ratio

    # team A: 2020 class = 2 blue-chip of 4; 2021 class = 4 blue-chip of 4
    rec = pl.DataFrame(
        {
            "season": [2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021],
            "team_id": ["A"] * 8,
            "recruit_id": [f"r{i}" for i in range(8)],
            "stars": [5, 4, 3, 3, 4, 4, 5, 4],
            "grade": [95.0] * 8,
            "position": ["QB"] * 8,
        }
    )
    out = blue_chip_ratio(rec, window=4, division="fbs").sort("season")
    # season 2020 window sees only the 2020 class: 2/4 = 0.5
    row20 = out.filter(pl.col("season") == 2020).row(0, named=True)
    assert row20["n_recruits"] == 4 and row20["n_blue_chip"] == 2
    assert abs(row20["blue_chip_ratio"] - 0.5) < 1e-9
    # season 2021 window sees 2020+2021: (2+4)/(4+4) = 0.75
    row21 = out.filter(pl.col("season") == 2021).row(0, named=True)
    assert abs(row21["blue_chip_ratio"] - 0.75) < 1e-9


def _two_class_recruits() -> pl.DataFrame:
    # team A: 2020 class stars [5, 4] -> 170 pts; 2021 class [4] -> 70 pts
    # team B: 2021 class [3] -> 45 pts
    return pl.DataFrame(
        {
            "season": [2020, 2020, 2021, 2021],
            "team_id": ["A", "A", "A", "B"],
            "team": ["Team A", "Team A", "Team A", "Team B"],
            "recruit_id": ["r1", "r2", "r3", "r4"],
            "stars": [5, 4, 4, 3],
            "grade": [98.0, 92.0, 91.0, 85.0],
            "position": ["QB", "EDGE", "WR", "OT"],
        }
    )


def test_talent_composite_weighted_sum_and_rank(monkeypatch) -> None:
    from sportsdataverse.cfb.cfb_roster_talent import cfb_roster_talent

    monkeypatch.setattr(_mod, "load_recruit_classes", lambda *a, **k: _two_class_recruits())
    out = cfb_roster_talent(2021, division="fbs")
    # weights (1.0, 0.9, ...): A = 1.0*70 (2021 class) + 0.9*170 (2020 class) = 223
    row_a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    assert abs(row_a["talent_composite"] - 223.0) < 1e-9
    row_b = out.filter(pl.col("team_id") == "B").row(0, named=True)
    assert abs(row_b["talent_composite"] - 45.0) < 1e-9
    # dense rank desc within season + blue_chip_ratio joined in
    assert row_a["talent_rank"] == 1 and row_b["talent_rank"] == 2
    assert abs(row_a["blue_chip_ratio"] - 1.0) < 1e-9  # 3/3 blue chips
    assert row_a["n_recruits"] == 3
    assert out.schema["team_id"] == pl.Utf8


def test_talent_composite_247_override(monkeypatch) -> None:
    from sportsdataverse.cfb.cfb_roster_talent import cfb_roster_talent

    monkeypatch.setattr(_mod, "load_recruit_classes", lambda *a, **k: _two_class_recruits())
    ora = pl.DataFrame(
        {"season": [2021], "team_id": ["A"], "talent_247": [999.5]},
        schema={"season": pl.Int64, "team_id": pl.Utf8, "talent_247": pl.Float64},
    )
    out = cfb_roster_talent(2021, division="fbs", composite_247=ora)
    row_a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    assert abs(row_a["talent_composite"] - 999.5) < 1e-9  # 247 overrides
    row_b = out.filter(pl.col("team_id") == "B").row(0, named=True)
    assert abs(row_b["talent_composite"] - 45.0) < 1e-9  # ESPN-derived fallback


def test_loader_prefers_signed_institution_over_committed(monkeypatch) -> None:
    # r1 signed with team 71 but its committed_* drifted to 99 (decommit/transfer);
    # r2 never signed (signed_* null) -> falls back to committed 71.
    def _page(**kwargs):
        if kwargs.get("page", 1) > 1:
            return pl.DataFrame(schema={"key": pl.Int64})
        return pl.DataFrame(
            {
                "key": [1, 2],
                "committed_institution_team_key": [99.0, 71.0],
                "committed_institution_full_name": ["Wrong U", "Michigan Wolverines"],
                "signed_institution_team_key": [71.0, None],
                "signed_institution_full_name": ["Michigan Wolverines", None],
                "composite_star_rating": [4, 3],
                "composite_rating": [95.0, 88.0],
                "primary_position": ["QB", "WR"],
            }
        )

    monkeypatch.setattr(_mod, "sports247_recruits", _page)
    out = load_recruit_classes(2023)
    assert out["team_id"].unique().to_list() == ["71"]
    assert out["team"].unique().to_list() == ["Michigan Wolverines"]
