"""Offline tests for the stats.ncaa.org basketball officials / team-stats /
linescore parsers (the tabs the bigballR-port parsers didn't cover).

Real capture: WBB contest 5722355 (Coppin St. @ South Carolina 2024-11-14).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_box_tabs import (
    LINESCORE_SCHEMA,
    OFFICIALS_SCHEMA,
    TEAM_STATS_SCHEMA,
    parse_ncaa_bb_linescore,
    parse_ncaa_bb_officials,
    parse_ncaa_bb_team_stats,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "mbb_ncaa"


def _rd(name: str) -> str:
    return (FIX / f"bkb_{name}_5722355.html").read_text(encoding="utf-8")


def test_officials_full_crew_not_nav() -> None:
    df = parse_ncaa_bb_officials(_rd("officials"), contest_id="5722355")
    assert df.columns == list(OFFICIALS_SCHEMA.keys())
    assert df.height == 3  # a basketball crew of three
    assert "Charles Watson" in df.get_column("official").to_list()
    assert not set(df.get_column("official").to_list()) & {"Box Score", "Play By Play"}


def test_officials_empty() -> None:
    assert parse_ncaa_bb_officials("").height == 0


def test_team_stats_by_period() -> None:
    df = parse_ncaa_bb_team_stats(_rd("team_stats"), contest_id="5722355")
    assert df.columns == list(TEAM_STATS_SCHEMA.keys())
    assert df.height > 0
    periods = set(df.get_column("period").unique().to_list())
    assert "total" in periods
    assert {"1st Period", "2nd Period", "3rd Period", "4th Period"} <= periods
    assert df.get_column("away_team").drop_nulls().unique().to_list() == ["Coppin St."]


def test_team_stats_empty() -> None:
    df = parse_ncaa_bb_team_stats("")
    assert df.height == 0 and df.columns == list(TEAM_STATS_SCHEMA.keys())


def test_linescore_periods_final_meta() -> None:
    df = parse_ncaa_bb_linescore(_rd("box_score"), contest_id="5722355")
    assert df.columns == list(LINESCORE_SCHEMA.keys())
    assert set(df.get_column("home_away").unique().to_list()) == {"away", "home"}
    sc = df.filter(pl.col("team") == "South Carolina")
    assert sc.get_column("period").to_list() == ["1", "2", "3", "4"]
    assert sc.get_column("final").unique().to_list() == [92]
    assert sc.get_column("points").cast(pl.Int64).sum() == 92  # periods sum to final
    assert df.get_column("attendance").unique().to_list() == [15550]


def test_linescore_empty() -> None:
    df = parse_ncaa_bb_linescore("")
    assert df.height == 0 and df.columns == list(LINESCORE_SCHEMA.keys())
