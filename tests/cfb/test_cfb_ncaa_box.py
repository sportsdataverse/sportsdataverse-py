"""Offline tests for the stats.ncaa.org college-football box-score parsers.

Parses a committed real capture (contest 5362283, California @ Auburn 2024-09-07):
the drives, officials, team_stats (by-period), individual_stats, and box_score
(linescore) tabs.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.cfb.cfb_ncaa_box import (
    DRIVES_SCHEMA,
    LINESCORE_SCHEMA,
    OFFICIALS_SCHEMA,
    TEAM_STATS_SCHEMA,
    parse_cfb_ncaa_drives,
    parse_cfb_ncaa_linescore,
    parse_cfb_ncaa_officials,
    parse_cfb_ncaa_player_stats,
    parse_cfb_ncaa_team_stats,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "cfb_ncaa"


def _rd(name: str) -> str:
    return (FIX / f"mfb_{name}_5362283.html").read_text(encoding="utf-8")


# --- drives ---------------------------------------------------------------


def test_drives_schema_and_rows() -> None:
    df = parse_cfb_ncaa_drives(_rd("drives"), contest_id="5362283")
    assert df.columns == list(DRIVES_SCHEMA.keys())
    assert df.height >= 20  # a full game of drives
    # drive_number is a clean 1..N sequence
    assert df.get_column("drive_number").to_list() == list(range(1, df.height + 1))
    assert df.get_column("quarter").is_between(1, 5).all()


def test_drives_start_end_vocab() -> None:
    df = parse_cfb_ncaa_drives(_rd("drives"), contest_id="5362283")
    assert df.get_column("start_how").is_not_null().all()
    ends = set(df.get_column("end_how").unique().to_list())
    assert {"TD", "PUNT", "DOWNS"} <= ends  # football drive outcomes
    # yard lines look like TEAM+number when present
    yl = df.filter(pl.col("start_yard_line").is_not_null())
    assert yl.get_column("start_yard_line").str.contains(r"^[A-Z]{2,4}\d+$").all()


def test_drives_empty() -> None:
    df = parse_cfb_ncaa_drives("")
    assert df.height == 0 and df.columns == list(DRIVES_SCHEMA.keys())


# --- officials ------------------------------------------------------------


def test_officials_extracted_not_nav() -> None:
    df = parse_cfb_ncaa_officials(_rd("officials"), contest_id="5362283")
    assert df.columns == list(OFFICIALS_SCHEMA.keys())
    assert df.height >= 1
    assert "Riley Johnson" in df.get_column("official").to_list()
    # never captures a nav tab as an official
    assert not set(df.get_column("official").to_list()) & {"Box Score", "Play By Play"}


def test_officials_empty() -> None:
    assert parse_cfb_ncaa_officials("").height == 0


# --- team stats (by period) ----------------------------------------------


def test_team_stats_has_per_quarter_breakdown() -> None:
    df = parse_cfb_ncaa_team_stats(_rd("team_stats"), contest_id="5362283")
    assert df.columns == list(TEAM_STATS_SCHEMA.keys())
    assert df.height > 0
    ra = df.filter(pl.col("stat") == "Rush Attempts")
    periods = set(ra.get_column("period").to_list())
    assert "total" in periods
    assert {"1st Quarter", "2nd Quarter", "3rd Quarter", "4th Quarter"} <= periods
    # every period carries an away + home value (structure faithfully extracted;
    # NCAA's per-quarter values are recorded as-is and do not necessarily sum to
    # the total, so that is not asserted).
    assert ra.get_column("away_value").is_not_null().all()
    assert ra.get_column("home_value").is_not_null().all()
    assert int(ra.filter(pl.col("period") == "total").get_column("away_value")[0]) == 34
    assert {"Rushing", "Passing", "Receiving"} <= set(df.get_column("category").unique().to_list())


def test_team_stats_teams_labeled() -> None:
    df = parse_cfb_ncaa_team_stats(_rd("team_stats"), contest_id="5362283")
    assert df.get_column("away_team").unique().to_list() == ["California"]
    assert df.get_column("home_team").unique().to_list() == ["Auburn"]


# --- individual player stats ---------------------------------------------


def test_player_stats_categories() -> None:
    d = parse_cfb_ncaa_player_stats(_rd("individual_stats"), contest_id="5362283")
    assert {"rushing", "passing", "receiving"} <= set(d.keys())
    passing = d["passing"]
    for c in ("contest_id", "team_id", "name", "position", "pass_attempts", "completions", "pass_yards"):
        assert c in passing.columns
    # Mendoza's game line: 25/36, 233, 2 TD
    m = passing.filter(pl.col("name") == "Fernando Mendoza")
    assert m.height == 1
    row = m.row(0, named=True)
    assert row["pass_attempts"] == "36" and row["completions"] == "25" and row["pass_yards"] == "233"


def test_player_stats_two_teams() -> None:
    d = parse_cfb_ncaa_player_stats(_rd("individual_stats"), contest_id="5362283")
    assert d["rushing"].get_column("team_id").n_unique() == 2


def test_player_stats_empty() -> None:
    assert parse_cfb_ncaa_player_stats("") == {}


# --- linescore ------------------------------------------------------------


def test_linescore_teams_periods_meta() -> None:
    df = parse_cfb_ncaa_linescore(_rd("box_score"), contest_id="5362283")
    assert df.columns == list(LINESCORE_SCHEMA.keys())
    assert set(df.get_column("home_away").unique().to_list()) == {"away", "home"}
    # California away, final 21; Auburn home, final 14
    cal = df.filter(pl.col("team") == "California")
    assert cal.get_column("final").unique().to_list() == [21]
    assert cal.get_column("points").cast(pl.Int64).sum() == 21  # quarters sum to final
    assert df.get_column("attendance").unique().to_list() == [88043]


def test_linescore_empty() -> None:
    df = parse_cfb_ncaa_linescore("")
    assert df.height == 0 and df.columns == list(LINESCORE_SCHEMA.keys())
