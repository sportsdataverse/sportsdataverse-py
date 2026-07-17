"""Offline tests for the stats.ncaa.org college-baseball box-score parsers.

Parses a committed real capture (contest 6357953, Kansas @ A&M-Corpus Christi
2025-02-14, a 10-inning game): the box_score linescore, team_stats (by inning),
individual_stats (batting/pitching/fielding), and situational_stats tabs.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.baseball.college_baseball.college_baseball_ncaa_box import (
    LINESCORE_SCHEMA,
    TEAM_STATS_SCHEMA,
    parse_college_baseball_ncaa_linescore,
    parse_college_baseball_ncaa_player_stats,
    parse_college_baseball_ncaa_situational_stats,
    parse_college_baseball_ncaa_team_stats,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "college_baseball_ncaa"


def _rd(name: str) -> str:
    return (FIX / f"bsb_{name}_6357953.html").read_text(encoding="utf-8")


# --- linescore (innings + R/H/E) ------------------------------------------


def test_linescore_innings_rhe_and_meta() -> None:
    df = parse_college_baseball_ncaa_linescore(_rd("box_score"), contest_id="6357953")
    assert df.columns == list(LINESCORE_SCHEMA.keys())
    assert set(df.get_column("home_away").unique().to_list()) == {"away", "home"}
    ks = df.filter(pl.col("team") == "Kansas")
    # 10-inning game; per-inning runs sum to the R total (8), H=11, E=2
    assert ks.get_column("inning").to_list() == [str(i) for i in range(1, 11)]
    assert ks.get_column("runs").sum() == 8
    assert ks.get_column("runs_total").unique().to_list() == [8]
    assert ks.get_column("hits").unique().to_list() == [11]
    assert ks.get_column("errors").unique().to_list() == [2]
    assert df.get_column("attendance").unique().to_list() == [284]


def test_linescore_empty() -> None:
    df = parse_college_baseball_ncaa_linescore("")
    assert df.height == 0 and df.columns == list(LINESCORE_SCHEMA.keys())


# --- team stats (by inning) ----------------------------------------------


def test_team_stats_by_inning() -> None:
    df = parse_college_baseball_ncaa_team_stats(_rd("team_stats"), contest_id="6357953")
    assert df.columns == list(TEAM_STATS_SCHEMA.keys())
    assert df.height > 0
    periods = set(df.get_column("period").unique().to_list())
    assert "total" in periods
    assert {"1st Inning", "2nd Inning", "10th Inning"} <= periods
    assert df.get_column("away_team").drop_nulls().unique().to_list() == ["Kansas"]


def test_team_stats_empty() -> None:
    df = parse_college_baseball_ncaa_team_stats("")
    assert df.height == 0 and df.columns == list(TEAM_STATS_SCHEMA.keys())


# --- individual player stats (batting / pitching / fielding) --------------


def test_player_stats_batting_pitching_fielding() -> None:
    d = parse_college_baseball_ncaa_player_stats(_rd("individual_stats"), contest_id="6357953")
    assert {"batting", "pitching", "fielding"} == set(d.keys())
    for c in ("contest_id", "team_id", "name", "position", "ab", "h", "rbi"):
        assert c in d["batting"].columns
    for c in ("ip", "er", "so", "bb"):
        assert c in d["pitching"].columns
    for c in ("po", "a", "tc", "e"):
        assert c in d["fielding"].columns
    # both teams present; a known batter resolves
    assert d["batting"].get_column("team_id").n_unique() == 2
    assert "Brady Ballinger" in d["batting"].get_column("name").to_list()


def test_player_stats_empty() -> None:
    assert parse_college_baseball_ncaa_player_stats("") == {}


# --- situational splits ----------------------------------------------------


def test_situational_batting_pitching_fielding() -> None:
    d = parse_college_baseball_ncaa_situational_stats(_rd("situational_stats"), contest_id="6357953")
    assert {"batting", "pitching", "fielding"} == set(d.keys())
    assert "vs_lhp" in d["batting"].columns and "vs_rhp" in d["batting"].columns
    assert "vs_lhb" in d["pitching"].columns
    assert d["batting"].height > 0
    # never miscategorized as "other"
    assert "other" not in d


def test_situational_empty() -> None:
    assert parse_college_baseball_ncaa_situational_stats("") == {}
