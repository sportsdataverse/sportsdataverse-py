"""Offline tests for the sport-generic stats.ncaa.org reference parsers.

Fixtures are real baseball (MBA) captures -- the parsers graduated from the
MFB producer, so passing on a second sport is the sport-generic proof.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.scrape.ncaa.reference import (
    TEAM_LIST_SCHEMA,
    TEAM_ROSTER_SCHEMA,
    TEAM_SCHEDULE_SCHEMA,
    parse_ncaa_team_list,
    parse_ncaa_team_roster,
    parse_ncaa_team_schedule,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa_reference"


def _rd(name: str) -> str:
    return (FIX / f"{name}.html").read_text(encoding="utf-8")


def test_team_list_schema_and_rows() -> None:
    df = parse_ncaa_team_list(_rd("mba_team_list_2026_d1"))
    assert df.columns == list(TEAM_LIST_SCHEMA.keys())
    assert df.height == 308  # 2026 D-I baseball
    assert df.get_column("team_id").str.contains(r"^\d+$").all()


def test_team_schedule_doubleheaders_and_results() -> None:
    df = parse_ncaa_team_schedule(_rd("mba_team_page_614839"), team_id="614839")
    assert df.columns == list(TEAM_SCHEDULE_SCHEMA.keys())
    assert df.height >= 50  # full baseball season
    dh = df.filter(pl.col("game_number").is_not_null())
    assert dh.height >= 2  # doubleheader pairs
    assert set(dh.get_column("game_number").unique().to_list()) <= {1, 2}
    played = df.filter(pl.col("outcome").is_not_null())
    assert (played.get_column("team_score") >= 0).all()
    assert played.get_column("contest_id").str.contains(r"^\d+$").all()


def test_team_roster_header_keyed() -> None:
    df = parse_ncaa_team_roster(_rd("mba_roster_614839"), team_id="614839")
    assert df.columns == list(TEAM_ROSTER_SCHEMA.keys())
    assert df.height >= 30
    assert df.get_column("player_name").is_not_null().all()
    assert "P" in df.get_column("position").to_list()


def test_empty_inputs_zero_row_with_schema() -> None:
    assert parse_ncaa_team_list("").columns == list(TEAM_LIST_SCHEMA.keys())
    assert parse_ncaa_team_schedule("").height == 0
    assert parse_ncaa_team_roster("").height == 0


def test_team_name_strips_record_and_trailing_junk() -> None:
    # CodeRabbit PR #390: header continues past the W-L record
    # ("A&M-Corpus Christi (23-28) RPI Ranking - 202") -- name only survives
    df = parse_ncaa_team_schedule(_rd("mba_team_page_614839"), team_id="614839")
    assert df.get_column("team_name").unique().to_list() == ["A&M-Corpus Christi Islanders"]
    ro = parse_ncaa_team_roster(_rd("mba_roster_614839"), team_id="614839")
    assert ro.get_column("team_name").unique().to_list() == ["A&M-Corpus Christi Islanders"]
