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
    # the record in the page header (A&M-Corpus Christi, 23-28)
    assert dict(df.group_by("outcome").len().rows()) == {"W": 23, "L": 28}


def test_team_schedule_older_layout_2016() -> None:
    """2016 pages put a title row before the <th> header row (a skip-the-first-row
    rule parsed it as a game dated "Date") and print results as "W 17 - 2"."""
    df = parse_ncaa_team_schedule(_rd("mba_team_page_2016_85309"), team_id="85309")
    assert df.columns == list(TEAM_SCHEDULE_SCHEMA.keys())
    assert df.height == 73
    assert df.get_column("date").str.contains(r"^\d{2}/\d{2}/\d{4}").all()  # no "Date" header row
    # Coastal Carolina went 55-18 and won the 2016 College World Series
    assert dict(df.group_by("outcome").len().rows()) == {"W": 55, "L": 18}
    assert df.get_column("team_score").null_count() == 0
    assert df.get_column("contest_id").drop_nulls().len() == 72
    assert df.get_column("team_name")[0] == "Coastal Carolina Chanticleers"


def test_team_schedule_without_a_header_row_keeps_its_first_game() -> None:
    # headers are skipped by structure (<th>-only rows), not by position
    html = (
        "<table><tr><td>03/01/2016</td><td><a href='/teams/1'>Foo</a></td>"
        "<td><a href='/contests/9/box_score'>W 3 - 1</a></td></tr></table>"
    )
    df = parse_ncaa_team_schedule(html, team_id="2")
    assert df.select("date", "outcome", "contest_id").rows() == [("03/01/2016", "W", "9")]


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
