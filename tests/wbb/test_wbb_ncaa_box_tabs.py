"""WBB box-tab parsers are a by-reference re-export of the shared basketball
parsers (the men's and women's pages are league-agnostic). Pins the identity +
exercises them on the real WBB capture (contest 5722355)."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_box_tabs import (
    parse_ncaa_bb_linescore,
    parse_ncaa_bb_officials,
)
from sportsdataverse.wbb.wbb_ncaa_box_tabs import (
    OFFICIALS_SCHEMA,
    parse_ncaa_wbb_linescore,
    parse_ncaa_wbb_officials,
    parse_ncaa_wbb_team_stats,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "mbb_ncaa"


def test_wbb_box_tabs_are_bb_by_reference() -> None:
    assert parse_ncaa_wbb_officials is parse_ncaa_bb_officials
    assert parse_ncaa_wbb_linescore is parse_ncaa_bb_linescore


def test_wbb_officials_on_real_wbb_capture() -> None:
    html = (FIX / "bkb_officials_5722355.html").read_text(encoding="utf-8")
    df = parse_ncaa_wbb_officials(html, contest_id="5722355")
    assert df.columns == list(OFFICIALS_SCHEMA.keys())
    assert df.height == 3


def test_wbb_linescore_four_quarters() -> None:
    html = (FIX / "bkb_box_score_5722355.html").read_text(encoding="utf-8")
    df = parse_ncaa_wbb_linescore(html, contest_id="5722355")
    # WBB runs four quarters
    assert df.filter(pl.col("team") == "South Carolina").get_column("period").to_list() == ["1", "2", "3", "4"]


def test_wbb_empty_contracts() -> None:
    assert parse_ncaa_wbb_team_stats("").height == 0
