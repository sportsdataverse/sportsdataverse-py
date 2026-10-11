"""Zero-row contract of the bigballR-port stats.ncaa.org parsers (B-L44, B-L45).

A wrong page type or a non-HTML payload (``None`` / list / dict from a failed
fetch) returns a zero-row frame with the documented schema, like the 22 sibling
NCAA HTML parsers, instead of raising.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from sportsdataverse.mbb.mbb_ncaa_box_stats import parse_ncaa_bb_box
from sportsdataverse.mbb.mbb_ncaa_game_pbp import parse_ncaa_bb_game_pbp
from sportsdataverse.mbb.mbb_ncaa_schedule import parse_ncaa_bb_team_roster, parse_ncaa_bb_team_schedule
from sportsdataverse.mbb.mbb_ncaa_shots import parse_ncaa_bb_shots

FIX = Path(__file__).resolve().parents[1] / "fixtures"


@pytest.mark.parametrize("page", ["ncaa/test_lineup.html", "cfb_ncaa/mfb_box_score_5362283.html"])
def test_box_parser_other_page_type_is_zero_rows(page: str) -> None:
    """B-L44: real pages with >= 5 tables but no individual_stats layout (IndexError / ValueError before)."""
    df = parse_ncaa_bb_box((FIX / page).read_text(encoding="utf-8"), "x")
    assert df.height == 0
    assert df.columns == parse_ncaa_bb_box("", "x").columns


@pytest.mark.parametrize(
    "parse",
    [
        lambda h: parse_ncaa_bb_team_schedule(h, 1),
        lambda h: parse_ncaa_bb_team_roster(h, 1),
        lambda h: parse_ncaa_bb_box(h, "x"),
        lambda h: parse_ncaa_bb_game_pbp(h, "x"),
        lambda h: parse_ncaa_bb_shots(h, "x"),
    ],
    ids=["team_schedule", "team_roster", "box", "game_pbp", "shots"],
)
@pytest.mark.parametrize("payload", [None, [], {}], ids=["none", "list", "dict"])
def test_bigballr_ports_non_html_payload_is_zero_rows(parse, payload) -> None:
    """B-L45: TypeError / AttributeError / KeyError before."""
    df = parse(payload)
    assert df.height == 0
    assert df.columns == parse("").columns
