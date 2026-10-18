"""``parse_ncaa_bb_team_schedule`` page-format guard (B-GB-27).

Parity with bigballR lives in ``test_mbb_ncaa_schedule_parity.py``. The 2016 baseball team
page is a real capture in the older one-row-per-game layout: before the guard it raised
``ComputeError`` (a str team id) or mis-paired 36 of 73 games (an int one).
"""

from __future__ import annotations

import re
import warnings
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.errors import UnsupportedPageFormatWarning
from sportsdataverse.mbb.mbb_ncaa_schedule import _SCHEDULE_SCHEMA, parse_ncaa_bb_team_schedule
from tests.mbb._bigballr_oracle import HTML_DIR

PAGE_2016 = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa_reference" / "mba_team_page_2016_85309.html"
#: Real 2016-season MBB team page (tests/fixtures/ncaa_reference/README.md), served in the current layout.
MBB_PAGE_2016 = PAGE_2016.with_name("mbb_team_page_2016_21731.html")


@pytest.mark.parametrize("team_id", [85309, "85309"])
def test_format_guard_2016_team_page_warns_and_returns_zero_row_schema(team_id: object) -> None:
    html = PAGE_2016.read_text(encoding="utf-8")
    with pytest.warns(UnsupportedPageFormatWarning, match=r"team 85309 is the older stats\.ncaa\.org team-page layout"):
        got = parse_ncaa_bb_team_schedule(html, team_id)  # type: ignore[arg-type]
    assert got.height == 0
    assert got.schema == pl.Schema(_SCHEDULE_SCHEMA)


def test_format_guard_silent_on_supported_page() -> None:
    html = (HTML_DIR / "team_609554.html").read_text(encoding="utf-8")
    with warnings.catch_warnings():
        warnings.simplefilter("error", UnsupportedPageFormatWarning)
        got = parse_ncaa_bb_team_schedule(html, "609554")  # type: ignore[arg-type]
    assert got.height > 0
    assert got["home"].null_count() < got.height  # the str id still resolves the team name


def test_format_guard_silent_on_2016_basketball_team_page() -> None:
    """A 2016-season basketball team page is the current layout, unlike baseball's: read, not guarded (R853-4)."""
    html = MBB_PAGE_2016.read_text(encoding="utf-8")
    with warnings.catch_warnings():
        warnings.simplefilter("error", UnsupportedPageFormatWarning)
        got = parse_ncaa_bb_team_schedule(html, "21731")  # type: ignore[arg-type]
    contest_ids = set(re.findall(r"contests/(\d+)/box_score", html))
    assert got.height == len(contest_ids) == 31
    assert set(got["game_id"].to_list()) == contest_ids
    assert got["home"].null_count() < got.height
    assert got["game_date"].str.slice(6).is_in(["2015", "2016"]).all()
