"""WBB parity: ``parse_ncaa_bb_scoreboard`` vs the wbigballR ``get_date_games`` oracle.

Oracle: ``tests/fixtures/ncaa/bigballr/oracle/wbb/date_games.csv`` — wbigballR
``get_date_games("12/05/2024")`` (all_functions.R:1100-1328) run FULLY OFFLINE
(``use_file=TRUE`` over the committed capture
``tests/fixtures/ncaa/bigballr/html/scoreboard_18423_12-05-2024.html``).
wbigballR parses through ``XML::readHTMLTable`` — the direct-cell readout this
port implements — so the comparison here is EXACT on all 14 columns (unlike
the MBB twin test, whose chromote-path oracle is tainted on 3 columns; see
``tests/mbb/test_mbb_ncaa_scoreboard_parity.py``).

The scoreboard core is league-free (spec_wbigballr_divergence.md §2.2: the two
R implementations differ only in transport + the season-id table), so this
suite drives the shared ``parse_ncaa_bb_scoreboard`` plus the WBB season-id
knob directly.
"""

from __future__ import annotations

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse.mbb.mbb_ncaa_scoreboard import (
    NCAA_MBB_SEASON_DIVISIONS,
    NCAA_WBB_SEASON_DIVISIONS,
    SCOREBOARD_SCHEMA,
    _ncaa_bb_date_games,
    parse_ncaa_bb_scoreboard,
)
from tests.mbb._bigballr_oracle import HTML_DIR
from tests.mbb.test_mbb_ncaa_scoreboard_parity import oracle_scoreboard

FIXTURE = HTML_DIR / "scoreboard_18423_12-05-2024.html"
DATE = "12/05/2024"


@pytest.fixture(scope="module")
def parsed() -> pl.DataFrame:
    return parse_ncaa_bb_scoreboard(FIXTURE.read_text(encoding="utf-8"), DATE, season_id=18423)


def test_wbb_scoreboard_full_parity(parsed: pl.DataFrame) -> None:
    """EXACT equality vs the readHTMLTable-path oracle — all 14 columns."""
    oracle = oracle_scoreboard("wbb")
    assert parsed.shape == oracle.shape == (47, 14)
    assert_frame_equal(parsed, oracle)


def test_wbb_scoreboard_schema(parsed: pl.DataFrame) -> None:
    assert dict(parsed.schema) == SCOREBOARD_SCHEMA


def test_wbb_scoreboard_spot_values(parsed: pl.DataFrame) -> None:
    """Pin the direct-cell readout on real page values (game 1: West Ga. @ UNCW)."""
    row = parsed.row(0, named=True)
    assert row["home"] == "UNCW"
    assert row["away"] == "West Ga."
    assert row["home_score"] == "66"
    assert row["away_score"] == "70"
    assert row["attendance"] == "2,810"
    assert row["neutral_site"] is False
    assert row["home_wins"] == 6 and row["home_losses"] == 4
    assert row["away_wins"] == 4 and row["away_losses"] == 3
    assert row["game_id"] == "5731691"


def test_wbb_date_games_offline_fetcher_injection() -> None:
    """The shared core with league='wbb' resolves the WBB season id (18423)."""

    class FakeFetcher:
        def __init__(self) -> None:
            self.paths: list[str] = []

        def fetch_html(self, path: str, *, force: bool = False) -> str:
            self.paths.append(path)
            return FIXTURE.read_text(encoding="utf-8")

    fake = FakeFetcher()
    df = _ncaa_bb_date_games(
        DATE,
        conference="All",
        conference_id=None,
        fetcher=fake,  # type: ignore[arg-type]
        league="wbb",
    )
    assert fake.paths == ["season_divisions/18423/scoreboards?game_date=12%2F05%2F2024&conference_id=0&commit=Submit"]
    assert df.shape == (47, 14)


def test_wbb_season_table_divergence() -> None:
    """W table lacks 2009-10 and 2025-26 (wbigballR all_functions.R:1106-1147)."""
    assert "2009-10" not in NCAA_WBB_SEASON_DIVISIONS
    assert "2025-26" not in NCAA_WBB_SEASON_DIVISIONS
    assert len(NCAA_WBB_SEASON_DIVISIONS) == 15
    assert NCAA_WBB_SEASON_DIVISIONS["2024-25"] == 18423
    assert NCAA_WBB_SEASON_DIVISIONS["2010-11"] == 10200
    # The per-league ids really differ season-by-season (the league knob).
    shared = set(NCAA_WBB_SEASON_DIVISIONS) & set(NCAA_MBB_SEASON_DIVISIONS)
    assert all(NCAA_WBB_SEASON_DIVISIONS[s] != NCAA_MBB_SEASON_DIVISIONS[s] for s in shared)


def test_wbb_unknown_season_raises_value_error() -> None:
    """2025-26 exists for MBB but not WBB — the WBB path must raise."""
    with pytest.raises(ValueError, match="Season Not Available"):
        _ncaa_bb_date_games(
            "11/11/2025",
            conference="All",
            conference_id=None,
            fetcher=None,
            league="wbb",
        )
