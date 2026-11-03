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
    """The swept table reproduces wbigballR's hand-kept ids, and differs from the men's season by season.

    wbigballR (a stale fork) lacked 2009-10 and 2025-26; the table read off stats.ncaa.org has
    every season the site lists, so neither gap remains.
    """
    oracle = {
        "2010-11": 10200,
        "2011-12": 10520,
        "2012-13": 10882,
        "2013-14": 11660,
        "2014-15": 12321,
        "2015-16": 12701,
        "2016-17": 13120,
        "2017-18": 14100,
        "2018-19": 16720,
        "2019-20": 17001,
        "2020-21": 17440,
        "2021-22": 17763,
        "2022-23": 17941,
        "2023-24": 18220,
        "2024-25": 18423,
        "2025-26": 18704,
    }  # fmt: skip  -- wbigballR's hand-kept table (all_functions.R:1106-1147) + the 2026-08-01 backfill
    assert {k: NCAA_WBB_SEASON_DIVISIONS.get(k) for k in oracle} == oracle
    assert NCAA_WBB_SEASON_DIVISIONS["2026-27"] == 19003
    # The per-league ids really differ season-by-season (the league knob).
    shared = set(NCAA_WBB_SEASON_DIVISIONS) & set(NCAA_MBB_SEASON_DIVISIONS)
    assert all(NCAA_WBB_SEASON_DIVISIONS[s] != NCAA_MBB_SEASON_DIVISIONS[s] for s in shared)


def test_wbb_unknown_season_raises_value_error() -> None:
    """A season stats.ncaa.org never listed raises after one lookup, never a scoreboard fetch.

    1990 predates the site's WBB scoreboards (the swept table starts at 2009-10). The stub serves
    the site's answer to an unknown year, its current season, which must not be taken as 1990.
    """
    paths: "list[str]" = []

    class _Stub:
        def fetch_html(self, path: str) -> str:
            paths.append(path)
            return FIXTURE.read_text(encoding="utf-8")

    with pytest.raises(ValueError, match="Season Not Available"):
        _ncaa_bb_date_games(
            "12/05/1990",
            conference="All",
            conference_id=None,
            fetcher=_Stub(),
            league="wbb",
        )
    assert len(paths) == 1 and "academic_year=1991" in paths[0]  # the lookup only
