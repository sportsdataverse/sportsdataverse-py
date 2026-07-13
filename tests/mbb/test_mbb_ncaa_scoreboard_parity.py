"""MBB parity: ``parse_ncaa_bb_scoreboard`` vs the bigballR ``get_date_games`` oracle.

Oracle: ``tests/fixtures/ncaa/bigballr/oracle/mbb/date_games.csv`` — bigballR
``get_date_games("11/11/2025")`` run through the CHROMOTE path
(``rvest::html_table``) over the committed capture
``tests/fixtures/ncaa/bigballr/html/scoreboard_18703_11-11-2025.html``.

**Tainted-column note (mirrors ``WBB_CLOCK_TAINTED``'s precedent).** The
chromote/rvest path expands the nested per-half line-score table's cells into
the parent row, so the oracle's ``Away_Score`` / ``Neutral_Site`` /
``Attendance`` are wrong-by-construction: ``Away_Score`` holds the away
FIRST-HALF score, ``Attendance`` holds the home first-half score, and
``Neutral_Site`` is TRUE for every played game (the away second-half score
cell is never empty). The Python port implements the direct-cell readout the
V1..V7 field positions were designed against (and which the wbigballR /
``XML::readHTMLTable`` oracle confirms exactly — see the WBB twin test), so
those three columns are compared by invariant here and exactly in the WBB
suite. See the module docstring of
``sportsdataverse/mbb/mbb_ncaa_scoreboard.py``.
"""

from __future__ import annotations

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse.mbb.mbb_ncaa_scoreboard import (
    NCAA_MBB_SEASON_DIVISIONS,
    SCOREBOARD_SCHEMA,
    ncaa_mbb_date_games,
    parse_ncaa_bb_scoreboard,
)
from tests.mbb._bigballr_oracle import HTML_DIR, load_oracle

FIXTURE = HTML_DIR / "scoreboard_18703_11-11-2025.html"
DATE = "11/11/2025"

#: R contract -> sdv-py snake_case contract (shared with the WBB twin test).
SCOREBOARD_RENAME: dict[str, str] = {
    "Date": "date",
    "Start_Time": "start_time",
    "Home": "home",
    "Away": "away",
    "BoxID": "box_id",
    "GameID": "game_id",
    "Home_Score": "home_score",
    "Away_Score": "away_score",
    "Attendance": "attendance",
    "Neutral_Site": "neutral_site",
    "Home_Wins": "home_wins",
    "Home_Losses": "home_losses",
    "Away_Wins": "away_wins",
    "Away_Losses": "away_losses",
}

#: Oracle columns polluted by the chromote/rvest nested-table expansion.
MBB_SCOREBOARD_TAINTED = ["away_score", "neutral_site", "attendance"]


def oracle_scoreboard(league: str) -> pl.DataFrame:
    """date_games oracle renamed + cast to the SCOREBOARD_SCHEMA contract.

    ``read_csv`` infers all-numeric chr columns (ids, scores, MBB attendance)
    as Int64 — casting back to the contract's Utf8 restores R's character
    values exactly (they are plain integers, never float-formatted).
    """
    df = load_oracle("date_games", league).rename(SCOREBOARD_RENAME)
    return df.with_columns([pl.col(c).cast(dtype) for c, dtype in SCOREBOARD_SCHEMA.items()]).select(
        list(SCOREBOARD_SCHEMA)
    )


@pytest.fixture(scope="module")
def parsed() -> pl.DataFrame:
    return parse_ncaa_bb_scoreboard(FIXTURE.read_text(encoding="utf-8"), DATE, season_id=18703)


def test_mbb_scoreboard_parity_untainted_columns(parsed: pl.DataFrame) -> None:
    """Exact equality on the 11 columns the chromote oracle got right."""
    oracle = oracle_scoreboard("mbb")
    assert parsed.shape == oracle.shape == (82, 14)
    keep = [c for c in SCOREBOARD_SCHEMA if c not in MBB_SCOREBOARD_TAINTED]
    assert_frame_equal(parsed.select(keep), oracle.select(keep))


def test_mbb_scoreboard_schema(parsed: pl.DataFrame) -> None:
    assert dict(parsed.schema) == SCOREBOARD_SCHEMA


def test_mbb_scoreboard_tainted_columns_direct_readout(parsed: pl.DataFrame) -> None:
    """The 3 oracle-tainted columns carry the DIRECT-cell (correct) values.

    Fixture game 1 is IU Columbus @ IU Indy, final 77-121, attendance 5,568:
    the oracle recorded away_score=38 (away H1), attendance=48 (home H1) and
    neutral_site=TRUE — this pins the port to the real page values instead.
    """
    row = parsed.row(0, named=True)
    assert row["away"] == "IU Columbus"
    assert row["away_score"] == "77"  # oracle: "38" (away first-half score)
    assert row["home_score"] == "121"
    assert row["attendance"] == "5,568"  # oracle: "48" (home first-half score)
    assert row["neutral_site"] is False  # oracle: TRUE for every played game
    # Every played game on this slate: numeric final scores, no nulls.
    assert parsed["away_score"].str.contains(r"^\d+$").all()
    assert parsed["attendance"].str.contains(r"^[\d,]+$").all()
    assert parsed["neutral_site"].null_count() == 0
    # The oracle's taint, documented: away H1 <= the real final.
    oracle = oracle_scoreboard("mbb")
    assert (oracle["away_score"].cast(pl.Int64) <= parsed["away_score"].cast(pl.Int64)).all()


def test_mbb_scoreboard_game_ids_all_assigned(parsed: pl.DataFrame) -> None:
    """82 /contests/ links -> 82 games, ids equal the oracle's exactly."""
    oracle = oracle_scoreboard("mbb")
    assert parsed["game_id"].null_count() == 0
    assert parsed["game_id"].to_list() == oracle["game_id"].to_list()
    assert parsed["box_id"].to_list() == parsed["game_id"].to_list()


def test_mbb_date_games_offline_fetcher_injection() -> None:
    """URL construction + end-to-end through the public fn with a fake fetcher."""

    class FakeFetcher:
        def __init__(self) -> None:
            self.paths: list[str] = []

        def fetch_html(self, path: str, *, force: bool = False) -> str:
            self.paths.append(path)
            return FIXTURE.read_text(encoding="utf-8")

    fake = FakeFetcher()
    df = ncaa_mbb_date_games(DATE, fetcher=fake)  # type: ignore[arg-type]
    assert fake.paths == ["season_divisions/18703/scoreboards?game_date=11%2F11%2F2025&conference_id=0&commit=Submit"]
    assert df.shape == (82, 14)


def test_mbb_unknown_season_raises_value_error() -> None:
    """Deliberate fix of R's type-unstable string return 'Season Not Available'."""
    with pytest.raises(ValueError, match="Season Not Available"):
        ncaa_mbb_date_games("01/01/2005")


def test_mbb_season_table_boundaries() -> None:
    """May-1 season boundary + table endpoints (all_functions.R:1132-1183)."""
    assert NCAA_MBB_SEASON_DIVISIONS["2025-26"] == 18703
    assert NCAA_MBB_SEASON_DIVISIONS["2009-10"] == 10060
    assert len(NCAA_MBB_SEASON_DIVISIONS) == 17
