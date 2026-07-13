"""Smoke suite for the WBB bigballR-family shims.

Every shim must (1) import, (2) produce output identical to the shared MBB
core called with the WBB knobs bound (``period_model=(4, 600, 300)``,
``league="wbb"``) — scrapers verified on committed offline fixtures, pure
transforms verified as delegation identities on the WBB oracle pbp frame.
Deep value parity vs the R oracle lives in the ``test_wbb_ncaa_*_parity.py``
files; this suite pins the shim wiring only.
"""

from __future__ import annotations

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse.mbb.mbb_ncaa_game_pbp import _ncaa_bb_game_pbp, _ncaa_bb_play_by_play
from sportsdataverse.mbb.mbb_ncaa_lineups import (
    ncaa_mbb_lineups,
    ncaa_mbb_on_off,
    ncaa_mbb_player_combos,
    ncaa_mbb_player_lineups,
)
from sportsdataverse.mbb.mbb_ncaa_possession_seg import ncaa_mbb_possessions
from sportsdataverse.mbb.mbb_ncaa_schedule import (
    parse_ncaa_bb_team_roster,
    parse_ncaa_bb_team_schedule,
)
from sportsdataverse.mbb.mbb_ncaa_scoreboard import _ncaa_bb_date_games
from sportsdataverse.mbb.mbb_ncaa_stats_agg import ncaa_mbb_player_stats, ncaa_mbb_team_stats
from sportsdataverse.wbb.wbb_ncaa_game_pbp import ncaa_wbb_game_pbp, ncaa_wbb_play_by_play
from sportsdataverse.wbb.wbb_ncaa_lineups import (
    ncaa_wbb_lineups,
    ncaa_wbb_on_off,
    ncaa_wbb_player_combos,
    ncaa_wbb_player_lineups,
)
from sportsdataverse.wbb.wbb_ncaa_possession_seg import ncaa_wbb_possessions
from sportsdataverse.wbb.wbb_ncaa_schedule import ncaa_wbb_team_roster, ncaa_wbb_team_schedule
from sportsdataverse.wbb.wbb_ncaa_scoreboard import ncaa_wbb_date_games
from sportsdataverse.wbb.wbb_ncaa_stats_agg import ncaa_wbb_player_stats, ncaa_wbb_team_stats
from sportsdataverse.wbb.wbb_ncaa_team_ids import ncaa_wbb_team_ids, resolve_ncaa_team_id
from tests.mbb._bigballr_oracle import HTML_DIR, load_oracle_pbp

WBB_PERIOD_MODEL = (4, 600, 300)
WBB_GAME = "5722355"
WBB_TEAM_ID = 592003  # South Carolina 2024-25
SCOREBOARD_HTML = "scoreboard_18423_12-05-2024.html"


class FakePbpFetcher:
    """Offline fetcher for the game-pbp shims (fixture-backed)."""

    def fetch_game_pbp(self, game_id: object) -> str:
        return (HTML_DIR / f"pbp_{game_id}.html").read_text(encoding="utf-8")


class FakeHtmlFetcher:
    """Offline fetcher for the schedule/roster/scoreboard shims; records paths."""

    def __init__(self, filename: str) -> None:
        self._filename = filename
        self.paths: list[str] = []

    def fetch_html(self, path: str) -> str:
        self.paths.append(path)
        return (HTML_DIR / self._filename).read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def wbb_pbp() -> pl.DataFrame:
    """WBB oracle pbp frame — the canonical transform input."""
    return load_oracle_pbp("wbb")


@pytest.fixture(scope="module")
def wbb_lineups(wbb_pbp: pl.DataFrame) -> pl.DataFrame:
    return ncaa_mbb_lineups(wbb_pbp)


# ---------------------------------------------------------------------------
# scraper shims: fixture-based equality against the league-parameterized core
# ---------------------------------------------------------------------------


def test_ncaa_wbb_game_pbp_matches_core_quarter_model() -> None:
    """Shim == private core with period_model=(4, 600, 300) on a real capture."""
    expected = _ncaa_bb_game_pbp(WBB_GAME, fetcher=FakePbpFetcher(), period_model=WBB_PERIOD_MODEL)
    got = ncaa_wbb_game_pbp(WBB_GAME, fetcher=FakePbpFetcher())
    assert got.height > 0
    assert_frame_equal(got, expected)
    # quarters fix: a regulation WBB game must show 4 periods, not 2 halves
    # (wbigballR's halves math misreads this page as 2-OT; design.md).
    assert got["period"].max() == 4


def test_ncaa_wbb_play_by_play_matches_core_quarter_model() -> None:
    expected = _ncaa_bb_play_by_play([WBB_GAME], fetcher=FakePbpFetcher(), period_model=WBB_PERIOD_MODEL)
    got = ncaa_wbb_play_by_play([WBB_GAME], fetcher=FakePbpFetcher())
    assert got.height > 0
    assert_frame_equal(got, expected)


def test_ncaa_wbb_date_games_matches_core_wbb_league() -> None:
    expected = _ncaa_bb_date_games(
        "12/05/2024",
        conference="All",
        conference_id=None,
        fetcher=FakeHtmlFetcher(SCOREBOARD_HTML),
        league="wbb",
    )
    fake = FakeHtmlFetcher(SCOREBOARD_HTML)
    got = ncaa_wbb_date_games("12/05/2024", fetcher=fake)
    assert got.height > 0
    assert_frame_equal(got, expected)
    # league="wbb" selects the WBB season_divisions id (18423 for 2024-25).
    assert fake.paths[0].startswith("season_divisions/18423/scoreboards")


def test_ncaa_wbb_date_games_missing_seasons_raise() -> None:
    """WBB season table lacks 2009-10 and 2025-26 (module docstring caveat)."""
    for date in ("12/05/2009", "12/05/2025"):
        with pytest.raises(ValueError, match="Season Not Available"):
            ncaa_wbb_date_games(date, fetcher=FakeHtmlFetcher(SCOREBOARD_HTML))


def test_ncaa_wbb_team_ids_table() -> None:
    df = ncaa_wbb_team_ids()
    assert df.height == 5613
    assert df.columns == ["team", "conference", "id", "season"]
    assert df.schema["id"] == pl.Int64


def test_ncaa_wbb_team_schedule_matches_core() -> None:
    html = (HTML_DIR / f"team_{WBB_TEAM_ID}.html").read_text(encoding="utf-8")
    expected = parse_ncaa_bb_team_schedule(html, WBB_TEAM_ID, league="wbb")
    got = ncaa_wbb_team_schedule(WBB_TEAM_ID, fetcher=FakeHtmlFetcher(f"team_{WBB_TEAM_ID}.html"))
    assert got.height > 0
    assert_frame_equal(got, expected)


def test_ncaa_wbb_team_schedule_resolves_wbb_table() -> None:
    """Name resolution hits the WOMEN'S crosswalk (deliberate wbigballR fix).

    wbigballR resolved names through bigballR::teamids (men's table), which
    for South Carolina 2024-25 yields 590724; the WBB table yields 592003.
    """
    assert resolve_ncaa_team_id("South Carolina", "2024-25", league="wbb") == WBB_TEAM_ID
    assert resolve_ncaa_team_id("South Carolina", "2024-25", league="mbb") != WBB_TEAM_ID
    fake = FakeHtmlFetcher(f"team_{WBB_TEAM_ID}.html")
    ncaa_wbb_team_schedule(team="South Carolina", season="2024-25", fetcher=fake)
    assert fake.paths == [f"teams/{WBB_TEAM_ID}"]


def test_ncaa_wbb_team_roster_matches_core() -> None:
    html = (HTML_DIR / f"roster_{WBB_TEAM_ID}.html").read_text(encoding="utf-8")
    expected = parse_ncaa_bb_team_roster(html, WBB_TEAM_ID)
    fake = FakeHtmlFetcher(f"roster_{WBB_TEAM_ID}.html")
    got = ncaa_wbb_team_roster(team="South Carolina", season="2024-25", fetcher=fake)
    assert got.height > 0
    assert_frame_equal(got, expected)
    assert fake.paths == [f"teams/{WBB_TEAM_ID}/roster"]


# ---------------------------------------------------------------------------
# transform shims: delegation identity (same frame in -> equal frame out)
# ---------------------------------------------------------------------------


def test_ncaa_wbb_lineups_delegates(wbb_pbp: pl.DataFrame, wbb_lineups: pl.DataFrame) -> None:
    assert wbb_lineups.height > 0
    assert_frame_equal(ncaa_wbb_lineups(wbb_pbp), wbb_lineups)


def test_ncaa_wbb_player_lineups_delegates(wbb_lineups: pl.DataFrame) -> None:
    player = wbb_lineups["p1"][0]
    assert_frame_equal(
        ncaa_wbb_player_lineups(wbb_lineups, included=player),
        ncaa_mbb_player_lineups(wbb_lineups, included=player),
    )


def test_ncaa_wbb_player_combos_delegates(wbb_lineups: pl.DataFrame) -> None:
    assert_frame_equal(
        ncaa_wbb_player_combos(wbb_lineups, n=2),
        ncaa_mbb_player_combos(wbb_lineups, n=2),
    )


def test_ncaa_wbb_on_off_delegates(wbb_lineups: pl.DataFrame) -> None:
    player = wbb_lineups["p1"][0]
    assert_frame_equal(
        ncaa_wbb_on_off(player, wbb_lineups),
        ncaa_mbb_on_off(player, wbb_lineups),
    )


def test_ncaa_wbb_player_stats_delegates(wbb_pbp: pl.DataFrame) -> None:
    got = ncaa_wbb_player_stats(wbb_pbp)
    assert got.height > 0
    assert_frame_equal(got, ncaa_mbb_player_stats(wbb_pbp))


def test_ncaa_wbb_team_stats_delegates(wbb_pbp: pl.DataFrame) -> None:
    got = ncaa_wbb_team_stats(wbb_pbp)
    assert got.height > 0
    assert_frame_equal(got, ncaa_mbb_team_stats(wbb_pbp))


def test_ncaa_wbb_possessions_delegates(wbb_pbp: pl.DataFrame) -> None:
    got = ncaa_wbb_possessions(wbb_pbp)
    assert got.height > 0
    assert_frame_equal(got, ncaa_mbb_possessions(wbb_pbp))


def test_return_as_pandas_passthrough(wbb_pbp: pl.DataFrame) -> None:
    """Every shim's return_as_pandas kwarg reaches the core."""
    import pandas as pd

    assert isinstance(ncaa_wbb_team_ids(return_as_pandas=True), pd.DataFrame)
    assert isinstance(ncaa_wbb_possessions(wbb_pbp, return_as_pandas=True), pd.DataFrame)
