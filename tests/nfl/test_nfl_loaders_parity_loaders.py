"""Smoke tests for the NFL loaders ported from nflreadpy.

Each loader gets a single live-call test that verifies:
- The function returns a non-empty `pl.DataFrame`.
- The dataframe is the polars type (not pandas) when `return_as_pandas`
  defaults to False.

Column-name assertions are intentionally avoided — the upstream
nflverse parquet schemas drift and we don't want spurious failures.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from sportsdataverse.nfl import (
    load_nfl_ff_opportunity,
    load_nfl_ff_playerids,
    load_nfl_ff_rankings,
    load_nfl_ftn_charting,
    load_nfl_team_stats,
    load_nfl_trades,
)
from sportsdataverse.nfl.utils_date import (
    get_current_nfl_season,
    get_current_nfl_week,
    most_recent_nfl_season,
)
from tests.conftest import skip_if_no_live


@skip_if_no_live
def test_load_nfl_team_stats_week_2024():
    df = load_nfl_team_stats(seasons=[2024], summary_level="week")
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0


@skip_if_no_live
def test_load_nfl_ftn_charting_2024():
    df = load_nfl_ftn_charting(seasons=[2024])
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0


@skip_if_no_live
def test_load_nfl_trades():
    df = load_nfl_trades()
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0


@skip_if_no_live
def test_load_nfl_ff_playerids():
    df = load_nfl_ff_playerids()
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0


@skip_if_no_live
def test_load_nfl_ff_rankings_draft():
    df = load_nfl_ff_rankings(type="draft")
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0


@skip_if_no_live
def test_load_nfl_ff_opportunity_2024_weekly():
    df = load_nfl_ff_opportunity(seasons=[2024], stat_type="weekly", model_version="latest")
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0


# Date utility tests — pure date math, safe to run unconditionally.


def test_get_current_nfl_season_returns_int():
    season = get_current_nfl_season()
    assert isinstance(season, int)
    # Should be either current calendar year or previous, never further off.
    today_year = date.today().year
    assert season in (today_year - 1, today_year)


def test_get_current_nfl_season_roster_flag_is_independent():
    s = get_current_nfl_season(roster=False)
    r = get_current_nfl_season(roster=True)
    assert isinstance(s, int)
    assert isinstance(r, int)
    # Both within one year of today.
    today_year = date.today().year
    assert s in (today_year - 1, today_year)
    assert r in (today_year - 1, today_year)


def test_get_current_nfl_week_use_date_returns_int_in_range():
    wk = get_current_nfl_week(use_date=True)
    assert isinstance(wk, int)
    assert 1 <= wk <= 22


def test_most_recent_nfl_season_alias_matches():
    assert most_recent_nfl_season() == get_current_nfl_season()
