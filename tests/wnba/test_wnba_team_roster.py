"""Smoke tests for sportsdataverse.wnba.wnba_team_roster."""

from __future__ import annotations

import polars as pl

from sportsdataverse.wnba import espn_wnba_team_roster
from tests.conftest import skip_if_no_live

CORE_COLUMNS: set[str] = {
    "athlete_id",
    "full_name",
    "jersey",
    "position_abbreviation",
    "team_id",
    "season",
}


@skip_if_no_live
def test_espn_wnba_team_roster_returns_polars_with_core_columns():
    df = espn_wnba_team_roster(team_id=3, season=2024)
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] > 0
    missing = CORE_COLUMNS - set(df.columns)
    assert not missing, f"missing columns: {missing}"
    assert df["season"].unique().to_list() == [2024]
    assert df["team_id"].unique().to_list() == [3]


@skip_if_no_live
def test_espn_wnba_team_roster_raw_returns_dict():
    raw = espn_wnba_team_roster(team_id=3, season=2024, raw=True)
    assert isinstance(raw, dict)
    # ESPN sometimes nests differently, so accept either top-level shape.
    assert "athletes" in raw or "team" in raw


@skip_if_no_live
def test_espn_wnba_team_roster_return_as_pandas_returns_pandas():
    import pandas as pd

    df = espn_wnba_team_roster(team_id=3, season=2024, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
