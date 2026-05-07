"""Smoke tests for sportsdataverse.wbb.wbb_standings."""

from __future__ import annotations

import polars as pl

from sportsdataverse.wbb import espn_wbb_standings
from tests.conftest import skip_if_no_live

CORE_COLUMNS: set[str] = {
    "team_id",
    "team_uid",
    "team_location",
    "team_name",
    "team_abbreviation",
    "team_display_name",
    "wins",
    "losses",
    "win_percent",
    "season",
}


@skip_if_no_live
def test_espn_wbb_standings_returns_polars_by_default():
    df = espn_wbb_standings(season=2024, group=50)
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] > 0
    missing = CORE_COLUMNS - set(df.columns)
    assert not missing, f"missing columns: {missing}"
    assert df["season"].unique().to_list() == [2024]


@skip_if_no_live
def test_espn_wbb_standings_pandas_round_trip():
    import pandas as pd

    df = espn_wbb_standings(season=2024, group=50, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    missing = CORE_COLUMNS - set(df.columns)
    assert not missing, f"missing columns: {missing}"


@skip_if_no_live
def test_espn_wbb_standings_raw_returns_dict():
    raw = espn_wbb_standings(season=2024, group=50, raw=True)
    assert isinstance(raw, dict)
    # ESPN nests conferences under children[]; tolerate either shape.
    assert "children" in raw or "standings" in raw
