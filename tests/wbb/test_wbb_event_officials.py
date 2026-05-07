"""Smoke tests for sportsdataverse.wbb.wbb_event_officials."""

from __future__ import annotations

import polars as pl

from sportsdataverse.wbb import espn_wbb_event_officials
from tests.conftest import skip_if_no_live

# 2024 NCAA Division I women's basketball championship game (Iowa vs South Carolina).
GAME_ID = 401637613
SEASON = 2024

CORE_COLUMNS: set[str] = {
    "game_id",
    "season",
    "official_id",
    "full_name",
    "display_name",
    "position_name",
    "order",
}


@skip_if_no_live
def test_espn_wbb_event_officials_returns_polars_by_default():
    df = espn_wbb_event_officials(game_id=GAME_ID, season=SEASON)
    assert isinstance(df, pl.DataFrame)
    missing = CORE_COLUMNS - set(df.columns)
    assert not missing, f"missing columns: {missing}"
    if df.height > 0:
        assert df["game_id"].unique().to_list() == [GAME_ID]
        assert df["season"].unique().to_list() == [SEASON]


@skip_if_no_live
def test_espn_wbb_event_officials_pandas_round_trip():
    import pandas as pd

    df = espn_wbb_event_officials(game_id=GAME_ID, season=SEASON, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    missing = CORE_COLUMNS - set(df.columns)
    assert not missing, f"missing columns: {missing}"


@skip_if_no_live
def test_espn_wbb_event_officials_raw_returns_dict():
    raw = espn_wbb_event_officials(game_id=GAME_ID, season=SEASON, raw=True)
    assert isinstance(raw, dict)
    # ESPN's core-api officials endpoint always wraps the list in ``items``.
    assert "items" in raw
