"""Smoke tests for sportsdataverse.wnba.wnba_draft."""

from __future__ import annotations

import polars as pl

from sportsdataverse.wnba import espn_wnba_draft
from tests.conftest import skip_if_no_live

CORE_COLUMNS: set[str] = {
    "season",
    "round_number",
    "pick_number",
    "overall_pick",
    "team_id",
    "athlete_id",
    "athlete_display_name",
    "headshot_href",
    "link_web",
}


@skip_if_no_live
def test_espn_wnba_draft_returns_polars_by_default():
    df = espn_wnba_draft(season=2024)
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] > 0
    missing = CORE_COLUMNS - set(df.columns)
    assert not missing, f"missing columns: {missing}"
    assert df["season"].unique().to_list() == [2024]


@skip_if_no_live
def test_espn_wnba_draft_pandas_round_trip():
    import pandas as pd

    df = espn_wnba_draft(season=2024, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    missing = CORE_COLUMNS - set(df.columns)
    assert not missing, f"missing columns: {missing}"


@skip_if_no_live
def test_espn_wnba_draft_raw_returns_dict():
    raw = espn_wnba_draft(season=2024, raw=True)
    assert isinstance(raw, dict)
    # ESPN modern endpoint inlines picks at the top level; legacy nests
    # under rounds[].picks. Accept either.
    assert "picks" in raw or "rounds" in raw
