"""Smoke tests for sportsdataverse.wnba.wnba_team_stats."""

from __future__ import annotations

import polars as pl

from sportsdataverse.wnba import espn_wnba_team_stats
from tests.conftest import skip_if_no_live

EXPECTED_CATEGORIES: set[str] = {"Averages", "Totals", "Misc"}


@skip_if_no_live
def test_espn_wnba_team_stats_returns_polars_by_default():
    result = espn_wnba_team_stats(team_id=17, season=2024)
    assert isinstance(result, dict)
    assert EXPECTED_CATEGORIES.issubset(set(result.keys())), (
        f"missing categories: {EXPECTED_CATEGORIES - set(result.keys())}"
    )
    for cat in EXPECTED_CATEGORIES:
        assert isinstance(result[cat], pl.DataFrame), f"{cat} is not a polars DataFrame"


@skip_if_no_live
def test_espn_wnba_team_stats_pandas_round_trip():
    import pandas as pd

    result = espn_wnba_team_stats(team_id=17, season=2024, return_as_pandas=True)
    assert isinstance(result, dict)
    for cat in EXPECTED_CATEGORIES:
        assert cat in result
        assert isinstance(result[cat], pd.DataFrame), f"{cat} is not a pandas DataFrame"


@skip_if_no_live
def test_espn_wnba_team_stats_raw_returns_dict():
    raw = espn_wnba_team_stats(team_id=17, season=2024, raw=True)
    assert isinstance(raw, dict)
    assert "results" in raw or "stats" in raw or "categories" in raw
