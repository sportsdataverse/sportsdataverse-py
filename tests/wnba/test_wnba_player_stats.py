"""Smoke tests for sportsdataverse.wnba.wnba_player_stats."""

from __future__ import annotations

import polars as pl

from sportsdataverse.wnba import espn_wnba_player_stats
from tests.conftest import skip_if_no_live

EXPECTED_CATEGORIES: set[str] = {"Averages", "Totals", "Misc"}


@skip_if_no_live
def test_espn_wnba_player_stats_returns_dict_with_canonical_categories():
    result = espn_wnba_player_stats(athlete_id=3149391, season=2024)
    assert isinstance(result, dict)
    assert EXPECTED_CATEGORIES.issubset(set(result.keys())), (
        f"missing categories: {EXPECTED_CATEGORIES - set(result.keys())}"
    )
    for cat, frame in result.items():
        if cat in EXPECTED_CATEGORIES:
            assert isinstance(frame, pl.DataFrame), f"{cat} is not a polars DataFrame"


@skip_if_no_live
def test_espn_wnba_player_stats_per_category_schema():
    result = espn_wnba_player_stats(athlete_id=3149391, season=2024)
    expected_cols = {
        "stat_name",
        "display_value",
        "value",
        "description",
        "category",
        "athlete_id",
        "season",
    }
    for cat in EXPECTED_CATEGORIES:
        frame = result[cat]
        missing = expected_cols - set(frame.columns)
        assert not missing, f"{cat} missing columns: {missing}"


@skip_if_no_live
def test_espn_wnba_player_stats_raw_returns_dict_with_categories_key():
    raw = espn_wnba_player_stats(athlete_id=3149391, season=2024, raw=True)
    assert isinstance(raw, dict)
    # ESPN may use 'categories' or 'statCategories' depending on year
    assert "categories" in raw or "statCategories" in raw
