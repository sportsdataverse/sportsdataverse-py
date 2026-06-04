"""Smoke tests for sportsdataverse.wbb.wbb_player_stats (core-v2 season line).

``espn_wbb_player_stats`` returns ONE wide row combining athlete identity,
the season stat line (``{category}_{stat}`` columns), and team identity --
mirroring wehoop's ``espn_wbb_player_stats``. The richer web-v3 payload
lives in ``espn_wbb_player_stats_v3`` instead.
"""

from __future__ import annotations

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.wbb import espn_wbb_player_stats
from tests.conftest import skip_if_no_live

# Kylie Feuerbach (Iowa), a stable WBB athlete with a 2025 season line.
ATHLETE_ID = 4433985
SEASON = 2025

# Identity / echo columns that must always be present and self-describing.
IDENTITY_COLS: set[str] = {
    "season",
    "season_type",
    "total",
    "athlete_id",
    "full_name",
    "display_name",
}


@skip_if_no_live
def test_espn_wbb_player_stats_returns_single_wide_row():
    df = espn_wbb_player_stats(athlete_id=ATHLETE_ID, season=SEASON)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 1, "season line should be a single wide row"
    assert df.width > 20, "expected a wide frame (athlete + stats + team)"


@skip_if_no_live
def test_espn_wbb_player_stats_has_identity_and_team_columns():
    df = espn_wbb_player_stats(athlete_id=ATHLETE_ID, season=SEASON)
    missing = IDENTITY_COLS - set(df.columns)
    assert not missing, f"missing identity columns: {missing}"
    # At least the core team-identity columns are present.
    assert {"team_id", "team_display_name"}.issubset(set(df.columns))
    # Return echoes its inputs.
    row = df.to_dicts()[0]
    assert row["season"] == SEASON
    assert row["athlete_id"] == ATHLETE_ID
    assert row["season_type"] == "regular"


@skip_if_no_live
def test_espn_wbb_player_stats_has_wide_stat_columns():
    df = espn_wbb_player_stats(athlete_id=ATHLETE_ID, season=SEASON)
    # Stat columns are namespaced ``{category}_{stat}``; at least one of the
    # canonical core-v2 categories should surface.
    stat_cols = [c for c in df.columns if c.startswith(("offensive_", "defensive_", "general_"))]
    assert stat_cols, "expected at least one {category}_{stat} column"


@skip_if_no_live
def test_espn_wbb_player_stats_return_as_pandas():
    df = espn_wbb_player_stats(athlete_id=ATHLETE_ID, season=SEASON, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


@skip_if_no_live
def test_espn_wbb_player_stats_raw_returns_core_v2_statistics():
    raw = espn_wbb_player_stats(athlete_id=ATHLETE_ID, season=SEASON, raw=True)
    assert isinstance(raw, dict)
    # core-v2 statistics node carries the season stat line under ``splits``.
    assert "splits" in raw


def test_espn_wbb_player_stats_rejects_bad_season_type():
    with pytest.raises(ValueError, match="season_type"):
        espn_wbb_player_stats(athlete_id=ATHLETE_ID, season=SEASON, season_type="foo")
