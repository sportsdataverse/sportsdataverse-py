"""Offline tests for special-teams (PP/PK) per-skater value."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.nhl.nhl_player_impact_constants import get_constants
from sportsdataverse.nhl.nhl_special_teams import nhl_special_teams_value

FIX = Path(__file__).parent.parent / "fixtures" / "nhl_player_impact"
MODELS = FIX / "xg_models"

_CFG = get_constants("nhl")
_PP_RATE = _CFG.league_xg_rate_pp  # 6.2 per 60
_PK_RATE = _CFG.league_xg_rate_pk  # 6.2 per 60


def _synthetic_stints() -> pl.DataFrame:
    duration = 600  # 10 minutes
    # PP stint 1: home team (on the 5v4 power play) with an above-baseline unit -- rate
    # exactly 2x league_xg_rate_pp.
    above_xgf = (2 * _PP_RATE) * duration / 3600.0
    # PP stint 2: home team power play, an exactly-replacement-rate unit.
    replacement_xgf = _PP_RATE * duration / 3600.0
    # PK stint: home team on the 4v5 penalty kill; the away (PP) team's xgf is *below*
    # league_xg_rate_pk -- i.e. the home PK unit suppresses below baseline (positive value).
    below_xga = (0.5 * _PK_RATE) * duration / 3600.0

    return pl.DataFrame(
        {
            "game_id": [1, 1, 1],
            "period": [1, 1, 1],
            "start_s": [0, 600, 1200],
            "end_s": [600, 1200, 1800],
            "duration": [duration, duration, duration],
            "home_ids": [[101, 102, 103, 104, 105], [201, 202, 203, 204, 205], [301, 302, 303, 304]],
            "away_ids": [[50, 51, 52, 53], [50, 51, 52, 53], [60, 61, 62, 63, 64]],
            "home_goalie": [None, None, None],
            "away_goalie": [None, None, None],
            "strength_state": ["5v4", "5v4", "4v5"],
            "xgf_home": [above_xgf, replacement_xgf, 0.0],
            "xgf_away": [0.0, 0.0, below_xga],
        }
    )


def test_above_baseline_pp_unit_gets_positive_pp_value():
    out = nhl_special_teams_value(pl.DataFrame(), pl.DataFrame(), _stints=_synthetic_stints())
    row = out.filter(pl.col("player_id") == 101)
    assert row.height == 1
    assert row["pp_value"][0] > 0
    assert abs(row["pp_value"][0] - (_PP_RATE * 10 / 60.0)) < 1e-6


def test_replacement_rate_pp_unit_gets_near_zero_value():
    out = nhl_special_teams_value(pl.DataFrame(), pl.DataFrame(), _stints=_synthetic_stints())
    row = out.filter(pl.col("player_id") == 201)
    assert row.height == 1
    assert abs(row["pp_value"][0]) < 1e-6


def test_suppressing_pk_unit_gets_positive_pk_value():
    out = nhl_special_teams_value(pl.DataFrame(), pl.DataFrame(), _stints=_synthetic_stints())
    row = out.filter(pl.col("player_id") == 301)
    assert row.height == 1
    assert row["pk_value"][0] > 0
    assert abs(row["pk_value"][0] - (0.5 * _PK_RATE * 10 / 60.0)) < 1e-6


def test_empty_input_returns_documented_schema():
    out = nhl_special_teams_value(pl.DataFrame(), pl.DataFrame())
    assert out.height == 0
    assert set(out.columns) == {"player_id", "pp_toi_minutes", "pk_toi_minutes", "pp_value", "pk_value"}
