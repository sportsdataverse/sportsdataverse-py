"""Offline tests for special-teams (PP/PK) per-skater value."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nhl.nhl_player_impact_constants import get_constants
from sportsdataverse.nhl.nhl_special_teams import nhl_special_teams_value, special_teams_split

FIX = Path(__file__).parent.parent / "fixtures" / "nhl_player_impact"
MODELS = FIX / "xg_models"

_CFG = get_constants("nhl")
_PP_RATE = _CFG.league_xg_rate_pp  # fitted, per 60
_PK_RATE = _CFG.league_xg_rate_pk  # fitted, per 60


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
    # a fifth of the 5v4 unit's 10 minutes at 2x the league rate
    assert abs(row["pp_value"][0] - (_PP_RATE * 10 / 60.0) / 5) < 1e-6


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
    # a quarter of the 4v5 unit's suppression
    assert abs(row["pk_value"][0] - (0.5 * _PK_RATE * 10 / 60.0) / 4) < 1e-6
    assert row["pk_toi_minutes"][0] == 10.0  # TOI stays the skater's own


def test_unit_value_is_split_across_its_skaters_not_copied():
    # Each PP/PK goal above the league rate is counted once inside GAR: the unit's skaters sum
    # to the unit's value (copying it to every skater counted a PP goal 5x, a PK goal 4x).
    out = nhl_special_teams_value(pl.DataFrame(), pl.DataFrame(), _stints=_synthetic_stints())
    pp_unit = out.filter(pl.col("player_id").is_in([101, 102, 103, 104, 105]))["pp_value"].sum()
    pk_unit = out.filter(pl.col("player_id").is_in([301, 302, 303, 304]))["pk_value"].sum()
    assert abs(pp_unit - _PP_RATE * 10 / 60.0) < 1e-6
    assert abs(pk_unit - 0.5 * _PK_RATE * 10 / 60.0) < 1e-6


def test_share_divides_by_the_id_list_not_the_strength_state():
    # A 5v4 stint whose PP list holds 4 ids (one missing from the shift chart) and whose PK list is
    # empty: the 4 split the unit's value exactly, and the empty side simply gets no credit.
    stints = (
        _synthetic_stints()
        .head(1)
        .with_columns(home_ids=pl.Series([[101, 102, 103, 104]]), away_ids=pl.Series([[]], dtype=pl.List(pl.Int64)))
    )
    out = nhl_special_teams_value(pl.DataFrame(), pl.DataFrame(), _stints=stints)
    assert out.height == 4
    assert out["pp_value"].to_list() == pytest.approx([_PP_RATE * 10 / 60.0 / 4] * 4)


def test_special_teams_split_picks_the_side_with_more_skaters():
    rec = {"duration": 60, "xgf_home": 0.1, "xgf_away": 0.4}
    assert special_teams_split({**rec, "strength_state": "4v5"}) == ("away_ids", "home_ids", 0.4)
    assert special_teams_split({**rec, "strength_state": "6v5"}) == ("home_ids", "away_ids", 0.1)
    assert special_teams_split({**rec, "strength_state": "5v5"}) is None
    assert special_teams_split({**rec, "strength_state": "5v4", "duration": 0}) is None
    assert special_teams_split({**rec, "strength_state": None}) is None


def test_empty_input_returns_documented_schema():
    out = nhl_special_teams_value(pl.DataFrame(), pl.DataFrame())
    assert out.height == 0
    assert set(out.columns) == {"player_id", "pp_toi_minutes", "pk_toi_minutes", "pp_value", "pk_value"}
