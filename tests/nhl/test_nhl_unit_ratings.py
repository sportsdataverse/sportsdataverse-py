"""Offline tests for on-ice line/pair ratings (summed-member RAPM + observed on-ice xG)."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.nhl.nhl_player_impact_constants import spearman_corr
from sportsdataverse.nhl.nhl_unit_ratings import nhl_unit_ratings

FIX = Path(__file__).parent.parent / "fixtures" / "nhl_player_impact"
MODELS = FIX / "xg_models"


def _synthetic_stints() -> pl.DataFrame:
    # Two forward trios (home team): {1,2,3} dominates (high xGF, low xGA); {4,5,6}
    # is below-average (low xGF, high xGA). Each trio accumulates 25 minutes (1500s)
    # across many short stints -- comfortably above min_toi=20 -- while the 3rd trio
    # ({7,8,9}, added below) gets only 1 minute so min_toi drops it.
    rows = []
    gid = 1
    t = 0
    for _ in range(15):
        rows.append(
            {
                "game_id": gid,
                "period": 1,
                "start_s": t,
                "end_s": t + 100,
                "duration": 100,
                "home_ids": [1, 2, 3],
                "away_ids": [50, 51, 52],
                "home_goalie": None,
                "away_goalie": None,
                "strength_state": "5v5",
                "xgf_home": 0.5,
                "xgf_away": 0.05,
            }
        )
        t += 100
    for _ in range(15):
        rows.append(
            {
                "game_id": gid,
                "period": 1,
                "start_s": t,
                "end_s": t + 100,
                "duration": 100,
                "home_ids": [4, 5, 6],
                "away_ids": [50, 51, 52],
                "home_goalie": None,
                "away_goalie": None,
                "strength_state": "5v5",
                "xgf_home": 0.05,
                "xgf_away": 0.5,
            }
        )
        t += 100
    # A short-TOI trio (below min_toi threshold) that should be dropped.
    rows.append(
        {
            "game_id": gid,
            "period": 1,
            "start_s": t,
            "end_s": t + 60,
            "duration": 60,
            "home_ids": [7, 8, 9],
            "away_ids": [50, 51, 52],
            "home_goalie": None,
            "away_goalie": None,
            "strength_state": "5v5",
            "xgf_home": 0.2,
            "xgf_away": 0.0,
        }
    )
    return pl.DataFrame(rows)


def _synthetic_rapm() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "player_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 50, 51, 52],
            "xg_rapm_off": [0.6, 0.6, 0.6, -0.4, -0.4, -0.4, 0.1, 0.1, 0.1, 0.0, 0.0, 0.0],
            "xg_rapm_def": [0.3, 0.3, 0.3, -0.2, -0.2, -0.2, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "xg_rapm": [0.9, 0.9, 0.9, -0.6, -0.6, -0.6, 0.1, 0.1, 0.1, 0.0, 0.0, 0.0],
            "toi_minutes": [15.0] * 9 + [30.0, 30.0, 30.0],
        }
    )


def test_dominant_trio_has_higher_unit_value_and_short_toi_units_dropped():
    stints = _synthetic_stints()
    rapm = _synthetic_rapm()
    units = nhl_unit_ratings(
        pl.DataFrame(), pl.DataFrame(), unit_type="forward_line", min_toi=20.0, _stints=stints, _rapm=rapm
    )
    assert units.height > 0
    dominant = units.filter(pl.col("unit_ids") == "1-2-3")
    weak = units.filter(pl.col("unit_ids") == "4-5-6")
    assert dominant.height == 1 and weak.height == 1
    assert dominant["unit_value"][0] > weak["unit_value"][0]
    assert dominant["on_ice_xgf_pct"][0] > 0.5
    # The 60s-TOI trio {7,8,9} (< min_toi=20 min) must be dropped.
    assert units.filter(pl.col("unit_ids") == "7-8-9").height == 0


def test_unit_ratings_empty_input_returns_documented_schema():
    out = nhl_unit_ratings(pl.DataFrame(), pl.DataFrame())
    assert out.height == 0
    assert set(out.columns) == {
        "team",
        "unit_ids",
        "unit_players",
        "toi_minutes",
        "on_ice_xgf",
        "on_ice_xga",
        "on_ice_xgf_pct",
        "summed_rapm",
        "unit_value",
    }


def test_internal_gate_summed_rapm_tracks_observed_on_ice_xg_diff():
    stints = _synthetic_stints()
    rapm = _synthetic_rapm()
    units = nhl_unit_ratings(
        pl.DataFrame(), pl.DataFrame(), unit_type="forward_line", min_toi=0.0, _stints=stints, _rapm=rapm
    )
    corr = spearman_corr(units["summed_rapm"].to_numpy(), (units["on_ice_xgf"] - units["on_ice_xga"]).to_numpy())
    assert corr >= 0.5, f"summed_rapm should track observed on-ice xG-diff: corr={corr:.3f}"
