"""Offline tests for native v3 xG scoring (fastRhockey booster port)."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.nhl.nhl_xg import add_shot_geometry, nhl_xg, prepare_xg_features

FIX = Path(__file__).parent.parent / "fixtures" / "nhl_player_impact"
MODELS = FIX / "xg_models"


def _pbp() -> pl.DataFrame:
    return pl.read_parquet(FIX / "pbp_sample.parquet")


def test_prepare_features_only_unblocked_shots():
    feat = prepare_xg_features(_pbp())
    assert set(feat["event_type"].unique()).issubset({"SHOT", "MISSED_SHOT", "GOAL"})
    assert {"rebound", "rush", "total_skaters_on", "wrist_shot"}.issubset(feat.columns)


def test_nhl_xg_scores_shots_in_unit_interval():
    out = nhl_xg(_pbp(), model_dir=MODELS)
    scored = out.filter(pl.col("xg").is_not_null())
    assert scored.height > 0
    assert scored["xg"].min() >= 0.0 and scored["xg"].max() <= 1.0


def test_nhl_xg_empty_input_returns_null_xg_no_raise():
    empty = _pbp().head(0)
    out = nhl_xg(empty, model_dir=MODELS)
    assert "xg" in out.columns and out.height == 0


def test_add_shot_geometry_slot_shot_is_high_danger():
    # Right in the slot: small x-distance to the goal line, y near center.
    df = pl.DataFrame(
        {
            "x_fixed": [80],
            "y": [0],
            "event_type": ["SHOT"],
        }
    )
    out = add_shot_geometry(df, league="nhl")
    assert out["shot_danger"][0] == "high"
    assert out["distance_to_net"][0] < 15.0


def test_add_shot_geometry_point_shot_is_low_danger():
    # A point shot: far from the net, near the blue line.
    df = pl.DataFrame(
        {
            "x_fixed": [30],
            "y": [0],
            "event_type": ["SHOT"],
        }
    )
    out = add_shot_geometry(df, league="nhl")
    assert out["shot_danger"][0] == "low"
