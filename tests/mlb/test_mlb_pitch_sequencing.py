"""Tests for pitch tunneling geometry + sequence run value (model ⑥)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.mlb.mlb_pitch_features import add_sequence_features, pitch_features
from sportsdataverse.mlb.mlb_pitch_sequencing import mlb_pitch_tunneling, mlb_sequence_run_value


def test_tunnel_geometry_hand_computed():
    # prev release (0,6) / cur release (0.3,6.4); prev plate (0,2.5) / cur plate (1.0,1.5).
    df = pl.DataFrame(
        {
            "pitcher": [1, 1],
            "game_pk": [1, 1],
            "at_bat_number": [1, 1],
            "pitch_number": [1, 2],
            "release_pos_x": [0.0, 0.3],
            "release_pos_z": [6.0, 6.4],
            "plate_x": [0.0, 1.0],
            "plate_z": [2.5, 1.5],
            "prev_release_pos_x": [None, 0.0],
            "prev_release_pos_z": [None, 6.0],
            "prev_plate_x": [None, 0.0],
            "prev_plate_z": [None, 2.5],
        }
    )
    out = mlb_pitch_tunneling(df)
    first, second = out.row(0, named=True), out.row(1, named=True)
    assert first["release_dist"] is None and first["tunnel_ratio"] is None
    assert abs(second["release_dist"] - 0.5) < 1e-4
    assert abs(second["plate_dist"] - 1.41421356) < 1e-4
    assert abs(second["tunnel_ratio"] - 2.82842712) < 1e-4


def test_tunnel_empty_input():
    out = mlb_pitch_tunneling(pl.DataFrame())
    assert out.height == 0 and "tunnel_ratio" in out.columns


def test_tunnel_real_fixture_finite_and_nonnegative():
    fixture = pl.read_parquet("tests/fixtures/mlb_pitching/pitches_2024-06-15.parquet")
    feats = add_sequence_features(pitch_features(fixture))
    out = mlb_pitch_tunneling(feats)
    nonnull = out.filter(pl.col("tunnel_ratio").is_not_null())
    assert nonnull.height > 0
    assert nonnull.filter(pl.col("tunnel_ratio") < 0).height == 0
    assert nonnull["tunnel_ratio"].is_finite().all()


def test_sequence_run_value_real_fixture():
    fixture = pl.read_parquet("tests/fixtures/mlb_pitching/pitches_2024-06-15.parquet")
    feats = add_sequence_features(pitch_features(fixture))
    out = mlb_sequence_run_value(feats)
    assert out.height > 0
    assert out.filter(pl.col("n") <= 0).height == 0


def test_sequence_run_value_empty_input():
    out = mlb_sequence_run_value(pl.DataFrame())
    assert out.height == 0 and "mean_run_value" in out.columns
