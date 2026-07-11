"""Tests for Command+/Location+ (model ②) — bundled xgboost location-value model."""

from __future__ import annotations

import polars as pl

from sportsdataverse.mlb.mlb_command_plus import COMMAND_FEATURES, mlb_command_plus
from sportsdataverse.mlb.mlb_pitch_features import pitch_features


def test_command_features_has_no_raw_physics():
    for banned in ("release_speed", "release_spin_rate", "pfx_x", "pfx_z", "velo_z", "spin_z"):
        assert banned not in COMMAND_FEATURES
    assert "plate_z_norm" in COMMAND_FEATURES


def test_mlb_command_plus_on_real_fixture_schema():
    fixture = pl.read_parquet("tests/fixtures/mlb_pitching/pitches_2024-06-15.parquet")
    feats = pitch_features(fixture)
    out = mlb_command_plus(feats, level="pitch")
    assert set(["pitcher", "pitch_type", "location_rv_hat", "command_plus"]).issubset(out.columns)
    assert out.height > 0


def test_mlb_command_plus_pitcher_level():
    fixture = pl.read_parquet("tests/fixtures/mlb_pitching/pitches_2024-06-15.parquet").head(500)
    feats = pitch_features(fixture)
    out = mlb_command_plus(feats, level="pitcher")
    assert set(["pitcher", "location_rv_hat", "command_plus"]).issubset(out.columns)
    assert out["pitcher"].n_unique() == out.height


def test_mlb_command_plus_return_as_pandas():
    fixture = pl.read_parquet("tests/fixtures/mlb_pitching/pitches_2024-06-15.parquet").head(200)
    feats = pitch_features(fixture)
    out = mlb_command_plus(feats, return_as_pandas=True)
    assert type(out).__name__ == "DataFrame"


def test_mlb_command_plus_empty_input():
    empty = mlb_command_plus(pl.DataFrame())
    assert empty.height == 0
    assert "command_plus" in empty.columns


def test_mlb_command_plus_empty_pitcher_level():
    empty = mlb_command_plus(pl.DataFrame(), level="pitcher")
    assert empty.height == 0
    assert "command_plus" in empty.columns
    assert "pitch_type" not in empty.columns
