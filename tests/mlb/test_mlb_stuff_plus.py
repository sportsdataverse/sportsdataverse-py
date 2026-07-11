"""Tests for Stuff+ (model ①) — bundled xgboost run-value model."""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.mlb.mlb_pitch_features import pitch_features
from sportsdataverse.mlb.mlb_pitching_constants import spearman_corr
from sportsdataverse.mlb.mlb_stuff_plus import STUFF_FEATURES, _to_plus, mlb_stuff_plus


def test_to_plus_centers_at_100():
    rv = np.array([0.0, -0.1, 0.1])
    out = _to_plus(rv, mean_rv=0.0, sd_rv=0.1, scale=10.0)
    assert abs(out[0] - 100.0) < 1e-9  # average pitch = 100
    assert abs(out[1] - 110.0) < 1e-9  # 1 SD better (more negative RV) = 110
    assert abs(out[2] - 90.0) < 1e-9  # 1 SD worse = 90


def test_to_plus_zero_sd_does_not_divide_by_zero():
    rv = np.array([0.0, 1.0])
    out = _to_plus(rv, mean_rv=0.0, sd_rv=0.0, scale=10.0)
    assert np.all(out == 100.0)


def test_stuff_features_has_no_location_or_count():
    for banned in ("plate_x", "plate_z", "balls", "strikes", "in_zone"):
        assert banned not in STUFF_FEATURES
    assert "velo_z" in STUFF_FEATURES


def test_mlb_stuff_plus_on_real_fixture_schema_and_velo_trend():
    fixture = pl.read_parquet("tests/fixtures/mlb_pitching/pitches_2024-06-15.parquet")
    feats = pitch_features(fixture)
    out = mlb_stuff_plus(feats, level="pitch")
    assert set(["pitcher", "pitch_type", "stuff_rv_hat", "stuff_plus"]).issubset(out.columns)
    assert out.height > 0
    ff = feats.filter(pl.col("pitch_type") == "FF").join(
        out.select("pitcher", "pitch_type", "stuff_plus"), on=["pitcher", "pitch_type"], how="inner"
    )
    # higher raw velocity should trend to a higher stuff_plus among fastballs
    corr = spearman_corr(ff["velo_z"].to_numpy(), ff["stuff_plus"].to_numpy())
    assert corr > 0


def test_mlb_stuff_plus_return_as_pandas():
    fixture = pl.read_parquet("tests/fixtures/mlb_pitching/pitches_2024-06-15.parquet").head(200)
    feats = pitch_features(fixture)
    out = mlb_stuff_plus(feats, return_as_pandas=True)
    assert type(out).__name__ == "DataFrame"


def test_mlb_stuff_plus_empty_input():
    empty = mlb_stuff_plus(pl.DataFrame())
    assert empty.height == 0
    assert "stuff_plus" in empty.columns
