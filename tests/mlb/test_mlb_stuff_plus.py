"""Tests for Stuff+ (model ①) — bundled xgboost run-value model."""

from __future__ import annotations

import numpy as np

from sportsdataverse.mlb.mlb_stuff_plus import STUFF_FEATURES, _to_plus


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
