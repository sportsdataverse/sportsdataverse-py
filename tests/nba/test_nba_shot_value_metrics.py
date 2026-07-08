"""Tests for the shot-value validation metrics + per-league constants."""

import numpy as np
import pytest

from sportsdataverse.nba.nba_shot_value_constants import (
    ZONE_COLLAPSE,
    get_court,
    get_shrinkage_k,
    mae,
    points_calibration_error,
    pps,
    split_half_reliability,
)


def test_points_calibration_perfect_is_zero():
    e = np.array([2.0, 3.0, 1.0])
    a = np.array([2.0, 3.0, 1.0])
    assert points_calibration_error(e, a) == 0.0


def test_points_calibration_manual():
    e = np.array([100.0])
    a = np.array([101.0])
    assert abs(points_calibration_error(e, a) - 1 / 101) < 1e-9


def test_points_calibration_zero_actual_is_zero():
    assert points_calibration_error(np.array([1.0]), np.array([0.0])) == 0.0


def test_split_half_monotonic_is_one():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = 2 * a
    assert abs(split_half_reliability(a, b) - 1.0) < 1e-9


def test_split_half_singleton_is_nan():
    assert np.isnan(split_half_reliability(np.array([1.0]), np.array([2.0])))


def test_mae_manual():
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_pps_manual():
    assert abs(pps(np.array([2.0, 0.0, 3.0]), np.array([1.0, 1.0, 1.0])) - 5 / 3) < 1e-9


def test_pps_no_attempts_is_zero():
    assert pps(np.array([2.0]), np.array([0.0])) == 0.0


def test_courts_resolve_and_unknown_raises():
    assert get_court("00").corner3_loc_x_abs == 220  # NBA
    assert get_court("10").corner3_loc_x_abs != get_court("00").corner3_loc_x_abs  # WNBA differs
    assert get_court("20").rim_radius_ft == get_court("00").rim_radius_ft  # G-League == NBA court
    with pytest.raises(ValueError):
        get_court("99")


def test_shrinkage_resolves_and_unknown_raises():
    assert get_shrinkage_k("00") > 0
    assert get_shrinkage_k("10") > 0
    with pytest.raises(ValueError):
        get_shrinkage_k("99")


def test_zone_collapse_corners_merge():
    assert ZONE_COLLAPSE["Left Corner 3"] == "corner_3" == ZONE_COLLAPSE["Right Corner 3"]
    assert ZONE_COLLAPSE["Restricted Area"] == "rim"
