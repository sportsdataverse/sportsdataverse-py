"""Unit tests for the NFL ratings/market spine metric helpers (Task 0.2)."""

import numpy as np

from sportsdataverse.nfl.nfl_prediction_constants import (
    brier_score,
    calibration_table,
    log_loss_score,
    mae,
    spearman_corr,
)


def test_brier_perfect_is_zero():
    y = np.array([1, 0, 1, 0])
    p = np.array([1.0, 0.0, 1.0, 0.0])
    assert brier_score(y, p) == 0.0


def test_brier_matches_manual():
    y = np.array([1, 0])
    p = np.array([0.75, 0.25])
    assert abs(brier_score(y, p) - 0.0625) < 1e-9  # mean of (0.25^2, 0.25^2)


def test_log_loss_beats_worse_prediction():
    y = np.array([1, 0, 1])
    good = np.array([0.9, 0.1, 0.8])
    bad = np.array([0.6, 0.4, 0.5])
    assert log_loss_score(y, good) < log_loss_score(y, bad)


def test_spearman_monotonic_is_one():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([10.0, 20.0, 30.0, 40.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_calibration_table_shape():
    y = np.random.default_rng(0).integers(0, 2, 200)
    p = np.random.default_rng(1).random(200)
    tbl = calibration_table(y, p, n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"]
    assert tbl.height <= 10


def test_mae_manual():
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9
