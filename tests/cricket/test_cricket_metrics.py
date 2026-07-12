"""Unit tests for the cricket calibration metrics (T7.3 Task 0.2)."""

from __future__ import annotations

import numpy as np

from sportsdataverse.cricket.cricket_model_constants import (
    auc_score,
    brier_score,
    calibration_table,
    log_loss_score,
    mae,
)


def test_brier_perfect_is_zero() -> None:
    y = np.array([1, 0, 1, 0])
    p = np.array([1.0, 0.0, 1.0, 0.0])
    assert brier_score(y, p) == 0.0


def test_brier_matches_manual() -> None:
    y = np.array([1, 0])
    p = np.array([0.75, 0.25])
    assert abs(brier_score(y, p) - 0.0625) < 1e-9


def test_log_loss_positive() -> None:
    y = np.array([1, 0])
    p = np.array([0.9, 0.1])
    assert log_loss_score(y, p) > 0.0


def test_auc_perfect_ranking_is_one() -> None:
    y = np.array([0, 0, 1, 1])
    p = np.array([0.1, 0.2, 0.8, 0.9])
    assert abs(auc_score(y, p) - 1.0) < 1e-9


def test_auc_reversed_ranking_is_zero() -> None:
    y = np.array([0, 0, 1, 1])
    p = np.array([0.9, 0.8, 0.2, 0.1])
    assert abs(auc_score(y, p) - 0.0) < 1e-9


def test_auc_single_class_is_half() -> None:
    y = np.array([1, 1, 1])
    p = np.array([0.2, 0.5, 0.9])
    assert auc_score(y, p) == 0.5


def test_auc_ties_are_half() -> None:
    y = np.array([0, 1])
    p = np.array([0.5, 0.5])
    assert abs(auc_score(y, p) - 0.5) < 1e-9


def test_mae_manual() -> None:
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_calibration_table_shape() -> None:
    rng = np.random.default_rng(0)
    y = rng.integers(0, 2, 200)
    p = rng.random(200)
    tbl = calibration_table(y, p, n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"]
    assert tbl.height <= 10
    assert int(tbl["n"].sum()) == 200
