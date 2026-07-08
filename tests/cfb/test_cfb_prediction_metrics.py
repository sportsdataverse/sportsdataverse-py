"""Tests for CFB prediction-spine validation metrics."""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl
import pytest

from sportsdataverse.cfb.cfb_prediction_constants import (
    PredictConfig,
    as_of_ratings_split,
    brier_score,
    calibration_table,
    get_constants,
    log_loss_score,
    mae,
    spearman_corr,
)


def test_brier_perfect_is_zero() -> None:
    y = np.array([1, 0, 1, 0])
    p = np.array([1.0, 0.0, 1.0, 0.0])
    assert brier_score(y, p) == 0.0


def test_brier_matches_manual() -> None:
    y = np.array([1, 0])
    p = np.array([0.75, 0.25])
    assert abs(brier_score(y, p) - 0.0625) < 1e-9  # mean of (0.25^2, 0.25^2)


def test_spearman_monotonic_is_one() -> None:
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([10.0, 20.0, 30.0, 40.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_calibration_table_shape() -> None:
    y = np.random.default_rng(0).integers(0, 2, 200)
    p = np.random.default_rng(1).random(200)
    tbl = calibration_table(y, p, n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"]
    assert tbl.height <= 10


def test_mae_manual() -> None:
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_log_loss_matches_manual() -> None:
    y = np.array([1, 0])
    p = np.array([0.75, 0.25])
    expected = -np.mean([np.log(0.75), np.log(1 - 0.25)])
    assert abs(log_loss_score(y, p) - expected) < 1e-9


def test_get_constants_and_unknown() -> None:
    assert isinstance(get_constants("modern"), PredictConfig)
    with pytest.raises(ValueError):
        get_constants("nope")


def test_as_of_split_excludes_same_day_and_later() -> None:
    r = pl.DataFrame(
        {
            "game_id": ["a", "b", "c"],
            "date": [dt.date(2023, 9, 1), dt.date(2023, 9, 8), dt.date(2023, 9, 15)],
        }
    )
    out = as_of_ratings_split(r, dt.date(2023, 9, 8))
    assert out["game_id"].to_list() == ["a"]
