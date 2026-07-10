"""Tests for the NHL/PWHL prediction-spine shared metrics + league-constants scaffold."""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nhl.nhl_prediction_constants import (
    as_of_ratings_split,
    brier_score,
    calibration_table,
    get_constants,
    log_loss_score,
    mae,
    spearman_corr,
)


def test_brier_perfect_is_zero():
    assert brier_score(np.array([1, 0, 1]), np.array([1.0, 0.0, 1.0])) == 0.0


def test_brier_matches_manual():
    assert abs(brier_score(np.array([1, 0]), np.array([0.75, 0.25])) - 0.0625) < 1e-9


def test_log_loss_matches_manual():
    y = np.array([1, 0])
    p = np.array([0.8, 0.2])
    expected = -np.mean(y * np.log(p) + (1 - y) * np.log(1 - p))
    assert abs(log_loss_score(y, p) - expected) < 1e-9


def test_spearman_monotonic_is_one():
    assert abs(spearman_corr(np.array([1.0, 2.0, 3.0, 4.0]), np.array([10.0, 20.0, 30.0, 40.0])) - 1.0) < 1e-9


def test_mae_manual():
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_calibration_table_shape():
    rng = np.random.default_rng(0)
    tbl = calibration_table(rng.integers(0, 2, 200), rng.random(200), n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"]
    assert tbl.height <= 10


def test_as_of_split_excludes_same_day_and_later():
    df = pl.DataFrame({"date": [dt.date(2023, 1, 1), dt.date(2023, 1, 2), dt.date(2023, 1, 3)]})
    out = as_of_ratings_split(df, dt.date(2023, 1, 2))
    assert out.height == 1 and out["date"][0] == dt.date(2023, 1, 1)


def test_constants_resolve_and_unknown_raises():
    assert get_constants("nhl").margin_sd > 0
    assert get_constants("pwhl").shrink_k >= get_constants("nhl").shrink_k
    with pytest.raises(ValueError):
        get_constants("khl")
