"""Behavioral pins for the shared validation metrics (T7.2 `_common.metrics`)."""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from sportsdataverse._common.metrics import (
    as_of_ratings_split,
    brier_score,
    calibration_table,
    log_loss_score,
    mae,
    spearman_corr,
)


def test_brier_perfect_is_zero():
    assert brier_score(np.array([1, 0, 1, 0]), np.array([1.0, 0.0, 1.0, 0.0])) == 0.0


def test_brier_matches_manual():
    # (0.75-1)^2 + (0.25-0)^2 = 0.0625 + 0.0625 -> mean 0.0625
    assert abs(brier_score(np.array([1, 0]), np.array([0.75, 0.25])) - 0.0625) < 1e-12


def test_brier_accepts_python_lists():
    assert abs(brier_score([1, 0], [0.9, 0.1]) - 0.01) < 1e-12


def test_log_loss_matches_manual():
    # -mean( ln(0.9) , ln(0.9) ) = -ln(0.9)
    got = log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
    assert abs(got - (-np.log(0.9))) < 1e-9


def test_log_loss_clips_extremes():
    # p=1.0 for y=0 would be inf without eps clipping; assert finite + large
    got = log_loss_score(np.array([0, 1]), np.array([1.0, 0.0]))
    assert np.isfinite(got) and got > 30.0


def test_spearman_monotonic_is_one():
    assert abs(spearman_corr(np.array([1.0, 2.0, 3.0, 4.0]), np.array([10.0, 20.0, 30.0, 40.0])) - 1.0) < 1e-12


def test_spearman_reversed_is_minus_one():
    assert abs(spearman_corr(np.array([1, 2, 3, 4]), np.array([4, 3, 2, 1])) + 1.0) < 1e-12


def test_spearman_uses_ranks_not_values():
    # rank correlation is invariant to monotone transform of one variable
    a = np.array([1.0, 2.0, 3.0, 4.0])
    assert abs(spearman_corr(a, a**3) - 1.0) < 1e-12


def test_mae_manual():
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-12


def test_calibration_table_schema_and_dtypes():
    y = np.array([1, 0, 1, 0, 1, 0])
    p = np.array([0.95, 0.05, 0.85, 0.15, 0.55, 0.45])
    tbl = calibration_table(y, p, n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"]
    # `n` is pl.len() -> UInt32 (schema is load-bearing for byte-for-byte migration)
    assert tbl.schema["n"] == pl.UInt32
    assert tbl.height <= 10


def test_calibration_table_bins_and_actuals():
    # two perfectly-separated buckets
    y = np.array([0, 0, 1, 1])
    p = np.array([0.05, 0.05, 0.95, 0.95])
    tbl = calibration_table(y, p, n_bins=10).sort("bin_mid")
    assert tbl["mean_actual"].to_list() == [0.0, 1.0]
    assert tbl["n"].to_list() == [2, 2]


def test_as_of_split_strict_less_than():
    df = pl.DataFrame(
        {"game_id": ["a", "b", "c"], "date": [dt.date(2024, 1, 1), dt.date(2024, 1, 5), dt.date(2024, 1, 9)]}
    )
    out = as_of_ratings_split(df, dt.date(2024, 1, 5))
    assert out["game_id"].to_list() == ["a"]  # same-day and later excluded
