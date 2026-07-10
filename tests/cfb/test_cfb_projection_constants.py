"""Substrate tests for the CFB recruiting/roster projection spine (T2.2 Task 0.1).

All hand-computable: constants resolution, the six metrics, the as-of-season split,
and the ridge/logistic fit helpers recover known signals.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse.cfb.cfb_projection_constants import (
    as_of_season_split,
    brier_score,
    fit_logistic,
    fit_ridge,
    get_constants,
    mae,
    predict_logistic,
    predict_ridge,
    r2_score,
    rmse,
    roc_auc,
    spearman_corr,
)


def test_constants_resolve_and_unknown_raises() -> None:
    assert get_constants("fbs").blue_chip_star_min == 4
    assert get_constants("fcs").division == "fcs"
    assert get_constants("FBS").division == "fbs"  # case-insensitive
    with pytest.raises(ValueError):
        get_constants("nba")


def test_spearman_monotonic_is_one() -> None:
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([10.0, 5.0, 3.0, 1.0])
    assert abs(spearman_corr(a, b) + 1.0) < 1e-9  # perfectly anti-monotonic


def test_mae_rmse_manual() -> None:
    a = np.array([1.0, 2.0])
    b = np.array([1.5, 2.5])
    assert abs(mae(a, b) - 0.5) < 1e-9
    assert abs(rmse(a, b) - 0.5) < 1e-9


def test_r2_perfect_and_mean_baseline() -> None:
    y = np.array([1.0, 2.0, 3.0, 4.0])
    assert abs(r2_score(y, y) - 1.0) < 1e-9  # perfect fit
    assert abs(r2_score(y, np.full_like(y, y.mean()))) < 1e-9  # mean predictor -> 0


def test_roc_auc_perfect_separation() -> None:
    y = np.array([0, 0, 1, 1])
    s = np.array([0.1, 0.2, 0.8, 0.9])
    assert abs(roc_auc(y, s) - 1.0) < 1e-9


def test_brier_manual() -> None:
    y = np.array([1.0, 0.0])
    p = np.array([0.9, 0.1])
    assert abs(brier_score(y, p) - 0.01) < 1e-9


def test_as_of_split_excludes_cutoff_and_later() -> None:
    df = pl.DataFrame({"season": [2020, 2021, 2022], "v": [1, 2, 3]})
    out = as_of_season_split(df, 2022)
    assert out["season"].to_list() == [2020, 2021]


def test_fit_ridge_recovers_linear() -> None:
    rng = np.random.default_rng(0)
    X = rng.normal(size=(200, 2))
    y = 3.0 + 2.0 * X[:, 0] - 1.0 * X[:, 1]
    b0, coef = fit_ridge(X, y, alpha=1e-6)
    assert abs(b0 - 3.0) < 0.05 and abs(coef[0] - 2.0) < 0.05 and abs(coef[1] + 1.0) < 0.05


def test_ridge_intercept_is_unpenalized() -> None:
    """A strong penalty shrinks slopes toward 0 but leaves the intercept ~ mean(y)."""
    rng = np.random.default_rng(2)
    X = rng.normal(size=(300, 2))
    y = 5.0 + 4.0 * X[:, 0]
    b0, coef = fit_ridge(X, y, alpha=1e6)
    assert abs(b0 - 5.0) < 0.2  # intercept survives
    assert abs(coef[0]) < 1.0  # slope crushed


def test_fit_logistic_separates() -> None:
    rng = np.random.default_rng(1)
    X = rng.normal(size=(400, 1))
    y = (X[:, 0] + rng.normal(scale=0.3, size=400) > 0).astype(float)
    b0, coef = fit_logistic(X, y, l2=1e-3)
    p = predict_logistic(b0, coef, X)
    assert roc_auc(y, p) > 0.9


def test_predict_ridge_matches_manual() -> None:
    coef = np.array([2.0, -1.0])
    X = np.array([[1.0, 1.0], [0.0, 2.0]])
    assert np.allclose(predict_ridge(3.0, coef, X), [3.0 + 2.0 - 1.0, 3.0 - 2.0])
