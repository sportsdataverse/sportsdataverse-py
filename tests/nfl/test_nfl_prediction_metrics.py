"""Unit tests for the NFL ratings/market spine metric helpers (Tasks 0.2/0.3)."""

import datetime as dt

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nfl.nfl_prediction_constants import (
    PredictConfig,
    PropConfig,
    RatingsConfig,
    as_of_ratings_split,
    brier_score,
    calibration_table,
    get_constants,
    get_prop_constants,
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


def test_get_constants_and_unknown():
    assert isinstance(get_constants("modern"), PredictConfig)
    assert isinstance(get_prop_constants("modern"), PropConfig)
    with pytest.raises(ValueError):
        get_constants("nope")
    with pytest.raises(ValueError):
        get_prop_constants("nope")


def test_ratings_config_defaults():
    cfg = RatingsConfig()
    assert cfg.ridge_lambda > 0
    assert 0.0 <= cfg.min_competitive_wp < cfg.max_competitive_wp <= 1.0


def test_as_of_split_excludes_same_day_and_later():
    r = pl.DataFrame(
        {
            "game_id": ["a", "b", "c"],
            "gameday": [dt.date(2023, 9, 7), dt.date(2023, 9, 14), dt.date(2023, 9, 21)],
        }
    )
    out = as_of_ratings_split(r, dt.date(2023, 9, 14))
    assert out["game_id"].to_list() == ["a"]
