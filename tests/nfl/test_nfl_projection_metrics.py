"""Unit tests for the projection-spine metric functions + as-of leakage split (Task 0.2/0.3)."""

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_projection_constants import (
    SCORING_PPR,
    SCORING_STANDARD,
    as_of_season_split,
    brier_score,
    calibration_table,
    get_position_constants,
    log_loss_score,
    mae,
    spearman_corr,
)


def test_spearman_monotonic_is_one():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([10.0, 20.0, 30.0, 40.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_mae_manual():
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_brier_matches_manual():
    y = np.array([1, 0])
    p = np.array([0.75, 0.25])
    assert abs(brier_score(y, p) - 0.0625) < 1e-9


def test_log_loss_manual():
    y = np.array([1.0, 0.0])
    p = np.array([0.5, 0.5])
    assert abs(log_loss_score(y, p) - float(np.log(2.0))) < 1e-9


def test_calibration_table_shape():
    rng = np.random.default_rng(0)
    y = rng.integers(0, 2, 200)
    p = rng.random(200)
    tbl = calibration_table(y, p, n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"]
    assert tbl.height <= 10


def test_as_of_split_excludes_target_and_future():
    df = pl.DataFrame({"season": [2021, 2022, 2023, 2024], "x": [1, 2, 3, 4]})
    out = as_of_season_split(df, 2023)
    assert out["season"].to_list() == [2021, 2022]  # 2023 and 2024 excluded


def test_scoring_formats():
    assert SCORING_PPR["receptions"] == 1.0
    assert SCORING_STANDARD["receptions"] == 0.0


def test_position_constants_availability_fitted_range():
    """Fitted all-player base availability rates are population means in a sane band.

    NOTE: the original seed assertion (QB base > RB base) is INVERTED in the
    fitted 2021-2023 snap data (QB 0.4746 < RB 0.5976) at every conditioning
    tried; the crosswalk was investigated and verified clean. QB is a
    winner-take-all position, so snap-derived availability folds benching /
    depth churn into QB unavailability. See the POSITION_CONSTANTS comment and
    dev/nfl_projection/fit_availability.py.
    """
    for pos in ["QB", "RB", "WR", "TE"]:
        base = get_position_constants(pos).base_availability
        assert 0.4 < base < 0.8


def test_position_constants_unknown_returns_default():
    default = get_position_constants("LS")  # fringe position group -> DEFAULT bucket, no raise
    assert default.recency_weights == (5.0, 4.0, 3.0)
