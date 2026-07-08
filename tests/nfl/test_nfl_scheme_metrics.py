"""Unit tests for the scheme-spine metric helpers + constants tables (Tasks 0.1/0.2)."""

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_scheme_constants import (
    EB_PRIOR,
    ENVIRONMENT_FG_COEF,
    PACE_CONSTANTS,
    PLAYCALL_ARTIFACT,
    STADIUM_ALTITUDE,
    as_of_split,
    auc_score,
    brier_score,
    calibration_table,
    log_loss_score,
    mae,
    spearman_corr,
)


def test_brier_matches_manual():
    y = np.array([1, 0])
    p = np.array([0.75, 0.25])
    assert abs(brier_score(y, p) - 0.0625) < 1e-9  # mean of (0.25^2, 0.25^2)


def test_log_loss_perfect_is_small():
    y = np.array([1, 0, 1])
    p = np.array([1.0, 0.0, 1.0])
    assert log_loss_score(y, p) < 1e-6


def test_auc_perfect_ranker_is_one():
    y = np.array([0, 0, 1, 1])
    p = np.array([0.1, 0.2, 0.8, 0.9])
    assert abs(auc_score(y, p) - 1.0) < 1e-9


def test_auc_reversed_is_zero():
    y = np.array([0, 0, 1, 1])
    p = np.array([0.9, 0.8, 0.2, 0.1])
    assert abs(auc_score(y, p) - 0.0) < 1e-9


def test_spearman_monotonic_is_one():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([10.0, 20.0, 30.0, 40.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_calibration_table_shape():
    rng = np.random.default_rng(0)
    y = rng.integers(0, 2, 200)
    p = rng.random(200)
    tbl = calibration_table(y, p, n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"]
    assert tbl.height <= 10


def test_mae_manual():
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_altitude_denver_high():
    assert STADIUM_ALTITUDE["DEN"] > 5000.0


def test_constant_tables_present():
    assert set(ENVIRONMENT_FG_COEF) >= {"wind", "temp", "altitude_kft", "temp_baseline"}
    assert set(PACE_CONSTANTS) >= {"intercept", "b_pace", "b_opp_pace", "b_total"}
    assert set(EB_PRIOR) >= {"K_fg", "K_pressure"}
    assert PLAYCALL_ARTIFACT.endswith(".ubj")


def test_as_of_split_excludes_current_and_future():
    df = pl.DataFrame(
        {
            "season": [2023, 2023, 2023, 2023],
            "week": [1, 2, 3, 3],
            "play_id": [1, 2, 3, 4],
        }
    )
    out = as_of_split(df, season=2023, week=3)
    assert out["week"].max() < 3
    assert out.height == 2


def test_as_of_split_keeps_prior_seasons():
    df = pl.DataFrame({"season": [2022, 2023], "week": [18, 1], "play_id": [1, 2]})
    out = as_of_split(df, season=2023, week=1)
    assert out["season"].to_list() == [2022]
