"""Tests for the game-state shared substrate: constants, metrics, as_of_split."""

import datetime as dt

import numpy as np
import polars as pl

from sportsdataverse.mlb.mlb_game_state_constants import (
    BASE_STATES,
    PYTHAGENPAT_EXPONENT,
    as_of_split,
    brier_score,
    calibration_table,
    mae,
    spearman_corr,
)


def test_exponent_and_base_states():
    assert PYTHAGENPAT_EXPONENT == 0.287
    assert BASE_STATES[0] == "___" and BASE_STATES[-1] == "123" and len(BASE_STATES) == 8


def test_brier_matches_manual():
    y = np.array([1, 0])
    p = np.array([0.75, 0.25])
    assert abs(brier_score(y, p) - 0.0625) < 1e-9


def test_spearman_monotonic_is_one():
    # Plan draft used [9, 8, 10] here, which is NOT rank-monotonic with [1, 2, 3]
    # (its ranks are [2, 1, 3], giving rho=0.5, not 1.0) -- a bug in the plan's
    # sample values, not the implementation. [7, 8, 10] is genuinely monotonic.
    assert abs(spearman_corr(np.array([1.0, 2.0, 3.0]), np.array([7.0, 8.0, 10.0])) - 1.0) < 1e-9


def test_mae_manual():
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_calibration_table_shape():
    rng = np.random.default_rng(0)
    tbl = calibration_table(rng.integers(0, 2, 200), rng.random(200), n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"] and tbl.height <= 10


def test_as_of_split_excludes_same_day_and_later():
    df = pl.DataFrame({"date": [dt.date(2024, 4, 1), dt.date(2024, 4, 2), dt.date(2024, 4, 3)], "x": [1, 2, 3]})
    out = as_of_split(df, dt.date(2024, 4, 2))
    assert out.height == 1 and out["x"].to_list() == [1]
