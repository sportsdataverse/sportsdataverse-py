"""Unit tests for the pitching-spine shared metrics + as-of-date leakage split."""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from sportsdataverse.mlb.mlb_pitching_constants import (
    as_of_split,
    calibration_table,
    get_baselines,
    mae,
    rmse,
    spearman_corr,
)


def test_get_baselines_resolves_known_season():
    b = get_baselines(2024)
    assert abs(b.league_era - 4.15) < 1e-9
    assert b.siera_coef == [6.0, -12.0, 8.0, -3.0, 2.0, 1.0]


def test_get_baselines_unknown_season_falls_back_to_nearest():
    b = get_baselines(2030)
    assert b == get_baselines(2024)
    b_early = get_baselines(1990)
    assert b_early == get_baselines(2021)


def test_spearman_monotonic_is_one():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([10.0, 20.0, 30.0, 40.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_spearman_inverse_is_negative_one():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([40.0, 30.0, 20.0, 10.0])
    assert abs(spearman_corr(a, b) - (-1.0)) < 1e-9


def test_rmse_and_mae_manual():
    a = np.array([1.0, 2.0])
    b = np.array([1.5, 2.5])
    assert abs(mae(a, b) - 0.5) < 1e-9
    assert abs(rmse(a, b) - 0.5) < 1e-9


def test_calibration_table_shape():
    rng = np.random.default_rng(0)
    y = rng.integers(0, 2, 200)
    p = rng.random(200)
    tbl = calibration_table(y, p, n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"]
    assert tbl.height <= 10


def test_as_of_split_excludes_cutoff_and_later():
    df = pl.DataFrame(
        {
            "game_date": [dt.date(2024, 6, 1), dt.date(2024, 6, 15), dt.date(2024, 6, 20)],
            "x": [1, 2, 3],
        }
    )
    out = as_of_split(df, dt.date(2024, 6, 15))
    assert out.height == 1 and out["x"].to_list() == [1]


def test_as_of_split_custom_date_col():
    df = pl.DataFrame(
        {
            "appearance_date": [dt.date(2023, 4, 1), dt.date(2023, 5, 1)],
            "x": [1, 2],
        }
    )
    out = as_of_split(df, dt.date(2023, 4, 15), date_col="appearance_date")
    assert out.height == 1 and out["x"].to_list() == [1]
