"""Unit tests for the MLB hitting-spine shared constants/metrics module (T6.2, Task 0.2)."""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.mlb.mlb_hitting_constants import (
    as_of_seasons_split,
    brier_score,
    calibration_table,
    mae,
    spearman_corr,
    spray_angle,
)


def test_spearman_monotonic_is_one() -> None:
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([10.0, 20.0, 30.0, 40.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_mae_manual() -> None:
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_brier_matches_manual() -> None:
    y = np.array([1, 0])
    p = np.array([0.75, 0.25])
    assert abs(brier_score(y, p) - 0.0625) < 1e-9


def test_calibration_table_shape() -> None:
    rng = np.random.default_rng(0)
    tbl = calibration_table(rng.integers(0, 2, 200), rng.random(200), n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"]
    assert tbl.height <= 10


def test_spray_angle_center_is_zero() -> None:
    # a ball hit straight to center (hc_x at plate x, hc_y up the middle) -> ~0 deg
    df = pl.DataFrame({"hc_x": [125.42], "hc_y": [98.27], "stand": ["R"]})
    out = df.with_columns(spray_angle(pl.col("hc_x"), pl.col("hc_y"), pl.col("stand")).alias("sa"))
    assert abs(out["sa"][0]) < 1e-6


def test_as_of_split_excludes_target_and_later() -> None:
    ps = pl.DataFrame({"batter": [1, 1, 1], "season": [2021, 2022, 2023], "xwoba": [0.3, 0.31, 0.32]})
    kept = as_of_seasons_split(ps, target_season=2023)
    assert kept["season"].to_list() == [2021, 2022]
