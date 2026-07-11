"""Tests for the advanced-stats canonical constants + metric helpers."""

import numpy as np
import polars as pl

from sportsdataverse.cfb.cfb_advanced_constants import (
    EXPLOSIVE_EPA,
    GARBAGE_TIME_MARGIN,
    SUCCESS_COEF,
    AdjustConfig,
    mae,
    rank_desc,
    spearman_corr,
)


def test_connelly_constants():
    assert GARBAGE_TIME_MARGIN == {1: 43, 2: 37, 3: 27, 4: 21}
    assert SUCCESS_COEF[1] == 0.5 and SUCCESS_COEF[2] == 0.7
    assert SUCCESS_COEF[3] == 1.0 and SUCCESS_COEF[4] == 1.0
    assert EXPLOSIVE_EPA["pass"] == 2.4 and EXPLOSIVE_EPA["rush"] == 1.8


def test_adjust_config_defaults():
    c = AdjustConfig()
    assert c.shrink == 0.0 and c.max_iter == 50 and c.tol == 1e-5


def test_spearman_monotonic_is_one():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([10.0, 20.0, 30.0, 40.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_mae_manual():
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_rank_desc_dense():
    df = pl.DataFrame({"x": [3.0, 1.0, 3.0, 2.0]}).with_columns(r=rank_desc(pl.col("x")))
    assert df["r"].to_list() == [1, 3, 1, 2]
    assert df.schema["r"] == pl.Int64
