from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_draft_constants import (
    as_of_class_split,
    auc,
    calibration_table,
    logistic_fit_irls,
    mae,
    ridge_fit,
    spearman_corr,
)


def test_spearman_monotonic_is_one() -> None:
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([9.0, 20.0, 30.0, 44.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_auc_perfect_separation() -> None:
    y = np.array([0, 0, 1, 1])
    p = np.array([0.1, 0.2, 0.8, 0.9])
    assert abs(auc(y, p) - 1.0) < 1e-9


def test_mae_manual() -> None:
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_ridge_recovers_linear_signal() -> None:
    rng = np.random.default_rng(0)
    X = rng.normal(size=(200, 3))
    beta_true = np.array([2.0, -1.0, 0.5])
    y = X @ beta_true + rng.normal(scale=0.01, size=200)
    beta = ridge_fit(X, y, lam=1e-6)
    assert np.allclose(beta[1:], beta_true, atol=0.05)


def test_logistic_separates() -> None:
    rng = np.random.default_rng(1)
    X = rng.normal(size=(300, 2))
    z = X @ np.array([3.0, -3.0])
    y = (1 / (1 + np.exp(-z)) > 0.5).astype(int)
    beta = logistic_fit_irls(X, y)
    p = 1 / (1 + np.exp(-(beta[0] + X @ beta[1:])))
    assert auc(y, p) > 0.95


def test_calibration_table_shape() -> None:
    y = np.random.default_rng(0).integers(0, 2, 200)
    p = np.random.default_rng(1).random(200)
    tbl = calibration_table(y, p, n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"]
    assert tbl.height <= 10


def test_as_of_class_split_excludes_future() -> None:
    df = pl.DataFrame({"draft_year": [2015, 2016, 2017, 2018], "v": [1, 2, 3, 4]})
    train, holdout = as_of_class_split(df, cutoff_year=2016)
    assert set(train["draft_year"].to_list()) == {2015, 2016}
    assert set(holdout["draft_year"].to_list()) == {2017, 2018}


def test_get_constants_unknown_raises() -> None:
    from sportsdataverse.nba.nba_draft_constants import get_constants

    for lg in ("nba", "wnba", "gleague"):
        assert get_constants(lg).box_value_coef
    with pytest.raises(ValueError):
        get_constants("euroleague")


def test_career_value_monotone_in_scoring() -> None:
    from sportsdataverse.nba.nba_draft_constants import career_value_from_seasons

    base = {
        "player_id": ["P"],
        "pts100": [20.0],
        "reb100": [8.0],
        "ast100": [5.0],
        "stl100": [1.5],
        "blk100": [1.0],
        "tov100": [2.0],
        "ts_pct": [0.58],
        "usg": [24.0],
        "minutes": [2000.0],
    }
    lo = pl.DataFrame(base)
    hi = lo.with_columns(pl.lit(30.0).alias("pts100"))
    v_lo = career_value_from_seasons(lo)["career_value"][0]
    v_hi = career_value_from_seasons(hi)["career_value"][0]
    assert v_hi > v_lo


def test_build_combine_features_join_and_derived_cols() -> None:
    from sportsdataverse.nba.nba_draft_constants import build_combine_features

    anthro = pl.DataFrame(
        {
            "player_id": ["1", "2"],
            "draft_year": [2019, 2019],
            "height_wo_shoes": [78.0, 74.0],
            "weight": [210.0, 180.0],
            "wingspan": [82.0, 76.0],
            "standing_reach": [102.0, 96.0],
            "body_fat_pct": [7.0, 6.0],
            "hand_length": [9.0, 8.0],
            "hand_width": [10.0, 9.0],
        }
    )
    drills = pl.DataFrame(
        {
            "player_id": ["1"],
            "lane_agility": [11.0],
            "three_quarter_sprint": [3.2],
            "standing_vertical": [30.0],
            "max_vertical": [36.0],
        }
    )
    empty = pl.DataFrame({"player_id": []}, schema={"player_id": pl.Utf8})

    out = build_combine_features(anthro, drills, empty, empty)
    assert out.height == 2
    assert set(["player_id", "draft_year", "bmi", "wingspan_diff"]).issubset(out.columns)

    row1 = out.filter(pl.col("player_id") == "1")
    assert row1["wingspan_diff"][0] == 4.0
    assert row1["lane_agility"][0] == 11.0

    # missing drill measurement for player 2 is imputed, not null
    row2 = out.filter(pl.col("player_id") == "2")
    assert row2["lane_agility"][0] is not None
    assert row2["lane_agility"][0] == 0.0


def test_build_combine_features_empty_input_has_full_schema() -> None:
    from sportsdataverse.nba.nba_draft_constants import COMBINE_FEATURES, build_combine_features

    empty = pl.DataFrame({"player_id": []}, schema={"player_id": pl.Utf8})
    out = build_combine_features(empty, empty, empty, empty)
    assert out.height == 0
    for col in COMBINE_FEATURES:
        assert col in out.columns
