import datetime as dt

import numpy as np
import polars as pl

from sportsdataverse.mlb.mlb_run_values import (
    as_of_split,
    count_strike_run_value,
    event_run_value,
    mae,
    pearson_corr,
    spearman_corr,
)


def test_pearson_and_spearman_perfect():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([2.0, 4.0, 6.0, 8.0])
    assert abs(pearson_corr(a, b) - 1.0) < 1e-9
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_mae_manual():
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_count_strike_run_value_sign():
    # a called strike costs the batting team runs (delta_run_exp negative) -> positive defensive value
    pitches = pl.DataFrame(
        {
            "balls": [0, 0, 0, 0],
            "strikes": [0, 0, 0, 0],
            "description": ["called_strike", "called_strike", "ball", "ball"],
            "delta_run_exp": [-0.04, -0.06, 0.03, 0.05],
        }
    )
    out = count_strike_run_value(pitches)
    assert out.schema["balls"] == pl.Int64
    assert out.schema["strikes"] == pl.Int64
    assert out.schema["strike_run_value"] == pl.Float64
    row = out.filter((pl.col("balls") == 0) & (pl.col("strikes") == 0)).row(0, named=True)
    # -(mean_strike - mean_ball) = -(-0.05 - 0.04) = 0.09
    assert abs(row["strike_run_value"] - 0.09) < 1e-9


def test_count_strike_run_value_empty():
    out = count_strike_run_value(pl.DataFrame(schema={"balls": pl.Int64}))
    assert out.height == 0
    assert set(out.columns) == {"balls", "strikes", "strike_run_value"}


def test_event_run_value_mean():
    df = pl.DataFrame(
        {
            "events": ["stolen_base_2b", "caught_stealing_2b", "single"],
            "delta_run_exp": [0.2, -0.45, 0.47],
        }
    )
    assert abs(event_run_value(df, ["stolen_base_2b"]) - 0.2) < 1e-9


def test_event_run_value_no_match_returns_zero():
    df = pl.DataFrame({"events": ["single"], "delta_run_exp": [0.47]})
    assert event_run_value(df, ["stolen_base_2b"]) == 0.0


def test_as_of_split_excludes_same_day_and_later():
    df = pl.DataFrame({"game_date": [dt.date(2024, 6, 1), dt.date(2024, 6, 15), dt.date(2024, 6, 20)]})
    out = as_of_split(df, dt.date(2024, 6, 15))
    assert out.height == 1
    assert out.row(0, named=True)["game_date"] == dt.date(2024, 6, 1)
