"""Unit tests for sportsdataverse.wexp.scoring (ECE, buckets, CLV, result rows)."""

import numpy as np
import polars as pl
import pytest

from sportsdataverse.wexp.scoring import (
    RESULT_SCHEMA,
    append_results,
    closing_line_value,
    ece,
    favorite_bucket_table,
    result_rows,
    winner_accuracy,
)


def test_ece_perfect_and_miscalibrated():
    rng = np.random.default_rng(7)
    p = rng.uniform(0.05, 0.95, 20_000)
    y = (rng.uniform(size=p.size) < p).astype(float)
    assert ece(y, p) < 0.02  # calibrated by construction
    assert ece(y, np.clip(p * 0.5, 0.01, 0.99)) > 0.10  # badly miscalibrated


def test_winner_accuracy():
    y = np.array([1, 0, 1, 1])
    p = np.array([0.9, 0.4, 0.2, 0.6])  # right, right, wrong, right
    assert winner_accuracy(y, p) == pytest.approx(0.75)


def test_favorite_bucket_table_folds_to_favorite():
    # home dogs at p=0.3 winning 30% of the time = favorite (away) wins 70%
    y = np.array([1, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    p = np.full(10, 0.3)
    tbl = favorite_bucket_table(y, p)
    assert tbl.height == 1
    row = tbl.row(0, named=True)
    assert row["n"] == 10
    assert row["mean_pred"] == pytest.approx(0.7)
    assert row["mean_actual"] == pytest.approx(0.9)  # away won 9/10
    assert row["abs_gap"] == pytest.approx(0.2)
    assert set(tbl.columns) == {"bucket", "mean_pred", "mean_actual", "n", "abs_gap"}


def test_closing_line_value_sign():
    # model likes home more than the bet-time market; close moved toward model -> +CLV
    clv = closing_line_value(
        p_model=np.array([0.60, 0.60]),
        p_bet=np.array([0.50, 0.50]),
        p_close=np.array([0.55, 0.45]),
    )
    assert clv == pytest.approx((0.05 + (-0.05)) / 2)
    # model likes away (p_model < p_bet): pick is away, CLV = -(close - bet)
    clv2 = closing_line_value(
        p_model=np.array([0.40]),
        p_bet=np.array([0.50]),
        p_close=np.array([0.45]),
    )
    assert clv2 == pytest.approx(0.05)


def test_result_rows_and_append(tmp_path):
    rows = result_rows(
        league="nfl",
        model_id="elo_baseline",
        variant_hash="abc123",
        vintage_policy="V2",
        season=2023,
        week_slice="all",
        era="post2017",
        metrics={"brier": 0.21, "log_loss": 0.62},
        n=272,
    )
    assert rows.height == 2
    assert rows.schema == pl.Schema(RESULT_SCHEMA)
    path = tmp_path / "leaderboard.parquet"
    append_results(rows, path)
    append_results(rows.with_columns(pl.lit(2024, dtype=pl.Int32).alias("season")), path)
    out = pl.read_parquet(path)
    assert out.height == 4
    assert set(out["season"].to_list()) == {2023, 2024}
    # appending never rewrites history
    assert out.filter(pl.col("season") == 2023).height == 2
