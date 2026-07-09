"""Tests for the NBA/WNBA/G-League prediction-stack metrics + constants (Phase 0)."""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_prediction_constants import (
    LEAGUE_CONSTANTS,
    as_of_ratings_split,
    brier_score,
    calibration_table,
    get_constants,
    log_loss_score,
    mae,
    spearman_corr,
)


def test_brier_perfect_is_zero() -> None:
    y = np.array([1, 0, 1, 0])
    p = np.array([1.0, 0.0, 1.0, 0.0])
    assert brier_score(y, p) == 0.0


def test_brier_matches_manual() -> None:
    y = np.array([1, 0])
    p = np.array([0.75, 0.25])
    assert abs(brier_score(y, p) - 0.0625) < 1e-9


def test_log_loss_matches_manual() -> None:
    y = np.array([1, 0])
    p = np.array([0.9, 0.1])
    expected = -float(np.mean([np.log(0.9), np.log(0.9)]))
    assert abs(log_loss_score(y, p) - expected) < 1e-9


def test_spearman_monotonic_is_one() -> None:
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([10.0, 20.0, 30.0, 40.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_calibration_table_shape() -> None:
    y = np.random.default_rng(0).integers(0, 2, 200)
    p = np.random.default_rng(1).random(200)
    tbl = calibration_table(y, p, n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"]
    assert tbl.height <= 10


def test_mae_manual() -> None:
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_league_constants_all_resolve() -> None:
    for league_id in ("00", "10", "20"):
        c = get_constants(league_id)
        assert c.hfa > 0
        assert c.margin_sd > 0


def test_wnba_game_minutes_is_40() -> None:
    assert get_constants("10").game_minutes == 40
    assert get_constants("00").game_minutes == 48
    assert get_constants("20").game_minutes == 48


def test_get_constants_unknown_league_raises() -> None:
    with pytest.raises(ValueError, match="unknown league_id"):
        get_constants("99")


def test_league_constants_dict_keys() -> None:
    assert set(LEAGUE_CONSTANTS) == {"00", "10", "20"}


def test_as_of_ratings_split_excludes_same_day_and_later() -> None:
    df = pl.DataFrame(
        {
            "game_id": ["a", "b", "c"],
            "date": [dt.date(2024, 1, 1), dt.date(2024, 1, 5), dt.date(2024, 1, 10)],
        }
    )
    out = as_of_ratings_split(df, dt.date(2024, 1, 5))
    assert out["game_id"].to_list() == ["a"]
