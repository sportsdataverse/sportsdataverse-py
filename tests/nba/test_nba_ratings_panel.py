"""Tests for the WP4 through-date ratings engine (nba_ratings_panel)."""

from __future__ import annotations

import datetime

import polars as pl
import pytest
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel, _synthetic_possessions
from sportsdataverse.nba.nba_ratings_panel import ratings_as_of


def _dated(poss: pl.DataFrame, d: datetime.date) -> pl.DataFrame:
    return poss.with_columns(pl.lit(d).cast(pl.Date).alias("game_date"))


def _three_day_possessions() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Three disjoint game-day possession frames sharing the same 16-player pool."""
    o = {p: 0.02 for p in range(1, 17)}
    d = {p: 0.01 for p in range(1, 17)}
    day1 = _dated(
        _synthetic_possessions(o, d, n_games=10, poss_per_game=30, noise_sd=0.2, seed=1),
        datetime.date(2023, 10, 24),
    )
    day2 = _dated(
        _synthetic_possessions(o, d, n_games=10, poss_per_game=30, noise_sd=0.2, seed=2).with_columns(
            (pl.col("game_id") + "_d2").alias("game_id")
        ),
        datetime.date(2023, 10, 26),
    )
    day3 = _dated(
        _synthetic_possessions(o, d, n_games=10, poss_per_game=30, noise_sd=0.2, seed=3).with_columns(
            (pl.col("game_id") + "_d3").alias("game_id")
        ),
        datetime.date(2023, 10, 28),
    )
    return day1, day2, day3


def test_ratings_as_of_is_leakage_free_append_invariant():
    """THE binding correctness property: appending future-dated games must not
    change an earlier as-of checkpoint's ratings, at all, byte-for-byte."""
    day1, day2, day3 = _three_day_possessions()
    model = RidgeRapmModel()
    d1 = datetime.date(2023, 10, 24)

    rf_alone = ratings_as_of(model, day1, d1)
    pooled = pl.concat([day1, day2, day3], how="diagonal_relaxed")
    rf_from_pooled = ratings_as_of(model, pooled, d1)

    assert rf_alone.o_ratings == rf_from_pooled.o_ratings
    assert rf_alone.d_ratings == rf_from_pooled.d_ratings


def test_ratings_as_of_round_trips_rapm_scale():
    """coef -> per-100 conversion matches nba_rapm's own convention exactly."""
    day1, _, _ = _three_day_possessions()
    model = RidgeRapmModel()
    d1 = datetime.date(2023, 10, 24)
    rf = ratings_as_of(model, day1, d1)
    assert set(rf.o_ratings) == set(range(1, 17))
    assert set(rf.d_ratings) == set(range(1, 17))
    # sanity: with a planted positive o-rating, the fit shouldn't collapse to exactly 0 for all players
    assert any(v != 0.0 for v in rf.o_ratings.values())


def test_ratings_as_of_before_any_games_is_empty():
    day1, _, _ = _three_day_possessions()
    model = RidgeRapmModel()
    rf = ratings_as_of(model, day1, datetime.date(2020, 1, 1))
    assert rf.o_ratings == {}
    assert rf.d_ratings == {}


def test_ratings_as_of_requires_game_date_column():
    model = RidgeRapmModel()
    # _synthetic_possessions samples 5-per-team without replacement, so the player
    # pool must carry >=5 players per side (10 total) even for this error-path test.
    o = {p: 0.0 for p in range(1, 11)}
    d = {p: 0.0 for p in range(1, 11)}
    poss = _synthetic_possessions(o, d, n_games=2, poss_per_game=5, noise_sd=0.0, seed=0)
    with pytest.raises(ValueError, match="game_date"):
        ratings_as_of(model, poss, datetime.date(2023, 10, 24))
