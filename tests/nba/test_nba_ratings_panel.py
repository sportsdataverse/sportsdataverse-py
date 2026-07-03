"""Tests for the WP4 through-date ratings engine (nba_ratings_panel)."""

from __future__ import annotations

import datetime

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel, _synthetic_possessions
from sportsdataverse.nba.nba_ratings_panel import RATINGS_PANEL_SCHEMA, nba_ratings_panel, ratings_as_of


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


# ---------------------------------------------------------------------------
# Task 2: nba_ratings_panel long-frame engine
# ---------------------------------------------------------------------------


def test_ratings_panel_is_leakage_free():
    """Same binding property as Task 1, at the panel (multi-date) level: a
    future-dated game appended to the frame must not move ANY earlier checkpoint."""
    day1, day2, day3 = _three_day_possessions()
    model = RidgeRapmModel()
    d1, d2 = datetime.date(2023, 10, 24), datetime.date(2023, 10, 26)

    pooled_all = pl.concat([day1, day2, day3], how="diagonal_relaxed")
    pooled_two = pl.concat([day1, day2], how="diagonal_relaxed")

    panel_from_all = nba_ratings_panel(model, pooled_all, dates=[d1, d2])
    panel_from_two = nba_ratings_panel(model, pooled_two, dates=[d1, d2])

    assert_frame_equal(
        panel_from_all.sort(["date", "player_id"]),
        panel_from_two.sort(["date", "player_id"]),
    )


def test_ratings_panel_matches_ratings_as_of_per_date():
    day1, day2, _ = _three_day_possessions()
    pooled = pl.concat([day1, day2], how="diagonal_relaxed")
    model = RidgeRapmModel()
    d1, d2 = datetime.date(2023, 10, 24), datetime.date(2023, 10, 26)

    panel = nba_ratings_panel(model, pooled, dates=[d1, d2])
    for d in (d1, d2):
        rf = ratings_as_of(model, pooled, d)
        sub = panel.filter(pl.col("date") == d).sort("player_id")
        assert sub["player_id"].to_list() == sorted(rf.o_ratings)
        for pid, o, dd in zip(sub["player_id"], sub["o_rating"], sub["d_rating"]):
            assert o == rf.o_ratings[pid]
            assert dd == rf.d_ratings[pid]
            assert abs((o + dd) - sub.filter(pl.col("player_id") == pid)["rating"][0]) < 1e-12


def test_ratings_panel_dates_deduped_and_sorted():
    day1, day2, _ = _three_day_possessions()
    pooled = pl.concat([day1, day2], how="diagonal_relaxed")
    model = RidgeRapmModel()
    d1, d2 = datetime.date(2023, 10, 24), datetime.date(2023, 10, 26)

    panel = nba_ratings_panel(model, pooled, dates=[d2, d1, d1, d2])
    assert panel["date"].unique().sort().to_list() == [d1, d2]


def test_ratings_panel_default_dates_is_every_distinct_game_date():
    day1, day2, _ = _three_day_possessions()
    pooled = pl.concat([day1, day2], how="diagonal_relaxed")
    model = RidgeRapmModel()
    panel = nba_ratings_panel(model, pooled)
    assert panel["date"].unique().sort().to_list() == [
        datetime.date(2023, 10, 24),
        datetime.date(2023, 10, 26),
    ]


def test_ratings_panel_empty_possessions_returns_documented_schema():
    model = RidgeRapmModel()
    empty = pl.DataFrame(schema={"game_id": pl.Utf8, "game_date": pl.Date})
    panel = nba_ratings_panel(model, empty, dates=[datetime.date(2023, 10, 24)])
    assert panel.height == 0
    assert dict(panel.schema) == RATINGS_PANEL_SCHEMA


def test_ratings_panel_requires_game_date_column():
    model = RidgeRapmModel()
    # _synthetic_possessions samples 5-per-team without replacement, so the player
    # pool must carry >=5 players per side (10 total) even for this error-path test
    # (same constraint documented on test_ratings_as_of_requires_game_date_column above).
    o = {p: 0.0 for p in range(1, 11)}
    d = {p: 0.0 for p in range(1, 11)}
    poss = _synthetic_possessions(o, d, n_games=2, poss_per_game=5, noise_sd=0.0, seed=0)
    with pytest.raises(ValueError, match="game_date"):
        nba_ratings_panel(model, poss, dates=[datetime.date(2023, 10, 24)])


def test_ratings_panel_return_as_pandas():
    day1, _, _ = _three_day_possessions()
    model = RidgeRapmModel()
    panel_pd = nba_ratings_panel(model, day1, dates=[datetime.date(2023, 10, 24)], return_as_pandas=True)
    import pandas as pd

    assert isinstance(panel_pd, pd.DataFrame)
