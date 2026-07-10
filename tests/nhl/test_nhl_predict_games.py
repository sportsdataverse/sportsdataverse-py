"""Tests for :func:`nhl_predict_games` (Task 2.2 -- vectorized pregame predictions)."""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.nhl.nhl_market import nhl_predict_games


def _games():
    return pl.DataFrame(
        {
            "game_id": ["1", "2"],
            "home_team": ["TOR", "BOS"],
            "away_team": ["BOS", "TOR"],
            "neutral_site": [False, False],
        }
    )


def _ratings():
    return pl.DataFrame(
        {
            "team": ["TOR", "BOS"],
            "adj_xgf": [2.9, 2.6],
            "adj_xga": [2.3, 2.4],
        }
    )


def test_nhl_predict_games_schema_and_values():
    out = nhl_predict_games(_games(), _ratings())
    assert out.columns == [
        "game_id",
        "home_team",
        "away_team",
        "neutral_site",
        "exp_margin",
        "home_win_prob",
        "exp_total",
        "market_edge",
    ]
    assert out.height == 2
    assert out["market_edge"].null_count() == 2  # no odds supplied


def test_nhl_predict_games_dtype_guard_raises_on_mismatch():
    games = _games().with_columns(pl.col("home_team").cast(pl.Categorical))
    # Force a genuine dtype mismatch: ratings.team stays Utf8, games.home_team becomes something else.
    ratings = _ratings()
    with pytest.raises(AssertionError):
        nhl_predict_games(games, ratings)


def test_nhl_predict_games_market_edge_with_odds():
    odds = pl.DataFrame({"game_id": ["1", "2"], "close_puck_line_home": [0.3, -0.2]})
    out = nhl_predict_games(_games(), _ratings(), odds=odds)
    assert out["market_edge"].null_count() == 0
    row = out.filter(pl.col("game_id") == "1").row(0, named=True)
    assert abs(row["market_edge"] - (row["exp_margin"] - 0.3)) < 1e-9


def test_nhl_predict_games_empty_input_returns_documented_schema():
    out = nhl_predict_games(pl.DataFrame(), pl.DataFrame())
    assert out.height == 0
    assert out.columns == [
        "game_id",
        "home_team",
        "away_team",
        "neutral_site",
        "exp_margin",
        "home_win_prob",
        "exp_total",
        "market_edge",
    ]
