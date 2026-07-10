"""Tests for :func:`in_game_features` (Task 3.1 -- in-game WP feature extraction)."""

from __future__ import annotations

import math

import polars as pl

from sportsdataverse.nhl.nhl_market import in_game_features


def _mini_pbp():
    return pl.DataFrame(
        {
            "game_id": [1, 1, 1],
            "home_score": [0, 1, 1],
            "away_score": [0, 0, 1],
            "game_seconds_remaining": [3600, 1800, 30],
            "home_skaters": [5, 5, 6],
            "away_skaters": [5, 5, 5],
            "home_goalie_in": [1, 1, 0],
            "away_goalie_in": [1, 1, 1],
        }
    )


def test_score_diff_and_sec_remaining():
    out = in_game_features(_mini_pbp(), pregame_home_prob=0.6)
    assert out["score_diff"].to_list() == [0, 1, 0]
    assert out["sec_remaining"].to_list() == [3600.0, 1800.0, 30.0]
    # sec_remaining decreasing across the play sequence
    assert out["sec_remaining"].to_list() == sorted(out["sec_remaining"].to_list(), reverse=True)


def test_sqrt_sec_remaining():
    out = in_game_features(_mini_pbp(), pregame_home_prob=0.6)
    assert abs(out["sqrt_sec_remaining"][0] - math.sqrt(3600.0)) < 1e-9


def test_strength_diff_reflects_pulled_goalie_6v5():
    out = in_game_features(_mini_pbp(), pregame_home_prob=0.6)
    # Row 3: home_skaters=6, away_skaters=5, home goalie pulled -> strength_diff = +1 (6v5 push)
    assert out["strength_diff"][2] == 1
    assert out["home_goalie_pulled"][2] == 1
    assert out["away_goalie_pulled"][2] == 0
    assert out["home_goalie_pulled"][0] == 0


def test_pregame_logit_matches_log_odds():
    p = 0.6
    out = in_game_features(_mini_pbp(), pregame_home_prob=p)
    expected_logit = math.log(p / (1 - p))
    assert abs(out["pregame_logit"][0] - expected_logit) < 1e-9
    # constant across all plays in the game
    assert out["pregame_logit"].n_unique() == 1


def test_empty_input_returns_documented_schema():
    empty = pl.DataFrame(
        schema={
            "game_id": pl.Int64,
            "home_score": pl.Int64,
            "away_score": pl.Int64,
            "game_seconds_remaining": pl.Int64,
            "home_skaters": pl.Int64,
            "away_skaters": pl.Int64,
            "home_goalie_in": pl.Int64,
            "away_goalie_in": pl.Int64,
        }
    )
    out = in_game_features(empty, pregame_home_prob=0.5)
    assert out.height == 0
    assert out.columns == [
        "score_diff",
        "sec_remaining",
        "sqrt_sec_remaining",
        "strength_diff",
        "home_goalie_pulled",
        "away_goalie_pulled",
        "pregame_logit",
    ]
