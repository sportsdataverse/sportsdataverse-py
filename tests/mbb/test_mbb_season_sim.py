"""Tests for the season / bracket Monte Carlo (``mbb_season_sim``)."""

import numpy as np
import polars as pl

from sportsdataverse.mbb.mbb_season_sim import mbb_bracket_sim, mbb_season_sim, simulate_game


def _ratings() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [2024, 2024],
            "team_id": ["A", "B"],
            "adj_em": [30.0, 0.0],
            "conference": ["X", "X"],
        }
    )


def _schedule(n_games: int = 10) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": [f"g{i}" for i in range(n_games)],
            "season": [2024] * n_games,
            "home_team_id": ["A"] * n_games,
            "away_team_id": ["B"] * n_games,
            "neutral_site": [True] * n_games,
        }
    )


def test_simulate_game_dominant_team_usually_wins():
    rng = np.random.default_rng(0)
    wins = sum(simulate_game(30.0, 0.0, True, rng) for _ in range(500))
    assert wins > 400


def test_season_sim_dominant_team_wins_most_games():
    out = mbb_season_sim(_ratings(), _schedule(), n_sims=2000, seed=1)
    a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    assert a["exp_wins"] > 9.0
    assert a["conf_title_prob"] > 0.99


def test_season_sim_reproducible_with_seed():
    one = mbb_season_sim(_ratings(), _schedule(), n_sims=500, seed=7)
    two = mbb_season_sim(_ratings(), _schedule(), n_sims=500, seed=7)
    assert one.equals(two)


def test_season_sim_columns():
    out = mbb_season_sim(_ratings(), _schedule(), n_sims=100, seed=0)
    assert out.columns == ["season", "team_id", "exp_wins", "playoff_prob", "conf_title_prob"]


def _bracket_field() -> pl.DataFrame:
    # bracket slot order: (S, W) meet in game 1, (M, W2) in game 2
    return pl.DataFrame(
        {
            "team_id": ["S", "W", "M", "W2"],
            "seed": [1, 4, 2, 3],
        }
    )


def _bracket_ratings() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [2024] * 4,
            "team_id": ["S", "W", "M", "W2"],
            "adj_em": [30.0, -5.0, 15.0, 5.0],
        }
    )


def test_bracket_sim_strongest_team_highest_champion_prob():
    out = mbb_bracket_sim(_bracket_field(), _bracket_ratings(), n_sims=2000, seed=3)
    best = out.sort("champion", descending=True).row(0, named=True)
    assert best["team_id"] == "S"


def test_bracket_sim_probabilities_valid():
    out = mbb_bracket_sim(_bracket_field(), _bracket_ratings(), n_sims=1000, seed=5)
    for col in ("reach_r32", "reach_s16", "reach_e8", "reach_f4", "reach_final", "champion"):
        vals = out.get_column(col)
        assert ((vals >= 0.0) & (vals <= 1.0)).all(), col
    assert (out.get_column("champion") <= out.get_column("reach_final")).all()
    # each round's reach probabilities sum to the number of surviving slots
    assert abs(out.get_column("champion").sum() - 1.0) < 1e-9
    assert abs(out.get_column("reach_final").sum() - 2.0) < 1e-9


def test_bracket_sim_reproducible_with_seed():
    one = mbb_bracket_sim(_bracket_field(), _bracket_ratings(), n_sims=300, seed=11)
    two = mbb_bracket_sim(_bracket_field(), _bracket_ratings(), n_sims=300, seed=11)
    assert one.equals(two)
