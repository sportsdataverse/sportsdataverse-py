"""WBB prediction-stack shims: delegation + women's-constants behavior.

Module handles come from ``importlib.import_module``: the packages'
``import *`` rebinds same-named attributes (e.g. ``wbb_team_ratings``) to the
FUNCTION, so ``from sportsdataverse.wbb import wbb_team_ratings`` would not
yield the module.
"""

import importlib

import numpy as np
import polars as pl

from sportsdataverse.mbb.mbb_bracketology import project_bracket as mbb_project_bracket
from sportsdataverse.wbb.wbb_bracketology import project_bracket as wbb_project_bracket

mbb_game_predict = importlib.import_module("sportsdataverse.mbb.mbb_game_predict")
mbb_prediction_constants = importlib.import_module("sportsdataverse.mbb.mbb_prediction_constants")
mbb_season_sim = importlib.import_module("sportsdataverse.mbb.mbb_season_sim")
mbb_team_ratings = importlib.import_module("sportsdataverse.mbb.mbb_team_ratings")
wbb_game_predict = importlib.import_module("sportsdataverse.wbb.wbb_game_predict")
wbb_prediction_constants = importlib.import_module("sportsdataverse.wbb.wbb_prediction_constants")
wbb_season_sim = importlib.import_module("sportsdataverse.wbb.wbb_season_sim")
wbb_strength_of_schedule = importlib.import_module("sportsdataverse.wbb.wbb_strength_of_schedule")
wbb_team_ratings = importlib.import_module("sportsdataverse.wbb.wbb_team_ratings")


def test_constants_reexported_by_reference():
    assert wbb_prediction_constants.get_constants is mbb_prediction_constants.get_constants
    assert wbb_prediction_constants.brier_score is mbb_prediction_constants.brier_score
    assert wbb_prediction_constants.LEAGUE_CONSTANTS is mbb_prediction_constants.LEAGUE_CONSTANTS


def test_league_agnostic_cores_reexported_by_reference():
    assert wbb_team_ratings.raw_game_efficiency is mbb_team_ratings.raw_game_efficiency
    assert wbb_team_ratings.adjust_efficiency is mbb_team_ratings.adjust_efficiency
    assert wbb_game_predict.in_game_features is mbb_game_predict.in_game_features
    assert wbb_project_bracket is mbb_project_bracket


def test_predict_margin_uses_womens_constants():
    c = mbb_prediction_constants.get_constants("womens")
    assert wbb_game_predict.predict_margin(10.0, 5.0) == 5.0 * c.em_scale + c.hfa
    assert wbb_game_predict.predict_margin(10.0, 5.0, neutral=True) == 5.0 * c.em_scale


def test_win_prob_uses_womens_sigma():
    c = mbb_prediction_constants.get_constants("womens")
    m = mbb_game_predict.win_prob_from_margin(8.0, league="womens")
    assert wbb_game_predict.win_prob_from_margin(8.0) == m
    assert c.margin_sd != mbb_prediction_constants.get_constants("mens").margin_sd


def test_wbb_predict_games_matches_mbb_with_womens_league():
    ratings = pl.DataFrame(
        {
            "season": [2024, 2024],
            "team_id": ["1", "2"],
            "adj_o": [100.0, 95.0],
            "adj_d": [90.0, 95.0],
            "adj_em": [10.0, 0.0],
            "adj_tempo": [72.0, 70.0],
        }
    )
    games = pl.DataFrame(
        {
            "game_id": ["g1"],
            "home_team_id": ["1"],
            "away_team_id": ["2"],
            "neutral_site": [False],
        }
    )
    ours = wbb_game_predict.wbb_predict_games(games, ratings)
    theirs = mbb_game_predict.mbb_predict_games(games, ratings, league="womens")
    assert ours.equals(theirs)


def test_wbb_in_game_win_prob_loads_womens_artifact():
    pbp = pl.DataFrame(
        {
            "game_id": [1],
            "start_game_seconds_remaining": [30],
            "home_score": [70],
            "away_score": [50],
            "team_id": [10],
            "home_team_id": [10],
        }
    )
    wp = wbb_game_predict.wbb_in_game_win_prob(pbp, 0.5)
    assert wp["home_win_prob"][0] > 0.95


def test_wbb_simulate_game_delegates_with_womens_league():
    a = wbb_season_sim.simulate_game(20.0, 0.0, True, np.random.default_rng(3))
    b = mbb_season_sim.simulate_game(20.0, 0.0, True, np.random.default_rng(3), league="womens")
    assert a == b


def test_wbb_season_and_bracket_sims_run():
    ratings = pl.DataFrame({"season": [2024, 2024], "team_id": ["A", "B"], "adj_em": [20.0, 0.0]})
    sched = pl.DataFrame(
        {
            "game_id": ["g1"],
            "season": [2024],
            "home_team_id": ["A"],
            "away_team_id": ["B"],
            "neutral_site": [True],
        }
    )
    season = wbb_season_sim.wbb_season_sim(ratings, sched, n_sims=200, seed=1)
    assert season.height == 2
    field = pl.DataFrame({"team_id": ["A", "B"], "seed": [1, 2]})
    bracket = wbb_season_sim.wbb_bracket_sim(field, ratings, n_sims=200, seed=1)
    assert abs(bracket.get_column("champion").sum() - 1.0) < 1e-9


def test_wbb_strength_of_schedule_core_is_shared():
    from sportsdataverse.mbb.mbb_strength_of_schedule import strength_of_schedule as core

    assert wbb_strength_of_schedule.strength_of_schedule is core
