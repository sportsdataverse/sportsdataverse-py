"""Women's college basketball pregame + in-game predictions.

Thin shim over :mod:`sportsdataverse.mbb.mbb_game_predict` -- the closed
forms and the in-game WP scorer are league-agnostic; every women's-specific
number (HFA, margin sigma, em_scale, the bundled ``wbb_in_game_wp.ubj``
artifact) comes from ``LEAGUE_CONSTANTS["womens"]``. The league-agnostic
feature extractor is re-exported **by reference**.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_game_predict import wbb_predict_games
        preds = wbb_predict_games(games, ratings)

See Also:
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
"""

from __future__ import annotations

from typing import Union

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_game_predict import (
    in_game_features,
    mbb_in_game_win_prob,
    mbb_predict_games,
)
from sportsdataverse.mbb.mbb_game_predict import predict_margin as _predict_margin
from sportsdataverse.mbb.mbb_game_predict import predict_total as _predict_total
from sportsdataverse.mbb.mbb_game_predict import win_prob_from_margin as _win_prob_from_margin

__all__ = [
    "in_game_features",
    "predict_margin",
    "predict_total",
    "wbb_in_game_win_prob",
    "wbb_predict_games",
    "win_prob_from_margin",
]


def predict_margin(home_adj_em: float, away_adj_em: float, neutral: bool = False) -> float:
    """Women's expected margin -- :func:`mbb predict_margin <sportsdataverse.mbb.mbb_game_predict.predict_margin>` with ``league="womens"``.

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_game_predict import predict_margin
            predict_margin(20.0, 10.0)
    """
    return _predict_margin(home_adj_em, away_adj_em, neutral, league="womens")


def win_prob_from_margin(exp_margin: float) -> float:
    """Women's win prob -- :func:`mbb win_prob_from_margin <sportsdataverse.mbb.mbb_game_predict.win_prob_from_margin>` with ``league="womens"``.

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_game_predict import win_prob_from_margin
            win_prob_from_margin(5.0)
    """
    return _win_prob_from_margin(exp_margin, league="womens")


def predict_total(
    home_adj_o: float,
    home_adj_d: float,
    away_adj_o: float,
    away_adj_d: float,
    home_tempo: float,
    away_tempo: float,
) -> float:
    """Women's expected total -- :func:`mbb predict_total <sportsdataverse.mbb.mbb_game_predict.predict_total>` with ``league="womens"``.

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_game_predict import predict_total
            predict_total(100.0, 88.0, 96.0, 92.0, 72.0, 70.0)
    """
    return _predict_total(home_adj_o, home_adj_d, away_adj_o, away_adj_d, home_tempo, away_tempo, league="womens")


def wbb_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Women's vectorized pregame predictions over a schedule.

    Delegates to
    :func:`sportsdataverse.mbb.mbb_game_predict.mbb_predict_games` with
    ``league="womens"`` -- see that function for the full contract.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_predict_games, wbb_team_ratings
            preds = wbb_predict_games(games, wbb_team_ratings(2024))
    """
    if return_as_pandas:
        return mbb_predict_games(games, ratings, league="womens", return_as_pandas=True)
    return mbb_predict_games(games, ratings, league="womens")


def wbb_in_game_win_prob(
    pbp: pl.DataFrame,
    pregame_home_prob: float,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Women's per-play in-game win probability (bundled ``wbb_in_game_wp.ubj``).

    Delegates to
    :func:`sportsdataverse.mbb.mbb_game_predict.mbb_in_game_win_prob` with
    ``league="womens"`` -- see that function for the full contract.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_in_game_win_prob
            wp = wbb_in_game_win_prob(pbp, 0.62)
    """
    if return_as_pandas:
        return mbb_in_game_win_prob(pbp, pregame_home_prob, league="womens", return_as_pandas=True)
    return mbb_in_game_win_prob(pbp, pregame_home_prob, league="womens")
