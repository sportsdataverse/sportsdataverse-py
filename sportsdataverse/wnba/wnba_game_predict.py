"""WNBA pregame + in-game predictions -- thin shim over the NBA core (league_id='10').

Binds ``league_id="10"`` on the league-agnostic closed forms + the in-game-WP
scorer (:mod:`sportsdataverse.nba.nba_game_predict`). The WNBA constants (HFA,
margin sigma, pace anchor) and the bundled ``wnba_in_game_wp.ubj`` artifact are
selected by that league id. G-League: call the nba functions with ``league_id="20"``.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_game_predict import expected_possessions as _expected_possessions
from sportsdataverse.nba.nba_game_predict import in_game_features as in_game_features
from sportsdataverse.nba.nba_game_predict import nba_in_game_win_prob as _in_game
from sportsdataverse.nba.nba_game_predict import nba_predict_games as _predict_games
from sportsdataverse.nba.nba_game_predict import predict_margin as _predict_margin
from sportsdataverse.nba.nba_game_predict import predict_total as _predict_total
from sportsdataverse.nba.nba_game_predict import win_prob_from_margin as _win_prob_from_margin


def _bind(fn, name: str):  # type: ignore[no-untyped-def]
    p = functools.partial(fn, league_id="10")
    functools.update_wrapper(p, fn)
    p.__doc__ = f"WNBA {name} (league_id='10'). See sportsdataverse.nba.nba_game_predict.{fn.__name__}."
    return p


wnba_expected_possessions = _bind(_expected_possessions, "expected possessions")
wnba_predict_margin = _bind(_predict_margin, "expected margin")
wnba_win_prob_from_margin = _bind(_win_prob_from_margin, "home win probability")
wnba_predict_total = _bind(_predict_total, "expected total")
wnba_predict_games = _bind(_predict_games, "vectorized pregame predictions")
wnba_in_game_win_prob = _bind(_in_game, "in-game win probability")

__all__ = [
    "in_game_features",
    "wnba_expected_possessions",
    "wnba_in_game_win_prob",
    "wnba_predict_games",
    "wnba_predict_margin",
    "wnba_predict_total",
    "wnba_win_prob_from_margin",
]
