"""WNBA player props -- thin shim over the NBA core (league_id='10').

Binds ``league_id="10"`` on :func:`sportsdataverse.nba.nba_player_props.nba_player_props`
(reads the WNBA box logs + ratings) and on the league-dependent helpers
(:func:`prop_distribution`, :func:`prob_over`, :func:`team_pace_projection`), so WNBA props use
the WNBA-fitted SD law, dispersion and pace. :func:`player_rates` and
:func:`project_player_line` are league-agnostic and re-exported by reference.
"""

from __future__ import annotations

import functools
from typing import Any, Callable

from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

from sportsdataverse.nba import nba_player_props as _nba
from sportsdataverse.nba.nba_player_props import player_rates as player_rates
from sportsdataverse.nba.nba_player_props import project_player_line as project_player_line


def _bind(core: Callable[..., Any], summary: str) -> functools.partial[Any]:
    bound = functools.partial(core, league_id="10")
    functools.update_wrapper(bound, core)
    bound.__signature__ = wnba_signature(core)  # type: ignore[attr-defined]
    bound.__doc__ = wnba_doc(f"{summary} See sportsdataverse.nba.nba_player_props.{core.__name__}.", core)
    return bound


wnba_player_props = _bind(_nba.nba_player_props, "WNBA player props (league_id='10').")
prop_distribution = _bind(_nba.prop_distribution, "WNBA prop distribution (league_id='10', WNBA-fitted SD law).")
prob_over = _bind(_nba.prob_over, "WNBA P(stat > line) (league_id='10', WNBA-fitted SD law).")
team_pace_projection = _bind(_nba.team_pace_projection, "WNBA matchup expected possessions (league_id='10').")

__all__ = [
    "player_rates",
    "project_player_line",
    "prob_over",
    "prop_distribution",
    "team_pace_projection",
    "wnba_player_props",
]
