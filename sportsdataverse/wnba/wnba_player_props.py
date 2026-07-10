"""WNBA player props -- thin shim over the NBA core (league_id='10').

Binds ``league_id="10"`` on :func:`sportsdataverse.nba.nba_player_props.nba_player_props`
(reads the WNBA box logs + ratings). Rate/distribution helpers are league-agnostic
and re-exported by reference.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_player_props import nba_player_props as _core
from sportsdataverse.nba.nba_player_props import player_rates as player_rates
from sportsdataverse.nba.nba_player_props import project_player_line as project_player_line
from sportsdataverse.nba.nba_player_props import prob_over as prob_over
from sportsdataverse.nba.nba_player_props import prop_distribution as prop_distribution
from sportsdataverse.nba.nba_player_props import team_pace_projection as team_pace_projection

wnba_player_props = functools.partial(_core, league_id="10")
functools.update_wrapper(wnba_player_props, _core)
wnba_player_props.__doc__ = (
    "WNBA player props (league_id='10'). See sportsdataverse.nba.nba_player_props.nba_player_props."
)

__all__ = [
    "player_rates",
    "project_player_line",
    "prob_over",
    "prop_distribution",
    "team_pace_projection",
    "wnba_player_props",
]
