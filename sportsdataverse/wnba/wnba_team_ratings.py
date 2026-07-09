"""WNBA team power ratings -- thin shim over the league-agnostic NBA core (league_id='10').

The ratings engine is league-agnostic (:mod:`sportsdataverse.nba.nba_team_ratings`);
``league_id="10"`` selects the WNBA constants (HFA, pace/efficiency baselines,
40-minute game) and the WNBA data loaders. G-League needs no shim -- call
``nba_team_ratings(..., league_id="20")``.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_team_ratings import adjust_efficiency as adjust_efficiency
from sportsdataverse.nba.nba_team_ratings import adjust_pace as adjust_pace
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings as _core
from sportsdataverse.nba.nba_team_ratings import raw_game_efficiency as raw_game_efficiency

wnba_team_ratings = functools.partial(_core, league_id="10")
functools.update_wrapper(wnba_team_ratings, _core)
wnba_team_ratings.__doc__ = (
    "WNBA team ratings (league_id='10'). See sportsdataverse.nba.nba_team_ratings.nba_team_ratings."
)

__all__ = ["adjust_efficiency", "adjust_pace", "raw_game_efficiency", "wnba_team_ratings"]
