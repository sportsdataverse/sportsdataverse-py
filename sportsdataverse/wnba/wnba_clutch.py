"""WNBA clutch model -- thin shim over the NBA core (league_id='10').

Binds ``league_id="10"`` on :func:`sportsdataverse.nba.nba_clutch.nba_team_clutch`
(which then reads the stats.wnba.com clutch + full-game feeds). The shrinkage +
delta helpers are league-agnostic and re-exported by reference.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_clutch import clutch_delta as clutch_delta
from sportsdataverse.nba.nba_clutch import nba_team_clutch as _core
from sportsdataverse.nba.nba_clutch import shrink_clutch as shrink_clutch

wnba_team_clutch = functools.partial(_core, league_id="10")
functools.update_wrapper(wnba_team_clutch, _core)
wnba_team_clutch.__doc__ = "WNBA clutch skill (league_id='10'). See sportsdataverse.nba.nba_clutch.nba_team_clutch."

__all__ = ["clutch_delta", "shrink_clutch", "wnba_team_clutch"]
