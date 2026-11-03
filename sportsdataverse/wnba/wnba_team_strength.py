"""WNBA team strength (BB-D19) -- thin shim over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_team_strength` is league-agnostic: ``league_id="10"`` reads
the ``wnba_stats_possessions`` / ``wnba_stats_schedules`` releases, 10-minute quarters and a
40-minute pace basis.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_team_strength import nba_team_strength as _core
from sportsdataverse.wnba._bind import wnba_doc

wnba_team_strength = functools.partial(_core, league_id="10")
functools.update_wrapper(wnba_team_strength, _core)
wnba_team_strength.__doc__ = wnba_doc(
    "WNBA team strength: adjusted ratings, SOS, SOV, pace and four factors (league_id='10'). "
    "See sportsdataverse.nba.nba_team_strength.nba_team_strength.",
    _core,
)

__all__ = ["wnba_team_strength"]
