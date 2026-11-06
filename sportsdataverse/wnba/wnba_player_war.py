"""WNBA WAR and VORP (BB-D26) -- thin shim over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_player_war` is league-agnostic: ``league_id="10"`` reads the
``wnba_player_impact`` / ``wnba_stats_possessions`` / ``wnba_stats_schedules`` releases and the
WNBA replacement level and points per win.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_player_war import nba_player_war as _core
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

wnba_player_war = functools.partial(_core, league_id="10")
functools.update_wrapper(wnba_player_war, _core)
wnba_player_war.__signature__ = wnba_signature(_core)  # type: ignore[attr-defined]
wnba_player_war.__doc__ = wnba_doc(
    "WNBA WAR and VORP per player-season from our BPM, RAPM and BOBAN (league_id='10'). "
    "See sportsdataverse.nba.nba_player_war.nba_player_war.",
    _core,
)

__all__ = ["wnba_player_war"]
