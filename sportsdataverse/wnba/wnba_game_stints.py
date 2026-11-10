"""WNBA game stints, in-game units and on/off (BB-D32) -- thin shim over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_game_stints` is league-agnostic: ``league_id="10"`` reads the
``wnba_stats_pbp`` / ``wnba_stats_game_lineups`` / ``wnba_stats_possessions`` /
``wnba_stats_schedules`` releases and 10-minute quarters.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_game_stints import nba_game_on_off, nba_game_stints, nba_game_units
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

wnba_game_stints = functools.partial(nba_game_stints, league_id="10")
functools.update_wrapper(wnba_game_stints, nba_game_stints)
wnba_game_stints.__signature__ = wnba_signature(nba_game_stints)  # type: ignore[attr-defined]
wnba_game_stints.__doc__ = wnba_doc(
    "WNBA game stints (league_id='10'). See sportsdataverse.nba.nba_game_stints.nba_game_stints.",
    nba_game_stints,
)

wnba_game_units = functools.partial(nba_game_units, league_id="10")
functools.update_wrapper(wnba_game_units, nba_game_units)
wnba_game_units.__signature__ = wnba_signature(nba_game_units)  # type: ignore[attr-defined]
wnba_game_units.__doc__ = wnba_doc(
    "WNBA in-game units (league_id='10'). See sportsdataverse.nba.nba_game_stints.nba_game_units.",
    nba_game_units,
)

wnba_game_on_off = functools.partial(nba_game_on_off, league_id="10")
functools.update_wrapper(wnba_game_on_off, nba_game_on_off)
wnba_game_on_off.__signature__ = wnba_signature(nba_game_on_off)  # type: ignore[attr-defined]
wnba_game_on_off.__doc__ = wnba_doc(
    "WNBA in-game on/off (league_id='10'). See sportsdataverse.nba.nba_game_stints.nba_game_on_off.",
    nba_game_on_off,
)

__all__ = ["wnba_game_on_off", "wnba_game_stints", "wnba_game_units"]
