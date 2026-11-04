"""WNBA game impact box (BB-D6) -- thin shim over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_game_impact` is league-agnostic: ``league_id="10"`` reads the
``wnba_stats_pbp`` / ``wnba_stats_game_lineups`` / ``wnba_stats_possessions`` /
``wnba_stats_schedules`` releases and 10-minute quarters.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_game_impact import nba_game_lineups_used, nba_game_player_impact
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

wnba_game_player_impact = functools.partial(nba_game_player_impact, league_id="10")
functools.update_wrapper(wnba_game_player_impact, nba_game_player_impact)
wnba_game_player_impact.__signature__ = wnba_signature(nba_game_player_impact)  # type: ignore[attr-defined]
wnba_game_player_impact.__doc__ = wnba_doc(
    "WNBA game impact box (league_id='10'). See sportsdataverse.nba.nba_game_impact.nba_game_player_impact.",
    nba_game_player_impact,
)

wnba_game_lineups_used = functools.partial(nba_game_lineups_used, league_id="10")
functools.update_wrapper(wnba_game_lineups_used, nba_game_lineups_used)
wnba_game_lineups_used.__signature__ = wnba_signature(nba_game_lineups_used)  # type: ignore[attr-defined]
wnba_game_lineups_used.__doc__ = wnba_doc(
    "WNBA lineups used per game (league_id='10'). See sportsdataverse.nba.nba_game_impact.nba_game_lineups_used.",
    nba_game_lineups_used,
)

__all__ = ["wnba_game_lineups_used", "wnba_game_player_impact"]
