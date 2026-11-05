"""WNBA game flow and sequence stats (BB-D27) -- thin shim over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_game_flow` is league-agnostic: ``league_id="10"`` reads the
``wnba_stats_pbp`` / ``wnba_stats_possessions`` / ``wnba_stats_schedules`` releases, 10-minute
quarters (20-minute halves through 2005) and the WNBA in-game win-probability model.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_game_flow import nba_game_flow, nba_team_season_flow
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

wnba_game_flow = functools.partial(nba_game_flow, league_id="10")
functools.update_wrapper(wnba_game_flow, nba_game_flow)
wnba_game_flow.__signature__ = wnba_signature(nba_game_flow)  # type: ignore[attr-defined]
wnba_game_flow.__doc__ = wnba_doc(
    "WNBA game flow per team and game: runs, droughts, kills, lead changes, time leading, game control "
    "(league_id='10'). See sportsdataverse.nba.nba_game_flow.nba_game_flow.",
    nba_game_flow,
)

wnba_team_season_flow = functools.partial(nba_team_season_flow, league_id="10")
functools.update_wrapper(wnba_team_season_flow, nba_team_season_flow)
wnba_team_season_flow.__signature__ = wnba_signature(nba_team_season_flow)  # type: ignore[attr-defined]
wnba_team_season_flow.__doc__ = wnba_doc(
    "WNBA game flow per team and season, with ranks (league_id='10'). "
    "See sportsdataverse.nba.nba_game_flow.nba_team_season_flow.",
    nba_team_season_flow,
)

__all__ = ["wnba_game_flow", "wnba_team_season_flow"]
