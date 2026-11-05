"""WNBA situational splits (BB-D21) -- thin shim over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_situational` is league-agnostic: ``league_id="10"`` reads the
``wnba_stats_possessions`` / ``wnba_stats_pbp`` / ``wnba_stats_schedules`` releases, 10-minute
quarters (20-minute halves through 2005) and ranks the top 4 teams for the ``opponent`` family.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_situational import nba_situational_lineup, nba_situational_player, nba_situational_team
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

wnba_situational_team = functools.partial(nba_situational_team, league_id="10")
functools.update_wrapper(wnba_situational_team, nba_situational_team)
wnba_situational_team.__signature__ = wnba_signature(nba_situational_team)  # type: ignore[attr-defined]
wnba_situational_team.__doc__ = wnba_doc(
    "WNBA team splits by situation (league_id='10'). See sportsdataverse.nba.nba_situational.nba_situational_team.",
    nba_situational_team,
)

wnba_situational_player = functools.partial(nba_situational_player, league_id="10")
functools.update_wrapper(wnba_situational_player, nba_situational_player)
wnba_situational_player.__signature__ = wnba_signature(nba_situational_player)  # type: ignore[attr-defined]
wnba_situational_player.__doc__ = wnba_doc(
    "WNBA player on-floor splits by situation (league_id='10'). "
    "See sportsdataverse.nba.nba_situational.nba_situational_player.",
    nba_situational_player,
)

wnba_situational_lineup = functools.partial(nba_situational_lineup, league_id="10")
functools.update_wrapper(wnba_situational_lineup, nba_situational_lineup)
wnba_situational_lineup.__signature__ = wnba_signature(nba_situational_lineup)  # type: ignore[attr-defined]
wnba_situational_lineup.__doc__ = wnba_doc(
    "WNBA five-man unit splits by situation (league_id='10'). "
    "See sportsdataverse.nba.nba_situational.nba_situational_lineup.",
    nba_situational_lineup,
)

__all__ = ["wnba_situational_lineup", "wnba_situational_player", "wnba_situational_team"]
