"""WNBA season team rates from possessions (BB-D3) -- thin shim over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_team_season_rates` is league-agnostic: ``league_id="10"`` reads
the ``wnba_stats_possessions`` / ``wnba_stats_schedules`` releases, 10-minute quarters and a
40-minute pace basis.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_team_season_rates import nba_team_game_possessions as _games_core
from sportsdataverse.nba.nba_team_season_rates import nba_team_season_rates as _core
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

wnba_team_season_rates = functools.partial(_core, league_id="10")
functools.update_wrapper(wnba_team_season_rates, _core)
wnba_team_season_rates.__signature__ = wnba_signature(_core)  # type: ignore[attr-defined]
wnba_team_season_rates.__doc__ = wnba_doc(
    "WNBA season team rates from possessions (league_id='10'). "
    "See sportsdataverse.nba.nba_team_season_rates.nba_team_season_rates.",
    _core,
)

wnba_team_game_possessions = functools.partial(_games_core, league_id="10")
functools.update_wrapper(wnba_team_game_possessions, _games_core)
wnba_team_game_possessions.__signature__ = wnba_signature(_games_core)  # type: ignore[attr-defined]
wnba_team_game_possessions.__doc__ = wnba_doc(
    "WNBA per team and game possession counts (league_id='10'). "
    "See sportsdataverse.nba.nba_team_season_rates.nba_team_game_possessions.",
    _games_core,
)

__all__ = ["wnba_team_game_possessions", "wnba_team_season_rates"]
