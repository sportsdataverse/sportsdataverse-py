"""WNBA luck and close-game record (BB-D22) -- thin shim over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_team_luck` is league-agnostic: ``league_id="10"`` reads the
``wnba_stats_possessions`` / ``wnba_stats_schedules`` releases, 10-minute quarters and the WNBA
Pythagorean exponent.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_team_luck import nba_team_luck as _core
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

wnba_team_luck = functools.partial(_core, league_id="10")
functools.update_wrapper(wnba_team_luck, _core)
wnba_team_luck.__signature__ = wnba_signature(_core)  # type: ignore[attr-defined]
wnba_team_luck.__doc__ = wnba_doc(
    "WNBA luck and close-game record: Pythagorean expected wins vs actual, close-game W-L (league_id='10'). "
    "See sportsdataverse.nba.nba_team_luck.nba_team_luck.",
    _core,
)

__all__ = ["wnba_team_luck"]
