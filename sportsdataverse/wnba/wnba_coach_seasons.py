"""WNBA coach seasons and careers (BB-D8) -- thin shim over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_coach_seasons` is league-agnostic: ``league_id="10"`` reads the
``wnba_stats_coaches`` / ``wnba_stats_schedules`` / ``wnba_stats_possessions`` releases, ESPN's
WNBA coach records and 40-minute pace.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_coach_seasons import nba_coach_careers as _careers
from sportsdataverse.nba.nba_coach_seasons import nba_coach_seasons as _seasons
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

wnba_coach_seasons = functools.partial(_seasons, league_id="10")
functools.update_wrapper(wnba_coach_seasons, _seasons)
wnba_coach_seasons.__signature__ = wnba_signature(_seasons)  # type: ignore[attr-defined]
wnba_coach_seasons.__doc__ = wnba_doc(
    "WNBA head-coach seasons: W-L, ratings and four factors over each coach's games (league_id='10'). "
    "See sportsdataverse.nba.nba_coach_seasons.nba_coach_seasons.",
    _seasons,
)

wnba_coach_careers = functools.partial(_careers, league_id="10")
functools.update_wrapper(wnba_coach_careers, _careers)
wnba_coach_careers.__signature__ = wnba_signature(_careers)  # type: ignore[attr-defined]
wnba_coach_careers.__doc__ = wnba_doc(
    "WNBA head-coach careers over the given seasons (league_id='10'). "
    "See sportsdataverse.nba.nba_coach_seasons.nba_coach_careers.",
    _careers,
)

__all__ = ["wnba_coach_careers", "wnba_coach_seasons"]
