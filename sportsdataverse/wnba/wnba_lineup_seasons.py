"""WNBA lineup seasons (BB-D4) -- thin shim over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_lineup_seasons` is league-agnostic: ``league_id="10"`` reads the
``wnba_stats_*`` releases, 10-minute quarters and a 200-possession rank gate.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_seasons
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

wnba_lineup_seasons = functools.partial(nba_lineup_seasons, league_id="10")
functools.update_wrapper(wnba_lineup_seasons, nba_lineup_seasons)
wnba_lineup_seasons.__signature__ = wnba_signature(nba_lineup_seasons)  # type: ignore[attr-defined]
wnba_lineup_seasons.__doc__ = wnba_doc(
    "WNBA lineup seasons (league_id='10'). See sportsdataverse.nba.nba_lineup_seasons.nba_lineup_seasons.",
    nba_lineup_seasons,
)

__all__ = ["wnba_lineup_seasons"]
