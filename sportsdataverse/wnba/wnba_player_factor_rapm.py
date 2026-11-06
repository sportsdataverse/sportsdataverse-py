"""WNBA four-factor RAPM split (BB-D10) -- a thin shim over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_player_factor_rapm` is league-agnostic: ``league_id="10"`` reads the
``wnba_stats_*`` releases and a 200-possession rank gate.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_player_factor_rapm import nba_player_factor_rapm
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

wnba_player_factor_rapm = functools.partial(nba_player_factor_rapm, league_id="10")
functools.update_wrapper(wnba_player_factor_rapm, nba_player_factor_rapm)
wnba_player_factor_rapm.__signature__ = wnba_signature(nba_player_factor_rapm)  # type: ignore[attr-defined]
wnba_player_factor_rapm.__doc__ = wnba_doc(
    "WNBA four-factor RAPM split (league_id='10'). See sportsdataverse.nba.nba_player_factor_rapm.nba_player_factor_rapm.",
    nba_player_factor_rapm,
)

__all__ = ["wnba_player_factor_rapm"]
