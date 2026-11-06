"""WNBA player on/off and WOWY pairs (BB-D5) -- thin shims over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_player_on_off` is league-agnostic: ``league_id="10"`` reads the
``wnba_stats_*`` releases (10-minute quarters, two 20-minute halves through 2005) and a
200-possession rank gate.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_player_on_off import nba_player_on_off, nba_player_wowy
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

wnba_player_on_off = functools.partial(nba_player_on_off, league_id="10")
functools.update_wrapper(wnba_player_on_off, nba_player_on_off)
wnba_player_on_off.__signature__ = wnba_signature(nba_player_on_off)  # type: ignore[attr-defined]
wnba_player_on_off.__doc__ = wnba_doc(
    "WNBA player on/off (league_id='10'). See sportsdataverse.nba.nba_player_on_off.nba_player_on_off.",
    nba_player_on_off,
)

wnba_player_wowy = functools.partial(nba_player_wowy, league_id="10")
functools.update_wrapper(wnba_player_wowy, nba_player_wowy)
wnba_player_wowy.__signature__ = wnba_signature(nba_player_wowy)  # type: ignore[attr-defined]
wnba_player_wowy.__doc__ = wnba_doc(
    "WNBA WOWY pairs (league_id='10'). See sportsdataverse.nba.nba_player_on_off.nba_player_wowy.",
    nba_player_wowy,
)

__all__ = ["wnba_player_on_off", "wnba_player_wowy"]
