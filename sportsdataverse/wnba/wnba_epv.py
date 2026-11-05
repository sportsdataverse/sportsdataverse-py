"""WNBA EPV and EPV added (BB-D24) -- thin shim over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_epv` is league-agnostic: ``league_id="10"`` reads the
``wnba_stats_pbp`` / ``wnba_stats_possessions`` releases and the bundled WNBA booster.
"""

from __future__ import annotations

import functools
from typing import Any

from sportsdataverse.nba.nba_epv import (
    nba_epv_lineup_games,
    nba_epv_player_games,
    nba_epv_plays,
    nba_epv_team_games,
)
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature


def _bind(fn: Any, what: str) -> Any:
    out = functools.partial(fn, league_id="10")
    functools.update_wrapper(out, fn)
    out.__signature__ = wnba_signature(fn)  # type: ignore[attr-defined]
    out.__doc__ = wnba_doc(f"WNBA {what} (league_id='10'). See sportsdataverse.nba.nba_epv.{fn.__name__}.", fn)
    return out


wnba_epv_plays = _bind(nba_epv_plays, "EPV and EPV added per play")
wnba_epv_player_games = _bind(nba_epv_player_games, "EPV added per player and game")
wnba_epv_lineup_games = _bind(nba_epv_lineup_games, "EPV added per five-man unit and game")
wnba_epv_team_games = _bind(nba_epv_team_games, "EPV added per team and game")

__all__ = ["wnba_epv_lineup_games", "wnba_epv_player_games", "wnba_epv_plays", "wnba_epv_team_games"]
