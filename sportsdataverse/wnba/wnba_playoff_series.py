"""WNBA playoff series (BB-D43) -- thin shims over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_playoff_series` is league-agnostic: ``league_id="10"`` reads the
``wnba`` ESPN schedule and box score releases and the WNBA Finals clinch rule.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba import nba_playoff_series as _m
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature


def _bind(core, summary: str):  # type: ignore[no-untyped-def]
    fn = functools.partial(core, league_id="10")
    functools.update_wrapper(fn, core)
    fn.__signature__ = wnba_signature(core)  # type: ignore[attr-defined]
    fn.__doc__ = wnba_doc(
        f"{summary} (league_id='10'). See sportsdataverse.nba.nba_playoff_series.{core.__name__}.", core
    )
    return fn


wnba_playoff_series = _bind(_m.nba_playoff_series, "WNBA postseason games keyed to their series")
wnba_playoff_series_teams = _bind(_m.nba_playoff_series_teams, "WNBA team totals per playoff series")
wnba_playoff_series_players = _bind(
    _m.nba_playoff_series_players, "WNBA player totals per playoff series against the regular season"
)

__all__ = ["wnba_playoff_series", "wnba_playoff_series_players", "wnba_playoff_series_teams"]
