"""sportsdataverse.wnba.wnba_espn_ext — ESPN endpoint wrappers ported from wehoop.

Registers ``espn_wnba_*`` wrappers via :func:`sportsdataverse._common_espn.make_league_module`.
~105 functions cover Site v2, Site v2 alt standings, Web v3 athlete + leaders,
and Core v2 (league, seasons, athletes, events, catalog).
"""

from __future__ import annotations

from sportsdataverse._common_espn import make_league_module

__all__ = make_league_module("basketball", "wnba", "wnba", globals(), include_ncaa=False, include_football=False)
