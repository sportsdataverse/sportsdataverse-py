"""sportsdataverse.cfb.cfb_espn_ext — ESPN endpoint wrappers ported from cfbfastR.

Registers ``espn_cfb_*`` wrappers via :func:`sportsdataverse._common_espn.make_league_module`.
~110 functions cover Site v2, Site v2 alt standings, Web v3 athlete + leaders,
Core v2 (league, seasons, athletes, events, catalog), plus NCAA extensions
(rankings, recruits, weekly rankings) and football extensions (QBR by season
and by week).
"""

from __future__ import annotations

from sportsdataverse._common_espn import make_league_module

__all__ = make_league_module(
    "football",
    "college-football",
    "cfb",
    globals(),
    include_ncaa=True,
    include_football=True,
)
