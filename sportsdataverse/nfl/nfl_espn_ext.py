"""sportsdataverse.nfl.nfl_espn_ext — ESPN endpoint wrappers.

Registers ``espn_nfl_*`` wrappers via :func:`sportsdataverse._common_espn.make_league_module`.
~107 functions cover Site v2, Site v2 alt standings, Web v3 athlete + leaders,
Core v2 (league, seasons, athletes, events, catalog), and football extensions
(QBR by season and by week).

R-package parity note: there is no R "nflfastR ESPN" companion; the closest
analog is cfbfastR's espn_cfb_* family, which this NFL module mirrors at the
sport=football level.
"""

from __future__ import annotations

from sportsdataverse._common_espn import make_league_module

__all__ = make_league_module(
    "football",
    "nfl",
    "nfl",
    globals(),
    include_ncaa=False,
    include_football=True,
)
