"""sportsdataverse.mlb.mlb_espn_ext — ESPN endpoint wrappers (greenfield).

Registers ``espn_mlb_*`` wrappers via :func:`sportsdataverse._common_espn.make_league_module`.
~105 functions cover Site v2, Site v2 alt standings, Web v3 athlete + leaders,
and Core v2 (league, seasons, athletes, events, catalog).

This is the ESPN side of MLB coverage; the parallel
:mod:`sportsdataverse.mlb.mlb_api` module wraps ``statsapi.mlb.com`` and
:mod:`sportsdataverse.mlb.mlb_statcast` wraps ``baseballsavant.mlb.com``.
"""

from __future__ import annotations

from sportsdataverse._common_espn import make_league_module

__all__ = make_league_module(
    "baseball", "mlb", "mlb", globals(),
    include_ncaa=False, include_football=False, include_mlb=True,
)
