"""sportsdataverse.wbb.wbb_espn_ext — ESPN endpoint wrappers ported from wehoop.

Registers ``espn_wbb_*`` wrappers via :func:`sportsdataverse._common_espn.make_league_module`.
~108 functions cover Site v2, Site v2 alt standings, Web v3 athlete + leaders,
Core v2 (league, seasons, athletes, events, catalog), plus NCAA-only
extensions (rankings, recruits, weekly rankings).
"""

from __future__ import annotations

from sportsdataverse._common_espn import make_league_module
from sportsdataverse._common_ncaa import register_ncaa_bracketology

__all__ = make_league_module(
    "basketball",
    "womens-college-basketball",
    "wbb",
    globals(),
    include_ncaa=True,
    include_football=False,
)
register_ncaa_bracketology("wbb", globals())
__all__ = list(__all__) + ["espn_wbb_bracketology"]
