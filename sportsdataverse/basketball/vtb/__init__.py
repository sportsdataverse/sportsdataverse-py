"""VTB United League (Russia) wrappers (codegen flat-API families ``vtb_infobasket`` and ``vtb_league``).

Two keyless public JSON backends of vtb-league.com: ``api.vtb-league.com/v2`` (the schedule API, ``vtb_league_*``) and
Infobasket, the Russian basketball federation's stats platform (``org.infobasket.su``, ``vtb_infobasket_*``: calendars,
box scores, play-by-play with shot x/y, standings, brackets, rosters, player and team stats). Seasons are END years
(2026 = 2025-26); Infobasket keys everything by competition id and the runtime maps the END year to it. A body can feed
several frames (``GetOnline`` feeds ``vtb_infobasket_game``, ``_box_score``, ``_team_box_score``, ``_pbp`` and
``_game_staff``), so fetch it once and parse it as many ways as needed::

    from sportsdataverse.basketball.vtb import vtb_infobasket_box_score, parse_vtb_infobasket_pbp

    raw = vtb_infobasket_box_score(game_id=921391, return_parsed=False)
    box, plays = parse_vtb_infobasket_box_score(raw), parse_vtb_infobasket_pbp(raw)

Unofficial use of a public source: payloads are not redistributed as release assets. Owner flag
(``sdv-internal-refs/vtb/README.md``): the league and the platform are run from Russia and several clubs are owned or
sponsored by sanctioned state companies (the KHL precedent). Fetching on your own machine is fine; any published data
release, hosted mirror or redistribution needs owner (sanctions / legal) sign-off first.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.vtb import vtb_infobasket as _infobasket
from sportsdataverse.basketball.vtb import vtb_infobasket_parsers as _infobasket_parsers
from sportsdataverse.basketball.vtb import vtb_league as _league
from sportsdataverse.basketball.vtb import vtb_league_parsers as _league_parsers
from sportsdataverse.basketball.vtb.vtb_infobasket import *  # noqa: F401,F403
from sportsdataverse.basketball.vtb.vtb_infobasket_parsers import *  # noqa: F401,F403
from sportsdataverse.basketball.vtb.vtb_league import *  # noqa: F401,F403
from sportsdataverse.basketball.vtb.vtb_league_parsers import *  # noqa: F401,F403

__all__ = [*_infobasket.__all__, *_infobasket_parsers.__all__, *_league.__all__, *_league_parsers.__all__]
