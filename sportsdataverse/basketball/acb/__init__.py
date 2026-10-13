"""ACB, Liga Endesa (Spain) wrappers: Liga Endesa, Copa del Rey, Supercopa Endesa, Minicopa Endesa (codegen flat-API family ``acb``).

One JSON host (``api2.acb.com``, the API behind acb.com and live.acb.com) feeds 45 wrappers over 33 routes: match
header, box score per period, play-by-play and shots (2016-17 onward), lead tracker, lineups, leaders, team
comparison, advanced statistics, playoff series and bracket, round lists, standings, season calendar, league shot
zones, teams, club profile, rosters, club and player statistics, player bio, career and game log, and search.
Match wrappers take ``match_id``; season wrappers take ``season``, the END year (2027 = 2026-27), and the club and
player wrappers take ``club_id`` and ``player_id`` (the ACB licence number). Several wrappers read the same route
(the box score feeds ``acb_player_box``, ``acb_team_box`` and ``acb_box_info``), so fetch it once and parse it as
many ways as needed::

    from sportsdataverse.basketball.acb import acb_player_box, parse_acb_box_info, parse_acb_team_box

    raw = acb_player_box(match_id=105401, return_parsed=False)
    teams, info = parse_acb_team_box(raw), parse_acb_box_info(raw)

``acb_codes`` decodes the ``playType`` / ``playTag`` / ``zone`` codes and the Spanish country names.

Unofficial use of a public source: payloads are not redistributed as release assets, and the league's legal notice
claims database rights (see each wrapper's Notes).
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.acb import acb as _api
from sportsdataverse.basketball.acb import acb_parsers as _parsers
from sportsdataverse.basketball.acb.acb import *  # noqa: F401,F403
from sportsdataverse.basketball.acb.acb_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
