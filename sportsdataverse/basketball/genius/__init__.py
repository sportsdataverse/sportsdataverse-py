"""Genius Sports LiveStats (20+ domestic competitions) wrappers (codegen flat-API family ``genius``).

One platform runs the official stats of the Greek, Polish, Croatian, Czech and Romanian leagues, BNXT, CEBL, BSN,
LNBP, the NZ NBL, the Basketball Africa League, the Israeli, French federation, British and Uruguayan leagues and
more. A competition is an ORG code plus a competition id, so the wrappers take ``org`` and ``comp_id`` (the org
and the competition are parameters, not one family each) and ``match_id`` for a game.

Find a game, then read it. ``genius_competitions`` lists an org's competitions with the sdv END-year ``season``;
``genius_schedule`` lists a competition's matches; one ``data.json`` fetch feeds ``genius_game``,
``genius_team_box``, ``genius_player_box``, ``genius_pbp`` and ``genius_shots``, so fetch it once and parse it as
many ways as needed::

    from sportsdataverse.basketball.genius import genius_game, parse_genius_pbp, parse_genius_shots

    raw = genius_game(match_id=2902690, return_parsed=False)
    pbp, shots = parse_genius_pbp(raw), parse_genius_shots(raw)

CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: set the optional ``GENIUS_PROXY`` environment
variable to an http(s) proxy URL (never logged or printed) when running from a server.

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._BASKETBALL_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.genius import genius as _api
from sportsdataverse.basketball.genius import genius_parsers as _parsers
from sportsdataverse.basketball.genius.genius import *  # noqa: F401,F403
from sportsdataverse.basketball.genius.genius_parsers import *  # noqa: F401,F403

# The per-competition families (wbbl_*, slb_*, ...), minted from the competition registry.
from sportsdataverse.basketball.genius import genius_competitions_runtime as _competitions
from sportsdataverse.basketball.genius.genius_competitions_runtime import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__, *_competitions.__all__]
