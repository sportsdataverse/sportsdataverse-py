"""Fox Sports raw API (``api.foxsports.com``) wrappers.

Cross-sport package (like :mod:`sportsdataverse.cbs`): every path is
``{sport}``-parameterized. Public names are ``fox_api_<endpoint>``; the
league-scoped ``fox_<league>_*`` wrappers live in each league package.
"""

from __future__ import annotations

from sportsdataverse.fox.fox_api import *  # noqa: F401,F403
from sportsdataverse.fox.fox_api_parsers import (  # noqa: F401
    parse_fox_api,
    parse_fox_api_events,
    parse_fox_api_header,
    parse_fox_api_nav,
    parse_fox_api_polls,
    parse_fox_api_roster,
    parse_fox_api_scorechip,
    parse_fox_api_search,
    parse_fox_api_standings,
    parse_fox_api_trending,
)
