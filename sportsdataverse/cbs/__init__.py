"""CBS Sports NAPI (``api.cbssports.com/napi``) wrappers.

A cross-sport package rather than a league one: every NAPI path is
``{leagueId}``/``{teamId}``/``{playerId}``-parameterized and the API serves 17
leagues with no sport bias, so it is homed here alongside
:mod:`sportsdataverse.odds` instead of under any one league.
"""

from __future__ import annotations

from sportsdataverse.cbs.cbs_napi import *  # noqa: F401,F403
from sportsdataverse.cbs.cbs_napi_parsers import (  # noqa: F401
    parse_cbs_napi,
    parse_cbs_napi_standings,
)
