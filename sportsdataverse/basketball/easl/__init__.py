"""East Asia Super League wrappers (codegen flat-API family ``easl``), read through an UNOFFICIAL third-party proxy.

WARNING: the data comes from ``easl.joshricha.com/api/v1``, an Express server run by an individual that mirrors
Sportradar DataCore (organisation ``b1e0z``) into SQLite. It is not EASL's and not Sportradar's, it can change or vanish
without notice, and a first-party DataCore feed needs credentials this package does not have.

The league has four editions here (2023-24 to 2026-27). Seasons are END years (2026 = EASL 25-26); ``season=None`` is
the ACTIVE season. The wrappers cover the fixture list, finished games (box score, play-by-play, team totals and
totals by period), standings, leaders, teams and players. Game routes serve finished games only, an active-season
list is refused when the mirror has stalled, contact fields are never carried, and requests are spaced at least 1 s
apart::

    from sportsdataverse.basketball.easl import easl_pbp, easl_player_box, easl_schedule

    games = easl_schedule(season=2026)
    fixture = games.filter(games["status"] == "CONFIRMED")["fixture_id"][0]
    box = easl_player_box(fixture_id=fixture)

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.easl import easl as _api
from sportsdataverse.basketball.easl import easl_parsers as _parsers
from sportsdataverse.basketball.easl.easl import *  # noqa: F401,F403
from sportsdataverse.basketball.easl.easl_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
