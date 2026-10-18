"""Basketligaen (Denmark) wrappers over www.basketligaen.dk/api (codegen flat-API family ``sportality``).

Basketligaen runs on the Sportality platform (the vendor behind ``shl`` and ``ha``); the stats
are ``genius-basketball``, a relay of Genius Sports LiveStats, so the Genius match, team and person
ids in these frames join the ``genius`` family. Seasons are END years (2026 = 2025/26); the runtime
maps them to the site's ssgt uuids. Game-level data (play-by-play with shot coordinates, box
scores, team stats) exists from 2023/24; schedules, standings and season tables from 2002/03.

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.dbl import sportality as _api
from sportsdataverse.basketball.dbl import sportality_parsers as _parsers
from sportsdataverse.basketball.dbl.sportality import *  # noqa: F401,F403
from sportsdataverse.basketball.dbl.sportality_parsers import *  # noqa: F401,F403
from sportsdataverse.basketball.dbl.sportality_runtime import sportality_player_crosswalk, sportality_ssgt_uuid

# Explicit, so the top-level ``from sportsdataverse.basketball.dbl import *`` exports only these.
__all__ = [*_api.__all__, *_parsers.__all__, "sportality_player_crosswalk", "sportality_ssgt_uuid"]
