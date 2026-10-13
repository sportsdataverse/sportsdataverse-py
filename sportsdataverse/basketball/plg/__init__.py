"""P. LEAGUE+ (Taiwan's men's league) wrappers (codegen flat-API families ``pleague`` and ``pleague_preciser``).

Two keyless sources behind one league:

* ``pleague`` (``pleagueofficial.com``, 11 wrappers): schedules, standings and player tables from 2020-21, the game
  page, box score and the shot-located play-by-play (from 2022-23). Behind Cloudflare: the runtime needs
  ``curl_cffi``. Seasons are END years (2025 = ``"2024-25"``); a game is keyed by ``gid``, ONE global integer
  sequence over every stage and season. Only finished games are served. Traditional Chinese text is translated in the
  column descriptions.
* ``pleague_preciser`` (``api.preciser.io``, 7 wrappers): the THIRD-PARTY analytics vendor behind the game page. Shots
  with court x / y, the post-game summary, the rotation chart and the injury report, keyed by the Preciser game uuid
  (``uuid`` of :func:`pleague_game`; 2022-23 on). Pass ``gid`` to verify a body against the site's game page and box.

Join a box row to a shot on ``person_id``, never on ``random_id`` (unreliable before 2022-23). The two sources use
different shot x / y frames (see :func:`pleague_pbp` and :func:`pleague_preciser_shots`).

Unofficial use of public sources: the site's ``/user-policy`` reserves its content and the vendor is a third party, so
the data release needs owner sign-off; payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.plg import pleague as _site_api
from sportsdataverse.basketball.plg import pleague_parsers as _site_parsers
from sportsdataverse.basketball.plg import pleague_preciser as _vendor_api
from sportsdataverse.basketball.plg import pleague_preciser_parsers as _vendor_parsers
from sportsdataverse.basketball.plg.pleague import *  # noqa: F401,F403
from sportsdataverse.basketball.plg.pleague_parsers import *  # noqa: F401,F403
from sportsdataverse.basketball.plg.pleague_preciser import *  # noqa: F401,F403
from sportsdataverse.basketball.plg.pleague_preciser_parsers import *  # noqa: F401,F403
from sportsdataverse.basketball.plg.pleague_runtime import season_slug

# Explicit, so the top-level ``from sportsdataverse.basketball.plg import *`` exports only these.
__all__ = [
    *_site_api.__all__,
    *_site_parsers.__all__,
    *_vendor_api.__all__,
    *_vendor_parsers.__all__,
    "season_slug",
]
