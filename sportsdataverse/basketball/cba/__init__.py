"""CBA (Chinese Basketball Association) wrappers over data-server.cbaleague.com (codegen flat-API family ``cba``).

Twenty-seven wrappers over the men's CBA data API (12 GET routes, 15 POST routes the wrappers send as JSON): the
reference lists, the schedule, per-game header, player box, advanced box, period scores, rosters, best players,
play-by-play, and the team-season and player-season tables. Every response body is AES-128-ECB encrypted with a public
key; ``cba_runtime`` decrypts it with a stdlib implementation (``cba_aes``) and refuses anything that does not
decrypt. The host blocks datacenter IPs: set ``CBA_PROXY`` to a residential proxy URL (never logged). Game wrappers take
``schedule_id`` and serve FINISHED games only; ``season`` arguments are the END year (2026 = 2025-26) and the runtime
sends the API's START year. Text fields are Chinese; the column descriptions carry English glosses::

    from sportsdataverse.basketball.cba import cba_player_box, parse_cba_player_box

    raw = cba_player_box(schedule_id=100105799, return_parsed=False)
    box = parse_cba_player_box(raw)

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.cba import cba as _api
from sportsdataverse.basketball.cba import cba_parsers as _parsers
from sportsdataverse.basketball.cba.cba import *  # noqa: F401,F403
from sportsdataverse.basketball.cba.cba_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
