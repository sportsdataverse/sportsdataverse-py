"""National League (Switzerland) wrappers over www.nationalleague.ch/api (codegen flat-API family ``nationalleague``).

The app API behind the National League site: schedule, playoff trees, game pages (events,
lineups, box scores, team stats and the only NL shot locations with xG) and season player and
team tables keyed by the SIHF licence number. ``player_id`` joins the ``sihf`` family's
``player_id`` (both ``Int64``), and ``game_id`` is the same SIHF game id on both.

robots.txt disallows ``/api``. The owner approved building on it on 2026-10-12 for polite,
incremental use (at least 1 s between requests, no bulk backfill without asking); see
``nationalleague_runtime``. Unofficial use of a public source: payloads are not redistributed as
release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.hockey.nl import nationalleague as _api
from sportsdataverse.hockey.nl import nationalleague_parsers as _parsers
from sportsdataverse.hockey.nl.nationalleague import *  # noqa: F401,F403
from sportsdataverse.hockey.nl.nationalleague_parsers import *  # noqa: F403

__all__ = [*_api.__all__, *_parsers.__all__]
