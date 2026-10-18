"""Ligue Magnus (France) wrappers over liguemagnus.com (codegen flat-API family ``magnus``).

Unofficial use of a public source: payloads are not redistributed as release assets.

The season is keyed by its END year (2027 = 2026-27); coverage starts in 2020-21. The four
game wrappers (``magnus_game``, ``magnus_pbp``, ``magnus_lineups``, ``magnus_shootout``) all
read the same ``/rencontre/{id}/`` page, so fetch it once and parse it as many ways as needed::

    from sportsdataverse.hockey.magnus import magnus_pbp, parse_magnus_game, parse_magnus_lineups

    sheet = magnus_pbp(game_id=77779, return_parsed=False)
    header, lineups = parse_magnus_game(sheet), parse_magnus_lineups(sheet)
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.hockey.magnus import magnus as _api
from sportsdataverse.hockey.magnus.magnus import *  # noqa: F401,F403
from sportsdataverse.hockey.magnus import magnus_parsers as _parsers
from sportsdataverse.hockey.magnus.magnus_parsers import *  # noqa: F401,F403

# Explicit, so ``from sportsdataverse.hockey.magnus import *`` (the top-level package does it)
# exports the parsers and never the submodule objects.
__all__ = [*_api.__all__, *_parsers.__all__]
