"""FIBA (national-team events and club competitions) wrappers (codegen flat-API families ``fiba_gdap`` and ``fiba_gamepage``).

Two sources: the GDAP JSON API (schedules, results, rosters, standings, awards and per-competition statistics for every
FIBA event back to 1950 and for the club competitions: Basketball Champions League, EuroLeague Women...) and the
fiba.basketball game pages (box score, play-by-play and shots, which only the pages serve). The competition is the
``comp`` argument, never a family of its own::

    from sportsdataverse.basketball.fiba_hub import fiba_gdap_schedule, fiba_gamepage_pbp

    games = fiba_gdap_schedule(comp=208182)                 # World Cup 2023, one row per game
    pbp = fiba_gamepage_pbp(game_id=100720, comp=208182)    # the final's play-by-play with shot x/y

Seasons are FIBA's own key, which equals the sdv key: the END year for club competitions, the event year for
national-team events (World Cup 2023 = 2023). Unofficial use of a public source; see the ``notes`` of any wrapper
for FIBA's terms, which restrict reuse. Payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.fiba_hub import fiba_gamepage as _gamepage
from sportsdataverse.basketball.fiba_hub import fiba_gamepage_parsers as _gamepage_parsers
from sportsdataverse.basketball.fiba_hub import fiba_gdap as _gdap
from sportsdataverse.basketball.fiba_hub import fiba_gdap_parsers as _gdap_parsers
from sportsdataverse.basketball.fiba_hub.fiba_gamepage import *  # noqa: F401,F403
from sportsdataverse.basketball.fiba_hub.fiba_gamepage_parsers import *  # noqa: F401,F403
from sportsdataverse.basketball.fiba_hub.fiba_gdap import *  # noqa: F401,F403
from sportsdataverse.basketball.fiba_hub.fiba_gdap_parsers import *  # noqa: F401,F403

# The per-competition families (fiba_wc_*, fiba_bcl_*, ...), minted from the competition registry.
from sportsdataverse.basketball.fiba_hub import fiba_competitions_runtime as _competitions
from sportsdataverse.basketball.fiba_hub.fiba_competitions_runtime import *  # noqa: F401,F403

# Explicit, so ``from sportsdataverse.basketball.fiba_hub import *`` (the top-level package does it)
# exports the wrappers and parsers and never the submodule objects.
__all__ = [
    *_gdap.__all__,
    *_gdap_parsers.__all__,
    *_gamepage.__all__,
    *_gamepage_parsers.__all__,
    *_competitions.__all__,
]
