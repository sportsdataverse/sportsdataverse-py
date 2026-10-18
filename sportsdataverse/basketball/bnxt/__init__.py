"""BNXT League (Belgium and the Netherlands) wrappers (codegen flat-API family ``sportpress``).

The data comes from the Sportpress CMS behind bnxtleague.com (``bnxt.sportpress.info/api/v1``): the
schedule, standings, rosters and statistics of the BNXT League from 2021-22, box scores for four of the
five seasons since, and play-by-play with court x/y for 2021-22 and 2026-27. ``sportpress_game_slider``
carries the ``genius_id`` that joins the ``genius`` family (organisation ``BNXT``), which has play-by-play
for the other seasons.

Seasons are END years (2026 = 2025-26). Everything but the season routes is keyed by the competition id
(one per season; ``sportpress_phases(season)`` maps a season to it), the competition-team id (new every
season), the roster id or the game id. The wrappers read a public header key from the site bundle at run
time (never printed), contact fields are dropped from every body, and requests are spaced at least 1 s
apart::

    from sportsdataverse.basketball.bnxt import sportpress_game_slider, sportpress_phases, sportpress_schedule

    competition = sportpress_phases(season=2026)["competition_id"][0]
    games = sportpress_schedule(season=2026)

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.bnxt import bnxt_parsers as _parsers
from sportsdataverse.basketball.bnxt import sportpress as _api
from sportsdataverse.basketball.bnxt.bnxt_parsers import *  # noqa: F401,F403
from sportsdataverse.basketball.bnxt.sportpress import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
