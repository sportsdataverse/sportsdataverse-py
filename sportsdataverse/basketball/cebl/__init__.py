"""CEBL (Canada) wrappers (codegen flat-API family ``cebl``).

The data comes from the Canadian Elite Basketball League's own stats API (``api.data.cebl.ca``, the backend of the
CEBL Stats Hub): seasons, standings, franchises, rosters, team and player statistic tables (totals or per game),
leaders, player profiles, season and career rows, game logs and the schedule with results, 2019 on. **CEBL's own
API, owner-approved gray area.** There is no box-score, play-by-play or shot route: ``cebl_games`` and
``cebl_player_gamelog`` carry ``genius_match_id``, the Genius LiveStats match id (organisation ``CEBL``) that joins
the ``genius`` family, where they live.

The season is the calendar year (May to August) = the END year, so there is no mapping. The wrappers read a public
header key from the Stats Hub bundle at run time (never printed), and requests are spaced at least 1 s apart::

    from sportsdataverse.basketball.cebl import cebl_games, cebl_standings

    standings = cebl_standings(season=2025)
    games = cebl_games(season=2025)

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

from sportsdataverse.basketball.cebl import cebl as _api
from sportsdataverse.basketball.cebl import cebl_parsers as _parsers
from sportsdataverse.basketball.cebl.cebl import *  # noqa: F401,F403
from sportsdataverse.basketball.cebl.cebl_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
