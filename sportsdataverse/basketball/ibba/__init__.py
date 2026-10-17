"""Israeli Premier League (IBBA) wrappers (codegen flat-API families ``ibba`` and ``ibba_segev``).

Two keyless hosts sit behind the Winner League. ``ibba_*`` reads the Israel Basketball Association's ASMX
web service (basket.co.il): seasons, boards, teams, rosters, coaches, schedules and results, box scores from
2004-05, season stats and leaders. ``ibba_segev_*`` reads Segev Stats, the live-stats vendor behind the game
pages: game header, rosters, box score, period scores and play-by-play with shot coordinates, from 2025-26.
``season`` is the END year (2026 = 2025-26) and the ASMX host's own ``cYear``. The two sources join on
``ibba_games.segev_game_id`` = ``ibba_segev_*`` ``game_id``; IBBA team and player ids are per-season
registrations, Segev ids are stable across seasons::

    from sportsdataverse.basketball.ibba import ibba_games, ibba_segev_pbp

    games = ibba_games(season=2026, board_id=5, board_round=1)
    pbp = ibba_segev_pbp(game_id=int(games["segev_game_id"][0]))

Several wrappers read the same route (``getBoxScore`` feeds ``ibba_segev_player_box``, ``ibba_segev_team_box``
and ``ibba_segev_period_scores``), so fetch it once and parse it as many ways as needed::

    from sportsdataverse.basketball.ibba import (
        ibba_segev_player_box, parse_ibba_segev_period_scores, parse_ibba_segev_team_box,
    )

    raw = ibba_segev_player_box(game_id=50, return_parsed=False)
    teams, periods = parse_ibba_segev_team_box(raw), parse_ibba_segev_period_scores(raw)

Unofficial use of public sources: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.ibba import ibba as _api
from sportsdataverse.basketball.ibba import ibba_parsers as _parsers
from sportsdataverse.basketball.ibba import ibba_segev as _segev_api
from sportsdataverse.basketball.ibba import ibba_segev_parsers as _segev_parsers
from sportsdataverse.basketball.ibba.ibba import *  # noqa: F401,F403
from sportsdataverse.basketball.ibba.ibba_parsers import *  # noqa: F401,F403
from sportsdataverse.basketball.ibba.ibba_segev import *  # noqa: F401,F403
from sportsdataverse.basketball.ibba.ibba_segev_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__, *_segev_api.__all__, *_segev_parsers.__all__]
