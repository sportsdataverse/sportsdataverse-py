"""TPBL (Taipei Professional Basketball League, Taiwan) wrappers over api.tpbl.basketball (codegen flat-API family ``tpbl``).

One host, keyless: the Laravel JSON API the tpbl.basketball site calls (11 routes, 16 wrappers: seasons, divisions,
teams, the season game list, standings with head-to-head records, one game and its referees, the box score as four
frames, a team roster, two shot lists and a player's game log). There is no play-by-play route. Season-keyed wrappers
take ``season``, the END year (2026 = 2025-26), which ``tpbl_runtime`` maps to the API's own season id; every other
wrapper takes the API's ids (``game_id``, ``division_id``, ``team_id``, ``player_id``). Game wrappers serve FINISHED
games only. Several wrappers read the same route (the box score feeds ``tpbl_box_players``, ``tpbl_box_player_periods``,
``tpbl_box_teams`` and ``tpbl_box_team_periods``), so fetch it once and parse it as many ways as needed::

    from sportsdataverse.basketball.tpbl import parse_tpbl_box_teams, tpbl_box_players

    raw = tpbl_box_players(game_id=6, return_parsed=False)
    players, teams = parse_tpbl_box_players(raw), parse_tpbl_box_teams(raw)

The box score keys its players by the GoHoops roster id (``gohoops_roster_id``): join it to ``tpbl_roster`` on that
column, never on the TPBL ``player_id`` alone or on names. Unofficial use of a public source: payloads are not
redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.tpbl import tpbl as _api
from sportsdataverse.basketball.tpbl import tpbl_parsers as _parsers
from sportsdataverse.basketball.tpbl.tpbl import *  # noqa: F401,F403
from sportsdataverse.basketball.tpbl.tpbl_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
