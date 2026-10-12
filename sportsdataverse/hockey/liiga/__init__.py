"""Liiga (Finland) wrappers over liiga.fi/api/v2 (codegen flat-API family ``liiga``).

Seasons are END years (2026 = 2025-26) and game ids repeat across seasons, so every game
wrapper takes ``season`` and ``game_id``. Several wrappers read the same route (the game page
feeds ``liiga_game``, ``_game_goals``, ``_game_penalties``, ``_game_shootout`` and
``_game_lineups``; the box score feeds ``_game_stats`` and ``_game_team_stats``), so fetch it
once and parse it as many ways as needed::

    from sportsdataverse.hockey.liiga import liiga_game, parse_liiga_game_lineups, parse_liiga_game_penalties

    raw = liiga_game(season=2026, game_id=1, return_parsed=False)
    lineups, penalties = parse_liiga_game_lineups(raw), parse_liiga_game_penalties(raw)

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES). The generated module's import is added
# by the integration codegen run; the parser module is re-exported here.
from sportsdataverse.hockey.liiga.liiga_parsers import *  # noqa: F401,F403
