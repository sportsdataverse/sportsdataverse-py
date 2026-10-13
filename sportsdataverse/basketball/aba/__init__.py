"""ABA League and ABA League 2 (Adriatic) wrappers (codegen flat-API family ``aba``).

The data comes from the leagues' own PHP sites (``www.aba-liga.com`` for the ABA League, ``druga.aba-liga.com``
for ABA League 2): keyless, no bot wall. Twelve wrappers over seven routes. Seasons are END years
(2026 = 2025-26) and ``league`` picks the league (``"aba"`` by default, ``"aba2"``). A game is keyed by
(``game_no``, ``season``, ``league_id``): the game number restarts at 1 for every season and league. Team and
player ids are global across seasons and both leagues; join on them, never on names (club names are the club's
current name on every season).

Box scores reach back to 2001-02; the play-by-play, the shot chart and the minute-by-minute score start in
2017-18. Only finished games are served. Several wrappers read the same page (the match page feeds ``aba_game``,
``aba_periods``, ``aba_box_players``, ``aba_box_teams`` and ``aba_pbp``), so fetch it once and parse it as many
ways as needed::

    from sportsdataverse.basketball.aba import aba_game, parse_aba_pbp

    raw = aba_game(game_no=6, season=2026, return_parsed=False)
    plays = parse_aba_pbp(raw)

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.aba import aba as _api
from sportsdataverse.basketball.aba import aba_parsers as _parsers
from sportsdataverse.basketball.aba.aba import *  # noqa: F401,F403
from sportsdataverse.basketball.aba.aba_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
