"""BBL and DBBL (Germany) wrappers over the SCB API (codegen flat-API family ``scb``).

One API on two hosts: ``api.basketball-bundesliga.de`` (BBL, men) and ``api-1.dbbl.scb.world`` (DBBL, women).
Every wrapper but the BBL-only ``scb_referees``, ``scb_transfers``, ``scb_transfers_teams`` and
``scb_team_rosters`` takes ``league="bbl"`` (default) or ``"dbbl"``. Seasons are END years (2026 = 2025-26);
the API's own ``seasonId`` is the START year and the runtime maps it. Game ids overlap between the two hosts,
so a game is ``(league, game_id)``. Several wrappers read the same route (the box score feeds
``scb_game_players``, ``scb_game_teams`` and ``scb_game_actions``), so fetch it once and parse it as many ways
as needed::

    from sportsdataverse.basketball.scb import parse_scb_game_actions, parse_scb_game_players, scb_game_players

    raw = scb_game_players(game_id="2006920", league="bbl", return_parsed=False)
    players, actions = parse_scb_game_players(raw), parse_scb_game_actions(raw)

Unofficial use of a public source: payloads are not redistributed as release assets. The BBL Impressum forbids
storing its content without written permission; see the ``Notes`` of any wrapper.
"""

from __future__ import annotations

# Nested from its first release (generate._BASKETBALL_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.scb import scb as _api
from sportsdataverse.basketball.scb import scb_parsers as _parsers
from sportsdataverse.basketball.scb.scb import *  # noqa: F401,F403
from sportsdataverse.basketball.scb.scb_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
