"""VHL wrappers over vhlru.ru pages (codegen flat-API family ``vhl``).

The VHL (Supreme Hockey League, Russia's second tier) serves no JSON: every wrapper fetches a
server-rendered page, the runtime refuses any page that is not the one asked for, and the
parsers turn it into frames. Seasons are END years (2026 = 2025/26) plus a ``stage``
(``regular`` / ``playoff``); the runtime maps them to the site's tournament id (``vhl_tn``).
One report page feeds ``vhl_game_info``, ``vhl_goals``, ``vhl_penalties``,
``vhl_player_box`` and ``vhl_goalie_box``, so fetch it once and parse it as many ways as
needed::

    from sportsdataverse.hockey.vhl import vhl_game_info, parse_vhl_goals, parse_vhl_penalties

    raw = vhl_game_info(season=2026, game_id=899223, return_parsed=False)
    goals, penalties = parse_vhl_goals(raw), parse_vhl_penalties(raw)

**Owner flag (sanctions).** These are Russian league operators, and several clubs are owned
or sponsored by sanctioned state companies. Wrappers that fetch on the user's machine are
fine; a published data release or a hosted mirror of this data needs the owner's sign-off
(sanctions / legal review) first.

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.hockey.vhl import vhl as _api
from sportsdataverse.hockey.vhl.vhl import *  # noqa: F401,F403
from sportsdataverse.hockey.vhl import vhl_parsers as _parsers
from sportsdataverse.hockey.vhl.vhl_parsers import *  # noqa: F401,F403
from sportsdataverse.hockey.vhl.vhl_runtime import vhl_tn

# Explicit, so the top-level ``from sportsdataverse.hockey.vhl import *`` exports only these.
__all__ = [*_api.__all__, *_parsers.__all__, "vhl_tn"]
