"""HockeyAllsvenskan (Sweden) wrappers over hockeyallsvenskan.se site API (codegen flat-API family ``ha``).

Seasons are END years (2026 = 2025/26). Stats and standings run from 2005/06; game records and
play-by-play from 2025/26. The schedule (``ha_schedule``) and the leaderboard (``ha_leaders``)
cover the season in progress only. A game's play-by-play request comes from its schedule row::

    from sportsdataverse.hockey.ha import ha_pbp, ha_schedule

    g = ha_schedule().row(0, named=True)
    pbp = ha_pbp(
        statnet_game_number=g["statnet_game_number"],
        home_statnet_id=g["home_statnet_id"],
        away_statnet_id=g["away_statnet_id"],
        scheduled_date_time=g["scheduled_date_time"],
    )

Teams have three key spaces (StatNet id, display code, CMS id) and every frame keeps them in
separately named columns; see :mod:`sportsdataverse.hockey.ha.ha_parsers`.

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.hockey.ha import ha as _api
from sportsdataverse.hockey.ha import ha_parsers as _parsers
from sportsdataverse.hockey.ha.ha import *  # noqa: F401,F403
from sportsdataverse.hockey.ha.ha_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
