"""Shared HockeyTech core (client, league registry, parsers, analytics).

Backs the per-league public modules (``pwhl``/``ahl``/``ohl``/``whl``/``qmjhl``).
Internal: import the per-league wrappers, not these helpers, unless you are
building a new league family.
"""

from __future__ import annotations

from sportsdataverse.hockeytech._client import hockeytech_api
from sportsdataverse.hockeytech._leagues import LEAGUES, LeagueConfig, resolve_season_id

__all__ = ["hockeytech_api", "LEAGUES", "LeagueConfig", "resolve_season_id"]
