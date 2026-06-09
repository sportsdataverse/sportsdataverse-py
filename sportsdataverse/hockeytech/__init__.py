"""Shared HockeyTech core (client, league registry, parsers, analytics).

Backs the per-league public modules (``pwhl``/``ahl``/``ohl``/``whl``/``qmjhl``).
Internal: import the per-league wrappers, not these helpers, unless you are
building a new league family.
"""

from __future__ import annotations

from sportsdataverse.hockeytech._leagues import LEAGUES, LeagueConfig, resolve_season_id

__all__ = ["LEAGUES", "LeagueConfig", "resolve_season_id"]
# hockeytech_api is added to this re-export in task A1.2.
