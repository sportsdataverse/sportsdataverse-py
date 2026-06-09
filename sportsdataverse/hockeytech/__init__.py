"""Shared HockeyTech core (client, league registry, parsers, analytics).

Backs the per-league public modules (``pwhl``/``ahl``/``ohl``/``whl``/``qmjhl``).
Internal: import the per-league wrappers, not these helpers, unless you are
building a new league family.
"""

from __future__ import annotations

# Re-exports (hockeytech_api, LEAGUES, LeagueConfig, resolve_season_id) are added
# in tasks A1.1 (_leagues.py) and A1.2 (_client.py).
__all__: list = []
