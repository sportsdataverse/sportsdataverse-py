"""sportsdataverse-py -- tidy sports data loaders for football, basketball, and hockey.

Python companion to the SportsDataverse R package family, providing
clean access to play-by-play, box score, schedule, roster, and
reference data across CFB, MBB / WBB, NBA / WNBA, NFL, and NHL. Each
sport gets its own submodule with consistent ``espn_<sport>_*`` and
``load_<sport>_*`` entry points so loaders look the same across leagues.

Example:
    Pull a representative slice from each sport submodule::

        import sportsdataverse as sdv

        cfb_pbp = sdv.cfb.espn_cfb_pbp(game_id=401628334)
        nba_schedule = sdv.nba.espn_nba_schedule(season=2024)
        wnba_pbp = sdv.wnba.espn_wnba_pbp(game_id=401620238)
        nfl_pbp = sdv.nfl.load_nfl_pbp(seasons=[2024])

    The ESPN-fronted wrappers (``espn_*``) hit the live ESPN API on every
    call; the bulk loaders (``load_*``) read pre-built parquet from the
    sportsdataverse-data and nflverse-data release buckets.

See Also:
    * `SportsDataverse (R)`_ -- the companion R package family
      (``cfbfastR``, ``hoopR``, ``wehoop``, ``fastRhockey``, ``hockeyR``,
      ``baseballr``, ``recruitR``, ...) that this package mirrors.
    * `nflverse`_ -- upstream parquet release source for the NFL
      submodule.
    * `nflreadpy`_ -- alternative Python NFL loader; the
      ``sportsdataverse.nfl`` API is intentionally compatible-shaped so
      callers can swap loaders with minimal churn.

.. _SportsDataverse (R): https://www.sportsdataverse.org
.. _nflverse: https://nflverse.nflverse.com
.. _nflreadpy: https://github.com/nflverse/nflreadpy
"""

from __future__ import annotations

# Re-expose the new top-level modules so ``sportsdataverse.cache`` and
# ``sportsdataverse.find`` are introspectable even after the NFL wildcard
# import would have shadowed them.
import sportsdataverse.cache as cache  # noqa: E402  (ordering needed for shadow)
import sportsdataverse.discover as discover  # noqa: E402
import sportsdataverse.find as find  # noqa: E402

# Top-level QoL helpers (0.0.51+). Imported AFTER the league wildcards so
# the bare name ``sportsdataverse.cache`` resolves to this module, not to
# ``sportsdataverse.nfl.cache`` (which exposes only ``clear_cache`` and
# stays available via the dotted path). Likewise ``sportsdataverse.find``
# / ``sportsdataverse.discover`` shadow any same-named league submodule.
#   * find_team / find_athlete / find_event — name-to-ID resolvers
#   * list_functions / function_count       — searchable function index
#   * set_cache_mode + clear_cache + cache_stats — tiered TTL response cache
from sportsdataverse.cache import (
    cache_stats,
    clear_cache,
    get_cache_mode,
    set_cache_mode,
    set_default_ttl,
)
from sportsdataverse.cfb import *
from sportsdataverse.discover import function_count, list_functions
from sportsdataverse.find import clear_team_cache, find_athlete, find_event, find_team
from sportsdataverse.mbb import *
from sportsdataverse.mlb import *
from sportsdataverse.nba import *
from sportsdataverse.nfl import *
from sportsdataverse.nhl import *
from sportsdataverse.wbb import *
from sportsdataverse.wnba import *
