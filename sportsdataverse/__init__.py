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

import logging as _logging

# Library logging convention (PEP 282): attach a NullHandler at the package
# root so merely importing sportsdataverse never emits log output unless the
# host application configures logging. Modules obtain their logger via
# ``logging.getLogger(__name__)`` and use it (notably) to make best-effort /
# graceful-degradation paths observable instead of silently swallowing errors.
_logging.getLogger("sportsdataverse").addHandler(_logging.NullHandler())

# isort: off
# IMPORT ORDER IS LOAD-BEARING — do not let isort/ruff re-sort this block.
# League wildcards MUST run before the top-level QoL imports because:
#   * sportsdataverse.nfl re-exports its submodule ``cache`` into the parent
#     namespace via ``from . import *``, shadowing the top-level
#     ``sportsdataverse.cache`` module we add below.
#   * Likewise ``find`` / ``discover`` could collide with league submodules.
# After the wildcards run we re-bind the package attributes to point at our
# top-level modules, then star-import their public names. The trailing
# ``import ... as ...`` aliases ensure ``sportsdataverse.cache`` /
# ``.find`` / ``.discover`` resolve to the QoL modules in dotted access.
from sportsdataverse.cfb import *
from sportsdataverse.mbb import *
from sportsdataverse.mlb import *
from sportsdataverse.nba import *
from sportsdataverse.nfl import *
from sportsdataverse.nhl import *
from sportsdataverse.pwhl import *
from sportsdataverse.wbb import *
from sportsdataverse.wnba import *
from sportsdataverse.ahl import *  # noqa: F401,F403,E402
from sportsdataverse.ohl import *  # noqa: F401,F403,E402
from sportsdataverse.qmjhl import *  # noqa: F401,F403,E402
from sportsdataverse.whl import *  # noqa: F401,F403,E402
from sportsdataverse.odds import *  # noqa: F401,F403,E402

# Top-level QoL helpers (0.0.51+).
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
from sportsdataverse.discover import function_count, list_functions
from sportsdataverse.find import (
    clear_team_cache,
    find_athlete,
    find_event,
    find_team,
)

# Re-expose the QoL modules as package attributes AFTER all wildcards so
# ``sportsdataverse.cache`` / ``.find`` / ``.discover`` resolve to OUR
# top-level modules rather than any same-named league submodule. The dotted
# names (e.g. ``sportsdataverse.nfl.cache``) remain reachable.
import sportsdataverse.cache as cache  # noqa: F401, E402
import sportsdataverse.discover as discover  # noqa: F401, E402
import sportsdataverse.find as find  # noqa: F401, E402
# isort: on
