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
from sportsdataverse.hockey.ahl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.ohl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.qmjhl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.whl import *  # noqa: F401,F403,E402
from sportsdataverse.odds import *  # noqa: F401,F403,E402

# --- ESPN additional leagues — sport param-families stay top-level; minor/alias
#     leagues are nested under sport-group packages (0.0.65+). ---
from sportsdataverse.soccer import *  # noqa: F401,F403,E402
from sportsdataverse.cbs import *  # noqa: F401,F403,E402
from sportsdataverse.yahoo import *  # noqa: F401,F403,E402
from sportsdataverse.pff import *  # noqa: F401,F403,E402
from sportsdataverse.cricket import *  # noqa: F401,F403,E402
from sportsdataverse.football.ufl import *  # noqa: F401,F403,E402
from sportsdataverse.football.xfl import *  # noqa: F401,F403,E402
from sportsdataverse.football.cfl import *  # noqa: F401,F403,E402

# Soccer sub-leagues, star-imported the same way the football ones are directly
# above. `from sportsdataverse.soccer import *` only re-exports the generic
# soccer_espn_ext wrappers plus the sub-league packages as attributes, so without
# these 12 lines `espn_mls_scoreboard` and its 1,343 siblings were reachable only
# by deep import while every other ESPN league family sat at the top level.
from sportsdataverse.soccer.bundesliga import *  # noqa: F401,F403,E402
from sportsdataverse.soccer.epl import *  # noqa: F401,F403,E402
from sportsdataverse.soccer.laliga import *  # noqa: F401,F403,E402
from sportsdataverse.soccer.ligamx import *  # noqa: F401,F403,E402
from sportsdataverse.soccer.ligue1 import *  # noqa: F401,F403,E402
from sportsdataverse.soccer.mls import *  # noqa: F401,F403,E402
from sportsdataverse.soccer.nwsl import *  # noqa: F401,F403,E402
from sportsdataverse.soccer.seriea import *  # noqa: F401,F403,E402
from sportsdataverse.soccer.ucl import *  # noqa: F401,F403,E402
from sportsdataverse.soccer.uel import *  # noqa: F401,F403,E402
from sportsdataverse.soccer.wc import *  # noqa: F401,F403,E402
from sportsdataverse.soccer.wwc import *  # noqa: F401,F403,E402
from sportsdataverse.baseball.college_baseball import *  # noqa: F401,F403,E402
from sportsdataverse.baseball.college_softball import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.mch import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.wch import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.echl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.sphl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.chl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.ushl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.bchl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.ajhl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.sjhl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.ojhl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.cchl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.gojhl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.mhl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.nojhl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.vijhl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.kijhl import *  # noqa: F401,F403,E402
from sportsdataverse.hockey.mjhl import *  # noqa: F401,F403,E402

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

# ---------------------------------------------------------------------------
# Back-compat: leagues moved under sport-group packages (0.0.65+). The old
# top-level names (``sportsdataverse.epl``, ``.ahl``, ``.ufl``, …) still
# resolve — to their nested home — with a DeprecationWarning. ``_MOVED`` maps
# the legacy leaf -> nested dotted suffix and is the single source of truth;
# tests/test_namespace_backcompat.py asserts it matches the grouped leagues.
# ---------------------------------------------------------------------------
import importlib as _importlib  # noqa: E402
import importlib.abc as _ilabc  # noqa: E402
import importlib.util as _ilutil  # noqa: E402
import sys as _sys  # noqa: E402
import warnings as _warnings  # noqa: E402

_MOVED = {
    "epl": "soccer.epl",
    "laliga": "soccer.laliga",
    "bundesliga": "soccer.bundesliga",
    "seriea": "soccer.seriea",
    "ligue1": "soccer.ligue1",
    "mls": "soccer.mls",
    "ligamx": "soccer.ligamx",
    "ucl": "soccer.ucl",
    "uel": "soccer.uel",
    "nwsl": "soccer.nwsl",
    "wwc": "soccer.wwc",
    "wc": "soccer.wc",
    "mch": "hockey.mch",
    "wch": "hockey.wch",
    "ahl": "hockey.ahl",
    "ohl": "hockey.ohl",
    "qmjhl": "hockey.qmjhl",
    "whl": "hockey.whl",
    "echl": "hockey.echl",
    "sphl": "hockey.sphl",
    "chl": "hockey.chl",
    "ushl": "hockey.ushl",
    "bchl": "hockey.bchl",
    "ajhl": "hockey.ajhl",
    "sjhl": "hockey.sjhl",
    "ojhl": "hockey.ojhl",
    "cchl": "hockey.cchl",
    "gojhl": "hockey.gojhl",
    "mhl": "hockey.mhl",
    "nojhl": "hockey.nojhl",
    "vijhl": "hockey.vijhl",
    "kijhl": "hockey.kijhl",
    "mjhl": "hockey.mjhl",
    "ufl": "football.ufl",
    "xfl": "football.xfl",
    "cfl": "football.cfl",
    "college_baseball": "baseball.college_baseball",
    "college_softball": "baseball.college_softball",
}


def __getattr__(name):  # PEP 562 module-level __getattr__
    target = _MOVED.get(name)
    if target is None:
        raise AttributeError(f"module 'sportsdataverse' has no attribute {name!r}")
    _warnings.warn(
        f"sportsdataverse.{name} moved to sportsdataverse.{target}; "
        f"import it from there — this top-level alias is deprecated.",
        DeprecationWarning,
        stacklevel=2,
    )
    mod = _importlib.import_module(f"sportsdataverse.{target}")
    globals()[name] = mod  # cache; __getattr__ won't fire for this name again
    return mod


class _MovedLeagueFinder(_ilabc.MetaPathFinder, _ilabc.Loader):
    """Make the statement ``import sportsdataverse.<leaf>`` resolve a moved league."""

    def find_spec(self, fullname, path=None, target=None):
        if not fullname.startswith("sportsdataverse."):
            return None
        leaf = fullname[len("sportsdataverse.") :]
        if "." in leaf or leaf not in _MOVED:
            return None
        return _ilutil.spec_from_loader(fullname, self)

    def create_module(self, spec):
        leaf = spec.name[len("sportsdataverse.") :]
        tgt = _MOVED[leaf]
        _warnings.warn(
            f"import sportsdataverse.{leaf} is deprecated; it moved to sportsdataverse.{tgt}.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _importlib.import_module(f"sportsdataverse.{tgt}")

    def exec_module(self, module):  # module already fully initialised
        pass


if not any(isinstance(f, _MovedLeagueFinder) for f in _sys.meta_path):
    _sys.meta_path.append(_MovedLeagueFinder())
