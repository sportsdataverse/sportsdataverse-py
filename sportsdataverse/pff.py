"""PFF wrappers for the top-level ``sportsdataverse`` namespace.

Two surfaces, clearly separated:

* **PFF Developer API (current)** -- ``pff_api_*`` (:mod:`sportsdataverse.nfl.pff_api`), PFF's
  official ``https://api.pff.com`` API, authenticated with one API key (``PFF_API_KEY``). Names
  mirror PFF's own ``restish pff <command>`` operations; ``league`` is an ordinary argument, so
  one function serves every league::

      import sportsdataverse as sdv

      sdv.pff_api_facet_passing_summary(league="ncaa", season=2025, week="1")
      sdv.pff_api_team_stats(league="nfl", season=2025, category="offense-passing")

* **PFF Premium Stats (LEGACY)** -- ``pff_<league>_<rest>`` aliases over the reverse-engineered,
  cookie-authenticated ``premium.pff.com`` wrappers (:mod:`sportsdataverse.nfl.pff_core`). The
  per-league shims all install the same 46 bare names bound to different leagues, so they cannot
  be star-imported into one namespace without colliding; this module mints league-prefixed aliases
  instead::

      sdv.pff_nfl_facet_blocking_summary()
      sdv.pff_ncaa_facet_blocking_summary()

  The module-scoped bare names are unchanged::

      from sportsdataverse.nfl import pff
      pff.pff_facet_blocking_summary()
"""

from __future__ import annotations

from sportsdataverse.nfl import pff_api
from sportsdataverse.nfl.pff_api import *  # noqa: F401,F403
from sportsdataverse.nfl.pff_league import make_pff_alias_module

__all__ = [*pff_api.__all__, *make_pff_alias_module(globals())]
