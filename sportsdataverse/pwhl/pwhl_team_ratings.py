"""PWHL native power ratings -- thin shim over
:mod:`sportsdataverse.nhl.nhl_team_ratings`.

The opponent-adjust + shrinkage solver is entirely league-agnostic (every
constant is passed in from ``LEAGUE_CONSTANTS``), so this module re-exports
the NHL implementation with ``league="pwhl"`` defaulted -- the same
by-reference pattern ``wbb_rapm`` uses over ``mbb_rapm``. Women's-league
constants live in the shared ``LEAGUE_CONSTANTS["pwhl"]`` row.

**Oracle gate deferred:** ``load_pwhl_pbp`` carries a categorical
``shot_quality`` column, not the numeric ``xg`` the rating engine consumes,
and lacks the even-strength skater/goalie state columns -- so a real PWHL
rating validation is deferred until xG-bearing PWHL pbp lands in sdv-py
(design spec Sec 9-7). See ``tests/fixtures/nhl_prediction/README.md`` for
the capture contract. The shim wiring is unit-tested now.

Example:
    Quick start::

        from sportsdataverse.pwhl.pwhl_team_ratings import pwhl_team_ratings

        ratings = pwhl_team_ratings(2024)

See Also:
    * `nhl-api-py`_ -- companion NHL Python client (the core this shim re-exports).

.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

import importlib
from typing import Any

# ponytail: importlib.import_module, not `from sportsdataverse.nhl import
# nhl_team_ratings` -- the nhl package __init__ exports a function of the same
# name, which shadows the submodule attribute. import_module reads sys.modules
# directly, so `_core.nhl_team_ratings` is always the function (and stays
# monkeypatch-interceptable at call time for the wiring test).
_core = importlib.import_module("sportsdataverse.nhl.nhl_team_ratings")

__all__ = ["pwhl_team_ratings"]


def pwhl_team_ratings(seasons: Any, *, league: str = "pwhl", **kwargs: Any) -> Any:
    """PWHL opponent-adjusted, shrunk even-strength xG team ratings.

    Delegates to :func:`sportsdataverse.nhl.nhl_team_ratings.nhl_team_ratings`
    with ``league="pwhl"`` defaulted. Oracle gate deferred (no xG-bearing
    PWHL pbp yet -- see module docstring).

    Args:
        seasons: an int or iterable of seasons.
        league: league key (defaults to ``"pwhl"``).
        **kwargs: forwarded to the NHL core (``as_of_date``, ``return_as_pandas``).

    Returns:
        The NHL core's ratings frame, computed with PWHL constants.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_team_ratings import pwhl_team_ratings
            ratings = pwhl_team_ratings(2024)
    """
    return _core.nhl_team_ratings(seasons, league=league, **kwargs)
