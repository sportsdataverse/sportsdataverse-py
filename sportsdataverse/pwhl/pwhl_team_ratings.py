"""PWHL native power ratings, built from PWHL data.

The opponent-adjust + shrinkage solver is the NHL one
(:func:`sportsdataverse.nhl.nhl_team_ratings.adjust_rate_opponent`, every
constant passed in from ``LEAGUE_CONSTANTS["pwhl"]``). The inputs are not:
``load_pwhl_pbp`` carries no ``xg`` and no skater-count columns, so the
per-game rates come from
:func:`sportsdataverse.pwhl.pwhl_xg_proxy.pwhl_ratings_from_proxy`, which
reads ``load_pwhl_pbp`` / ``load_pwhl_schedules`` / ``load_pwhl_game_info``
and scores shots with the PWHL coordinate xG model. No NHL loader is read.

Example:
    Quick start::

        from sportsdataverse.pwhl.pwhl_team_ratings import pwhl_team_ratings

        ratings = pwhl_team_ratings(2025)

See Also:
    * `fastRhockey`_ -- companion R PWHL/NHL client (raw HockeyTech capture).

.. _fastRhockey: https://fastRhockey.sportsdataverse.org
"""

from __future__ import annotations

import warnings
from typing import Any

from sportsdataverse.pwhl.pwhl_xg_proxy import pwhl_ratings_from_proxy

__all__ = ["pwhl_team_ratings"]


def pwhl_team_ratings(seasons: Any, *, league: Any = None, **kwargs: Any) -> Any:
    """PWHL opponent-adjusted, shrunk even-strength xG team ratings.

    Delegates to :func:`sportsdataverse.pwhl.pwhl_xg_proxy.pwhl_ratings_from_proxy`,
    which loads PWHL pbp, schedules and game info only.

    Args:
        seasons: an int or iterable of PWHL season end-years (``>= 2024``).
        league: deprecated and ignored (the ratings are always PWHL); passing it warns. Removed in 0.4.0.
        **kwargs: forwarded to :func:`pwhl_ratings_from_proxy` (``as_of_date``,
            ``xg_model``, ``xg_method``, ``return_as_pandas``).

    Raises:
        DeprecationWarning: as a warning, when ``league`` is passed.

    Returns:
        One row per (season, team), the
        :func:`sportsdataverse.nhl.nhl_team_ratings.nhl_team_ratings` schema
        (``season``, ``team``, ``adj_xgf``, ``adj_xga``, ``adj_xg_net``,
        ``adj_gf``, ``adj_ga``, ``games``, ``off_rank``, ``def_rank``,
        ``net_rank``, ``net_z``).

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_team_ratings import pwhl_team_ratings
            ratings = pwhl_team_ratings(2025)
    """
    if league is not None:
        warnings.warn(
            "pwhl_team_ratings(league=...) is deprecated and ignored (the ratings are always PWHL); it is removed in 0.4.0",
            DeprecationWarning,
            stacklevel=2,
        )
    return pwhl_ratings_from_proxy(seasons, **kwargs)
