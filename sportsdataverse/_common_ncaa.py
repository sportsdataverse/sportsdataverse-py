"""sportsdataverse._common_ncaa — NCAA-specific ESPN helpers.

Bracketology and the catalog-rooted (rather than league-rooted) NCAA endpoints
live here. League-rooted NCAA endpoints (rankings, recruits, week_rankings)
are in :mod:`sportsdataverse._common_espn`'s ``_NCAA_WRAPPERS`` table.

Bracketology
------------

Lives at ``sports.core.api.espn.com/v2/tournament/{tournamentId}/seasons/{year}/bracketology``
— **outside** the per-sport / per-league URL pattern. Tournament IDs:

* ``22`` — NCAA Men's basketball
* ``23`` — NCAA Women's basketball

Per-iteration snapshots are at ``/bracketology/{iteration}``.
"""

from __future__ import annotations

from functools import partial
from typing import Dict, Optional, Union

from sportsdataverse._common_espn import _get

_BRACKETOLOGY_BASE = "https://sports.core.api.espn.com/v2/tournament"

_TOURNAMENT_MBB = 22
_TOURNAMENT_WBB = 23


def _bracketology(tournament_id: int, season: Union[int, str],
                 iteration: Optional[Union[int, str]] = None, **kwargs) -> Dict:
    """GET /tournament/{tid}/seasons/{y}/bracketology[/{iteration}]."""
    suffix = f"/{iteration}" if iteration is not None else ""
    return _get(
        f"{_BRACKETOLOGY_BASE}/{tournament_id}/seasons/{season}/bracketology{suffix}",
        **kwargs,
    )


def espn_mbb_bracketology(season: Union[int, str],
                         iteration: Optional[Union[int, str]] = None, **kwargs) -> Dict:
    """ESPN NCAA Men's basketball bracketology projection."""
    return _bracketology(_TOURNAMENT_MBB, season, iteration, **kwargs)


def espn_wbb_bracketology(season: Union[int, str],
                         iteration: Optional[Union[int, str]] = None, **kwargs) -> Dict:
    """ESPN NCAA Women's basketball bracketology projection."""
    return _bracketology(_TOURNAMENT_WBB, season, iteration, **kwargs)


# Per-league bracketology wrappers (helpers exposed via the league extension imports).
_NCAA_BRACKETOLOGY_WRAPPERS = {
    "mbb": espn_mbb_bracketology,
    "wbb": espn_wbb_bracketology,
}


def register_ncaa_bracketology(league_short: str, namespace: dict) -> None:
    """Add the league-appropriate ``espn_{league}_bracketology`` to a namespace.

    Called by the NCAA basketball extension modules to expose bracketology
    alongside the universal wrappers (which live under ``espn_mbb_*`` /
    ``espn_wbb_*`` via :func:`sportsdataverse._common_espn.make_league_module`).
    """
    fn = _NCAA_BRACKETOLOGY_WRAPPERS.get(league_short)
    if fn is None:
        return
    name = f"espn_{league_short}_bracketology"
    namespace[name] = fn
