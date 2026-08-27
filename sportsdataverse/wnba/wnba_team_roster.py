"""ESPN WNBA team-level season roster scraper.

Mirror of :func:`sportsdataverse.wbb.espn_wbb_team_roster` for the WNBA
league slug. The actual fetch + parse logic lives in
``sportsdataverse.wbb.wbb_team_roster._espn_basketball_team_roster`` to keep
the wbb / wnba pair DRY.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import polars as pl

from sportsdataverse.wbb.wbb_team_roster import _espn_basketball_team_roster

_LEAGUE_SLUG: str = "wnba"


def espn_wnba_team_roster(
    team_id: int,
    season: int | None = None,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Pull the current ESPN team roster for a WNBA team.

    See :func:`sportsdataverse.wbb.espn_wbb_team_roster` for full documentation
    of the column set. ESPN's ``/teams/{id}/roster`` endpoint ignores
    ``?season=YYYY``; the ``season`` argument is recorded as an output column
    only and does not alter the request URL.

    Args:
        team_id: ESPN WNBA team identifier (e.g. ``3`` for Dallas Wings).
        season: Season year (recorded as output column only).
        raw: If True, returns the parsed JSON dict before any flattening.
        return_as_pandas: If True, returns a pandas DataFrame; otherwise polars.
        **kwargs: Forwarded to ``sportsdataverse.dl_utils.download``.

    Returns:
        Polars (or pandas) DataFrame with the same columns documented in
        :func:`sportsdataverse.wbb.espn_wbb_team_roster`. If ``raw=True``,
        returns the raw response dict.

    Raises:
        sportsdataverse.errors.NoDataError: ESPN returned 404.
        requests.exceptions.RequestException: Other network failures after retries.

    Example:
        Las Vegas Aces (team_id 17) current roster::

            from sportsdataverse.wnba import espn_wnba_team_roster
            roster = espn_wnba_team_roster(team_id=17, season=2024)
            print(roster.shape)
            roster.select(["athlete_id", "full_name", "jersey", "position_abbreviation"]).head()

        Pandas round-trip — useful for one-off notebook work::

            roster_pd = espn_wnba_team_roster(team_id=17, season=2024, return_as_pandas=True)
            roster_pd[["full_name", "jersey", "position_abbreviation", "height"]].head()

        Inspect the raw ESPN payload::

            payload = espn_wnba_team_roster(team_id=17, season=2024, raw=True)
            list(payload.keys())[:8]

        See Also:
            * `wehoop`_ — R sister package; mirrors this surface
            * `nba_api`_ — alternative Python source for NBA/WNBA stats endpoints
            * `hoopR`_ — companion R package for men's basketball

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _espn_basketball_team_roster(
        league=_LEAGUE_SLUG,
        team_id=team_id,
        season=season,
        raw=raw,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )
