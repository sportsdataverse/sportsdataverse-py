from __future__ import annotations

import polars as pl

from sportsdataverse import _espn_teams


@_espn_teams.cache_clear_shim
def espn_wnba_teams(return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_wnba_teams - look up WNBA teams

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe of the WNBA teams, one row per team, with the columns in the
        returns table. A payload without teams returns a zero-row frame with those columns. The
        result is not cached: every call fetches (through the package's opt-in TTL response
        cache, ``sportsdataverse.cache``, when it is on).

    Raises:
        NoDataError: ESPN answered 404 for the teams resource.
        AssetFetchError: the fetch failed (403, 429, 5xx, a dropped connection); never returned
            as an empty frame.

    Example:
        Pull the full WNBA team directory::

            from sportsdataverse.wnba import espn_wnba_teams
            teams = espn_wnba_teams()
            print(teams.shape)
            teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

        Find Las Vegas Aces (team_id 17)::

            teams.filter(__import__("polars").col("team_id") == "17").to_dicts()

        Pandas round-trip::

            teams_pd = espn_wnba_teams(return_as_pandas=True)

        See Also:
            * `wehoop`_ — R sister package; mirrors this surface
            * `nba_api`_ — alternative Python source for NBA/WNBA stats endpoints
            * `hoopR`_ — companion R package for men's basketball

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/wnba/teams"
    params = {"limit": 1000}
    return _espn_teams.teams_frame(url, params, return_as_pandas, **kwargs)
