from __future__ import annotations

import polars as pl

from sportsdataverse import _espn_teams


@_espn_teams.cache_clear_shim
def espn_nba_teams(return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_nba_teams - look up NBA teams

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe of the NBA teams, one row per team, with the columns in the
        returns table. A payload without teams returns a zero-row frame with those columns. The
        result is not cached: every call fetches (through the package's opt-in TTL response
        cache, ``sportsdataverse.cache``, when it is on).

    Raises:
        NoDataError: ESPN answered 404 for the teams resource.
        AssetFetchError: the fetch failed (403, 429, 5xx, a dropped connection); never returned
            as an empty frame.

    Example:
        Quick start::

            from sportsdataverse.nba import espn_nba_teams
            teams = espn_nba_teams()
            print(teams.shape)

        Pandas round-trip::

            teams_pd = espn_nba_teams(return_as_pandas=True)
            teams_pd.head()

        Pipeline next step (build a team_id to abbreviation map)::

            teams = espn_nba_teams()
            abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for NBA team data
            * `nba_api <https://github.com/swar/nba_api>`_ -- Python alternative to the NBA Stats API
    """
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams"
    params = {"limit": 1000}
    return _espn_teams.teams_frame(url, params, return_as_pandas, **kwargs)
