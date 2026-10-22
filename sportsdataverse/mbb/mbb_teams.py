from __future__ import annotations

import polars as pl

from sportsdataverse import _espn_teams


@_espn_teams.cache_clear_shim
def espn_mbb_teams(groups=None, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_mbb_teams - look up the men's college basketball teams

    Args:
        groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe of the men's college basketball teams, one row per team, with the columns in the
        returns table. A payload without teams returns a zero-row frame with those columns. The
        result is not cached: every call fetches (through the package's opt-in TTL response
        cache, ``sportsdataverse.cache``, when it is on).

    Raises:
        NoDataError: ESPN answered 404 for the teams resource.
        AssetFetchError: the fetch failed (403, 429, 5xx, a dropped connection); never returned
            as an empty frame.

    Example:
        Default groups (D1)::

            from sportsdataverse.mbb import espn_mbb_teams
            teams = espn_mbb_teams()
            print(teams.shape)
            print(teams.columns[:8])

        Walk every team-id (handy for batched scrapes)::

            team_ids = teams["team_id"].to_list()
            print(len(team_ids), "D1 teams")

        Pandas round-trip + Division II/III::

            d2_d3 = espn_mbb_teams(groups=51, return_as_pandas=True)
            d2_d3.head()

        See Also:
            * `hoopR`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com

    """
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"
    params = {"groups": groups if groups is not None else "50", "limit": 1000}
    return _espn_teams.teams_frame(url, params, return_as_pandas, **kwargs)
