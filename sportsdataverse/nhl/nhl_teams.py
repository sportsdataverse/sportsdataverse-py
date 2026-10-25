from __future__ import annotations

import polars as pl

from sportsdataverse import _espn_teams


@_espn_teams.cache_clear_shim
def espn_nhl_teams(return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_nhl_teams - look up NHL teams

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe of the NHL teams, one row per team, with the columns in the
        returns table. A payload without teams returns a zero-row frame with those columns. The
        result is not cached: every call fetches (through the package's opt-in TTL response
        cache, ``sportsdataverse.cache``, when it is on).

    Raises:
        NoDataError: ESPN answered 404 for the teams resource.
        AssetFetchError: the fetch failed (403, 429, 5xx, a dropped connection); never returned
            as an empty frame.

    Example:
        Pull the full NHL team directory::

            from sportsdataverse.nhl import espn_nhl_teams
            teams = espn_nhl_teams()
            print(teams.shape)
            teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

        Find Tampa Bay Lightning (team_id 14)::

            import polars as pl
            teams.filter(pl.col("team_id") == 14).to_dicts()

        Pandas round-trip::

            teams_pd = espn_nhl_teams(return_as_pandas=True)
            teams_pd[["team_id", "team_abbreviation", "team_display_name"]].head()

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    url = "http://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams"
    params = {"limit": 1000}
    return _espn_teams.teams_frame(url, params, return_as_pandas, **kwargs)
