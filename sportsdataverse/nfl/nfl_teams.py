from __future__ import annotations

import polars as pl

from sportsdataverse import _espn_teams


@_espn_teams.cache_clear_shim
def espn_nfl_teams(return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_nfl_teams - look up NFL teams

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe of the NFL teams, one row per team, with the columns in the
        returns table. A payload without teams returns a zero-row frame with those columns. The
        result is not cached: every call fetches (through the package's opt-in TTL response
        cache, ``sportsdataverse.cache``, when it is on).

    Raises:
        NoDataError: ESPN answered 404 for the teams resource.
        AssetFetchError: the fetch failed (403, 429, 5xx, a dropped connection); never returned
            as an empty frame.

    Example:
        Quick start::

            from sportsdataverse.nfl import espn_nfl_teams
            teams = espn_nfl_teams()
            teams.shape

        Pandas round-trip::

            teams_pd = espn_nfl_teams(return_as_pandas=True)
            teams_pd[["team_abbreviation", "team_display_name"]].head()

        Pipeline next step (build a team_id to abbreviation map)::

            abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))

    """
    url = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/teams"
    params = {"limit": 1000}
    return _espn_teams.teams_frame(url, params, return_as_pandas, **kwargs)
