from __future__ import annotations

import polars as pl

from sportsdataverse import _espn_teams


@_espn_teams.cache_clear_shim
def espn_mlb_teams(return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_mlb_teams - look up MLB teams from ESPN's Site v2 API.

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe.
            If False (default), returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe of the MLB teams, one row per team, with the columns in the
        returns table. A payload without teams returns a zero-row frame with those columns. The
        result is not cached: every call fetches (through the package's opt-in TTL response
        cache, ``sportsdataverse.cache``, when it is on).

    Raises:
        NoDataError: ESPN answered 404 for the teams resource.
        AssetFetchError: the fetch failed (403, 429, 5xx, a dropped connection); never returned
            as an empty frame.

    Example:
        Pull the full MLB team directory::

            from sportsdataverse.mlb import espn_mlb_teams
            teams = espn_mlb_teams()
            print(teams.shape)
            teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

        Find Los Angeles Dodgers (team_id 19)::

            import polars as pl
            teams.filter(pl.col("team_id") == 19).to_dicts()

        Pandas round-trip::

            teams_pd = espn_mlb_teams(return_as_pandas=True)
            teams_pd[["team_id", "team_abbreviation", "team_display_name"]].head()
    """
    url = "http://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams"
    params = {"limit": 1000}
    return _espn_teams.teams_frame(url, params, return_as_pandas, **kwargs)
