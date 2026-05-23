from __future__ import annotations

from functools import lru_cache

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, underscore


@lru_cache(maxsize=None)
def espn_mlb_teams(return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_mlb_teams - look up MLB teams from ESPN's Site v2 API.

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe.
            If False (default), returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing teams for MLB.
        This function caches by default, so if you want to refresh the data,
        use ``sportsdataverse.mlb.espn_mlb_teams.cache_clear()``.

    Example:
        Pull the full MLB team directory::

            from sportsdataverse.mlb import espn_mlb_teams
            teams = espn_mlb_teams()
            print(teams.shape)
            teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

        Find Los Angeles Dodgers (team_id 19)::

            import polars as pl
            teams.filter(pl.col("team_id") == "19").to_dicts()

        Refresh the cache (the call is ``lru_cache``'d) and round-trip to pandas::

            espn_mlb_teams.cache_clear()
            teams_pd = espn_mlb_teams(return_as_pandas=True)
            teams_pd[["team_id", "team_abbreviation", "team_display_name"]].head()
    """
    url = "http://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams"
    params = {"limit": 1000}
    resp = download(url=url, params=params, **kwargs)
    if resp is not None:
        events_txt = resp.json()

        teams = events_txt.get("sports")[0].get("leagues")[0].get("teams")
        del_keys = ["record", "links"]
        for team in teams:
            for k in del_keys:
                team.get("team").pop(k, None)
        teams = pd.json_normalize(teams, sep="_")
    teams.columns = [underscore(c) for c in teams.columns.tolist()]
    return teams if return_as_pandas else pl.from_pandas(teams)
