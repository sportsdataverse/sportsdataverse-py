from __future__ import annotations

from functools import lru_cache

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, underscore


@lru_cache(maxsize=None)
def espn_nhl_teams(return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_nhl_teams - look up NHL teams

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing teams for the requested league.
        This function caches by default, so if you want to refresh the data, use the command
        sportsdataverse.nhl.espn_nhl_teams.clear_cache().

    Example:
        Pull the full NHL team directory::

            from sportsdataverse.nhl import espn_nhl_teams
            teams = espn_nhl_teams()
            print(teams.shape)
            teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

        Find Tampa Bay Lightning (team_id 14)::

            import polars as pl
            teams.filter(pl.col("team_id") == "14").to_dicts()

        Refresh the cache (the call is ``lru_cache``'d) and round-trip to pandas::

            espn_nhl_teams.cache_clear()
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
