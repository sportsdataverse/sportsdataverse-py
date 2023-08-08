from functools import lru_cache

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, underscore


@lru_cache(maxsize=None)
def espn_mbb_teams(groups=None, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_mbb_teams - look up the men's college basketball teams

    Args:
        groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing teams for the requested league.
        This function caches by default, so if you want to refresh the data, use the command
        sportsdataverse.mbb.espn_mbb_teams.clear_cache().

    Example:
        `mbb_df = sportsdataverse.mbb.espn_mbb_teams()`

    """
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"
    params = {"groups": groups if groups is not None else "50", "limit": 1000}
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
