import pandas as pd
import polars as pl
import json
from sportsdataverse.dl_utils import download, underscore

def espn_nba_teams(return_as_pandas=True, **kwargs) -> pd.DataFrame:
    """espn_nba_teams - look up NBA teams

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pd.DataFrame: Pandas dataframe containing teams for the requested league.

    Example:
        `nba_df = sportsdataverse.nba.espn_nba_teams()`

    """
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams"
    params = {
        "limit": 1000
    }
    resp = download(url=url, params = params, **kwargs)
    if resp is not None:
        events_txt = resp.json()

        teams = events_txt.get('sports')[0].get('leagues')[0].get('teams')
        del_keys = ['record', 'links']
        for team in teams:
            for k in del_keys:
                team.get('team').pop(k, None)
        teams = pd.json_normalize(teams, sep='_')
    teams.columns = [underscore(c) for c in teams.columns.tolist()]
    return teams if return_as_pandas else pl.from_pandas(teams)

