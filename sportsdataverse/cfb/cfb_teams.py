import pandas as pd
import polars as pl
import json
from sportsdataverse.dl_utils import download, underscore

def espn_cfb_teams(groups=None, **kwargs) -> pd.DataFrame:
    """espn_cfb_teams - look up the college football teams

    Args:
        groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.

    Returns:
        pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.
    """
    params = {
        "groups": groups if groups is not None else "80",
        "limit": 1000
    }
    url = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/teams"
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
    return teams
