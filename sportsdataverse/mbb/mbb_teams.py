import pandas as pd
import json
from sportsdataverse.dl_utils import download
from urllib.error import URLError, HTTPError, ContentTooShortError

def espn_mbb_teams(groups=None) -> pd.DataFrame:
    """espn_mbb_teams - look up the men's college basketball teams

    Args:
        groups (int): Used to define different divisions. 50 is DI, 51 is DII, 52 is DIII.

    Returns:
        pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.
    """
    if groups is None:
        groups = '&groups=80'
    else:
        groups = '&groups=' + str(groups)
    ev = pd.DataFrame()
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?groups={}&limit=1000".format(groups)
    resp = download(url=url)
    if resp is not None:
        events_txt = json.loads(resp)

        teams = events_txt.get('sports')[0].get('leagues')[0].get('teams')
        for team in teams:
            if 'record' in team.get('team').keys():
                del team.get('team')['record']
            if 'links' in team.get('team').keys():
                del team.get('team')['links']
        teams = pd.json_normalize(teams)
    return teams

