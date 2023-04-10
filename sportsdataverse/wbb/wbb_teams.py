import pandas as pd
import json
from sportsdataverse.dl_utils import download, underscore
from urllib.error import URLError, HTTPError, ContentTooShortError

def espn_wbb_teams(groups=None) -> pd.DataFrame:
    """espn_wbb_teams - look up the women's college basketball teams

    Args:
        groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.

    Returns:
        pd.DataFrame: Pandas dataframe containing teams for the requested league.
    """
    if groups is None:
        groups = '&groups=50'
    else:
        groups = '&groups=' + str(groups)
    ev = pd.DataFrame()
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams?limit=1000{}".format(groups)
    resp = download(url=url)
    if resp is not None:
        events_txt = json.loads(resp)

        teams = events_txt.get('sports')[0].get('leagues')[0].get('teams')
        del_keys = ['record', 'links']
        for team in teams:
            for k in del_keys:
                team.get('team').pop(k, None)
        teams = pd.json_normalize(teams, sep='_')
    teams.columns = [underscore(c) for c in teams.columns.tolist()]
    return teams

