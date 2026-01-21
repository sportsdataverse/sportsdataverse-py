import pandas as pd
import json
from sportsdataverse.dl_utils import download, underscore


def espn_cfb_teams(groups=None) -> pd.DataFrame:
    """espn_cfb_teams - look up the college football teams

    Args:
        groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.

    Returns:
        pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.
    """
    if groups is None:
        groups = "&groups=80"
    else:
        groups = "&groups=" + str(groups)
    url = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/teams?{}&limit=1000".format(
        groups
    )
    resp = download(url=url)
    teams = pd.DataFrame()

    if resp is not None:
        try:
            events_txt = json.loads(resp)
            teams_data = (
                events_txt.get("sports", [{}])[0]
                .get("leagues", [{}])[0]
                .get("teams", [])
            )
            del_keys = ["record", "links"]
            for team in teams_data:
                for k in del_keys:
                    team.get("team", {}).pop(k, None)
            teams = pd.json_normalize(teams_data, sep="_")
        except (json.JSONDecodeError, ValueError, KeyError, IndexError) as e:
            print(f"Error decoding teams JSON from {url}: {e}")
            teams = pd.DataFrame()

    if not teams.empty:
        teams.columns = [underscore(c) for c in teams.columns.tolist()]
    return teams
