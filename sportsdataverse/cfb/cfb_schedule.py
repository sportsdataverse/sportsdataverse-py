import pandas as pd
import json
from sportsdataverse.dl_utils import download
from urllib.error import URLError, HTTPError, ContentTooShortError

def espn_cfb_schedule(dates=None, week=None, season_type=None, groups=None) -> pd.DataFrame:
    """espn_cfb_schedule - look up the college football schedule for a given season

    Args:
        dates (int): Used to define different seasons. 2002 is the earliest available season.
        week (int): Week of the schedule.
        groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.
        season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.

    Returns:
        pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.
    """
    if week is None:
        week = ''
    else:
        week = '&week=' + str(week)
    if dates is None:
        dates = ''
    else:
        dates = '&dates=' + str(dates)
    if season_type is None:
        season_type = ''
    else:
        season_type = '&seasontype=' + str(season_type)
    if groups is None:
        groups = '&groups=80'
    else:
        groups = '&groups=' + str(groups)
    ev = pd.DataFrame()
    url = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?limit=300{}{}{}{}".format(groups,dates,week,season_type)
    resp = download(url=url)
    if resp is not None:
        events_txt = json.loads(resp)

        events = events_txt['events']
        for event in events:
            if 'links' in event['competitions'][0]['competitors'][0]['team'].keys():
                del event['competitions'][0]['competitors'][0]['team']['links']
            if 'links' in event['competitions'][0]['competitors'][1]['team'].keys():
                del event['competitions'][0]['competitors'][1]['team']['links']
            if event['competitions'][0]['competitors'][0]['homeAway']=='home':
                event['competitions'][0]['home'] = event['competitions'][0]['competitors'][0]['team']
            else:
                event['competitions'][0]['away'] = event['competitions'][0]['competitors'][0]['team']
            if event['competitions'][0]['competitors'][1]['homeAway']=='away':
                event['competitions'][0]['away'] = event['competitions'][0]['competitors'][1]['team']
            else:
                event['competitions'][0]['home'] = event['competitions'][0]['competitors'][1]['team']

            del_keys = ['broadcasts','geoBroadcasts', 'headlines']
            for k in del_keys:
                if k in event['competitions'][0].keys():
                    del event['competitions'][0][k]

            ev = ev.append(pd.json_normalize(event['competitions'][0]))
    ev = pd.DataFrame(ev)
    return ev



def espn_cfb_calendar(season=None, groups=None) -> pd.DataFrame:
    """espn_cfb_calendar - look up the men's college football calendar for a given season

    Args:
        season (int): Used to define different seasons. 2002 is the earliest available season.
        groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.

    Returns:
        pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    if season is None:
        season = ''
    else:
        season = '&dates=' + str(season)
    if groups is None:
        groups = '&groups=80'
    else:
        groups = '&groups=' + str(groups)
    url = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?{}{}".format(season, groups)
    resp = download(url=url)
    txt = json.loads(resp)['leagues'][0]['calendar']
    full_schedule = pd.DataFrame()
    for i in range(len(txt)):
        reg = pd.DataFrame(txt[i]['entries'])
        full_schedule = pd.concat([full_schedule,reg], ignore_index=True)
    full_schedule['season']=season
    return full_schedule
