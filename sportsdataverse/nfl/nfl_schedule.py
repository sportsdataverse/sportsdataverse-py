import pandas as pd
import json
from sportsdataverse.dl_utils import download

def espn_nfl_schedule(dates=None, week=None, season_type=None, limit=500) -> pd.DataFrame:
    """espn_nfl_schedule - look up the NFL schedule for a given date from ESPN

    Args:
        dates (int): Used to define different seasons. 2002 is the earliest available season.
        week (int): Used to define different weeks.
        season_type (int): season type, 1 for pre-season, 2 for regular season, 3 for post-season, 4 for all-star, 5 for off-season
        limit (int): number of records to return, default: 500.
    Returns:
        pd.DataFrame: Pandas dataframe containing
        schedule events for the requested season.
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

    url = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?limit={}{}{}{}".format(limit, dates, week, season_type)
    resp = download(url=url)

    ev = pd.DataFrame()
    if resp is not None:
        events_txt = json.loads(resp)

        events = events_txt.get('events')
        for event in events:
            event.get('competitions')[0].get('competitors')[0].get('team').pop('links',None)
            event.get('competitions')[0].get('competitors')[1].get('team').pop('links',None)
            if event.get('competitions')[0].get('competitors')[0].get('homeAway')=='home':
                event['competitions'][0]['home'] = event.get('competitions')[0].get('competitors')[0].get('team')
                event['competitions'][0]['away'] = event.get('competitions')[0].get('competitors')[1].get('team')
            else:
                event['competitions'][0]['away'] = event.get('competitions')[0].get('competitors')[0].get('team')
                event['competitions'][0]['home'] = event.get('competitions')[0].get('competitors')[1].get('team')

            del_keys = ['broadcasts','geoBroadcasts', 'headlines', 'series']
            for k in del_keys:
                event.get('competitions')[0].pop(k, None)
            x = pd.json_normalize(event.get('competitions')[0])
            x['game_id'] = x['id'].astype(int)
            x['season'] = event.get('season').get('year')
            x['season_type'] = event.get('season').get('type')
            ev = pd.concat([ev, x], axis=0, ignore_index=True)
    ev = pd.DataFrame(ev)
    return ev



def espn_nfl_calendar(season=None) -> pd.DataFrame:
    """espn_nfl_calendar - look up the NFL calendar for a given season from ESPN

    Args:
        season (int): Used to define different seasons. 2002 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    url = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={}".format(season)
    resp = download(url=url)
    txt = json.loads(resp)['leagues'][0]['calendar']
    full_schedule = pd.DataFrame()
    for i in range(len(txt)):
        reg = pd.DataFrame(txt[i]['entries'])
        full_schedule = pd.concat([full_schedule,reg], ignore_index=True)
    full_schedule['season']=season
    return full_schedule
