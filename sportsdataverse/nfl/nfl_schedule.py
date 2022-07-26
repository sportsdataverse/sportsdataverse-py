import pandas as pd
import json
import time
from sportsdataverse.dl_utils import download, underscore
from urllib.error import URLError, HTTPError, ContentTooShortError

def espn_nfl_schedule(dates=None, week=None, season_type=None, limit=500) -> pd.DataFrame:
    """espn_nfl_schedule - look up the NFL schedule for a given season

    Args:
        dates (int): Used to define different seasons. 2002 is the earliest available season.
        week (int): Week of the schedule.
        season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.
        limit (int): number of records to return, default: 500.

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
    if limit is None:
        limit_url = ''
    else:
        limit_url = '&limit=' + str(limit)
    cache_buster = int(time.time() * 1000)
    cache_buster_url = '&'+str(cache_buster)
    url = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?{}{}{}{}{}{}".format(
        limit_url,
        dates,
        week,
        season_type,
        cache_buster_url
    )
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
            x = pd.json_normalize(event.get('competitions')[0], sep='_')
            x['game_id'] = x['id'].astype(int)
            x['season'] = event.get('season').get('year')
            x['season_type'] = event.get('season').get('type')
            ev = pd.concat([ev,x],axis=0, ignore_index=True)
    ev = pd.DataFrame(ev)
    ev.columns = [underscore(c) for c in ev.columns.tolist()]

    return ev



def espn_nfl_calendar(season=None, ondays=None) -> pd.DataFrame:
    """espn_nfl_calendar - look up the NFL calendar for a given season

    Args:
        season (int): Used to define different seasons. 2002 is the earliest available season.
        ondays (boolean): Used to return dates for calendar ondays

    Returns:
        pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    if ondays is not None:
        url = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{}/types/2/calendar/ondays".format(season)
        resp = download(url=url)
        txt = json.loads(resp).get('eventDate').get('dates')
        full_schedule = pd.DataFrame(txt,columns=['dates'])
        full_schedule['datenum'] = list(map(lambda x: x[:10].replace("-",""),full_schedule['dates']))
    else:
        if season is None:
            season_url = ''
        else:
            season_url = '&dates=' + str(season)
        url = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?{}{}".format(season_url)
        resp = download(url=url)
        txt = json.loads(resp)
        txt = txt.get('leagues')[0].get('calendar')
        full_schedule = pd.DataFrame()
        for i in range(len(txt)):
            if txt[i].get('entries', None) is not None:
                reg = pd.json_normalize(data = txt[i],
                                        record_path = 'entries',
                                        meta=["label","value","startDate","endDate"],
                                        meta_prefix='season_type_',
                                        record_prefix='week_',
                                        errors="ignore",
                                        sep='_')
                full_schedule = pd.concat([full_schedule,reg], ignore_index=True)
        full_schedule['season']=season
        full_schedule.columns = [underscore(c) for c in full_schedule.columns.tolist()]
        full_schedule = full_schedule.rename(columns={"week_value": "week", "season_type_value": "season_type"})
    return full_schedule
