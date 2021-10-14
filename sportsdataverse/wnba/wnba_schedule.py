import pyarrow.parquet as pq
import pandas as pd
import json
from typing import List, Callable, Iterator, Union, Optional
from sportsdataverse.config import WNBA_BASE_URL, WNBA_TEAM_BOX_URL, WNBA_PLAYER_BOX_URL, WNBA_TEAM_SCHEDULE_URL
from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.dl_utils import download

def espn_wnba_schedule(dates=None, season_type=None) -> pd.DataFrame:
    """espn_wnba_schedule - look up the WNBA schedule for a given season

    Args:
        dates (int): Used to define different seasons. 2002 is the earliest available season.
        season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.

    Returns:
        pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.
    """
    if dates is None:
        dates = ''
    else:
        dates = '&dates=' + str(dates)
    if season_type is None:
        season_type = ''
    else:
        season_type = '&seasontype=' + str(season_type)

    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard?limit=300{}{}".format(dates,season_type)
    ev = pd.DataFrame()
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

def espn_wnba_calendar(season=None) -> pd.DataFrame:
    """espn_wnba_calendar - look up the WNBA calendar for a given season

    Args:
        season (int): Used to define different seasons. 2002 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing
        calendar dates for the requested season.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    if int(season) < 2002:
        raise SeasonNotFoundError("season cannot be less than 2002")
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard?dates={}".format(season)
    resp = download(url=url)
    txt = json.loads(resp)['leagues'][0]['calendar']
    datenum = list(map(lambda x: x[:10].replace("-",""),txt))
    date = list(map(lambda x: x[:10],txt))

    year = list(map(lambda x: x[:4],txt))
    month = list(map(lambda x: x[5:7],txt))
    day = list(map(lambda x: x[8:10],txt))

    data = {"season": season,
            "datetime" : txt,
            "date" : date,
            "year": year,
            "month": month,
            "day": day,
            "dateURL": datenum
    }
    df = pd.DataFrame(data)
    df['url']="http://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard?dates="
    df['url']= df['url'] + df['dateURL']
    return df
