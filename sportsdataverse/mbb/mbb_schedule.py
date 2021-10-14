import pyarrow.parquet as pq
import pandas as pd
import json
from typing import List, Callable, Iterator, Union, Optional
from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.dl_utils import download

def espn_mbb_schedule(dates=None, groups=None, season_type=None) -> pd.DataFrame:
    """espn_mbb_schedule - look up the men's college basketball scheduler for a given season

    Args:
        dates (int): Used to define different seasons. 2002 is the earliest available season.
        groups (int): Used to define different divisions. 50 is Division I, 51 is Division II, 52 is Division III.
        season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.

    Returns:
        pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.
    """
    if dates is None:
        dates = ''
    else:
        dates = '&dates=' + str(dates)
    if groups is None:
        groups = '&groups=50'
    else:
        groups = '&groups=' + str(groups)
    if season_type is None:
        season_type = ''
    else:
        season_type = '&seasontype=' + str(season_type)
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?limit=300{}{}{}".format(dates, groups, season_type)
    resp = download(url=url)
    txt = json.loads(resp)['leagues'][0]['calendar']
    txt = list(map(lambda x: x[:10].replace("-",""),txt))

    ev = pd.DataFrame()
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

def espn_mbb_calendar(season=None) -> pd.DataFrame:
    """espn_mbb_calendar - look up the men's college basketball calendar for a given season

    Args:
        season (int): Used to define different seasons. 2002 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.
    """
    if int(season) < 2002:
        raise SeasonNotFoundError("season cannot be less than 2002")
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={}".format(season)
    resp = download(url=url)
    txt = json.loads(resp)['leagues'][0]['calendar']
    datenum = list(map(lambda x: x[:10].replace("-",""),txt))
    date = list(map(lambda x: x[:10],txt))

    year = list(map(lambda x: x[:4],txt))
    month = list(map(lambda x: x[5:7],txt))
    day = list(map(lambda x: x[8:10],txt))

    data = {
        "season": season,
        "datetime" : txt,
        "date" : date,
        "year": year,
        "month": month,
        "day": day,
        "dateURL": datenum
    }
    df = pd.DataFrame(data)
    df['url']="http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates="
    df['url']= df['url'] + df['dateURL']
    return df