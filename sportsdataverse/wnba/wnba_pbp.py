import pyarrow.parquet as pq
import pandas as pd
import json
from typing import List, Callable, Iterator, Union, Optional
from sportsdataverse.config import WNBA_BASE_URL, WNBA_TEAM_BOX_URL, WNBA_PLAYER_BOX_URL, WNBA_TEAM_SCHEDULE_URL
from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.dl_utils import download

def wnba_calendar(season: int) -> pd.DataFrame:
    """wnba_calendar - look up the WNBA calendar for a given season

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
