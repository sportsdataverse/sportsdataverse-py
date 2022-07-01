import pyarrow.parquet as pq
import pandas as pd
import json
from tqdm import tqdm
from typing import List, Callable, Iterator, Union, Optional
from sportsdataverse.config import CFB_BASE_URL, CFB_ROSTER_URL, CFB_TEAM_LOGO_URL, CFB_TEAM_SCHEDULE_URL, CFB_TEAM_INFO_URL
from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.dl_utils import download

def load_cfb_pbp(seasons: List[int]) -> pd.DataFrame:
    """Load college football play by play data going back to 2003

    Example:
        `cfb_df = sportsdataverse.cfb.load_cfb_pbp(seasons=range(2003,2021))`

    Args:
        seasons (list): Used to define different seasons. 2003 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2003.
    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2003:
            raise SeasonNotFoundError("season cannot be less than 2003")
        i_data = pd.read_parquet(CFB_BASE_URL.format(season=i), engine='auto', columns=None)
        #data = data.append(i_data)
        data = pd.concat([data,i_data],ignore_index=True)
    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_cfb_schedule(seasons: List[int]) -> pd.DataFrame:
    """Load college football schedule data

    Example:
        `cfb_df = sportsdataverse.cfb.load_cfb_schedule(seasons=range(2002,2021))`

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        i_data = pd.read_parquet(CFB_TEAM_SCHEDULE_URL.format(season = i), engine='auto', columns=None)
        #data = data.append(i_data)
        data = pd.concat([data,i_data],ignore_index=True)
    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)

    return data

def load_cfb_rosters(seasons: List[int]) -> pd.DataFrame:
    """Load roster data

    Example:
        `cfb_df = sportsdataverse.cfb.load_cfb_rosters(seasons=range(2014,2021))`

    Args:
        seasons (list): Used to define different seasons. 2014 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing rosters available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2014.
    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2004:
            raise SeasonNotFoundError("season cannot be less than 2004")
        i_data = pd.read_parquet(CFB_ROSTER_URL.format(season = i), engine='auto', columns=None)
        data = data.append(i_data)
    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)

    return data

def load_cfb_team_info(seasons: List[int]) -> pd.DataFrame:
    """Load college football team info

    Example:
        `cfb_df = sportsdataverse.cfb.load_cfb_team_info(seasons=range(2002,2021))`

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing the team info available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        try:
            i_data = pd.read_parquet(CFB_TEAM_INFO_URL.format(season = i), engine='auto', columns=None)
        except:
            print(f'We don\'t seem to have data for the {i} season.')
        data = data.append(i_data)
    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)

    return data

def get_cfb_teams() -> pd.DataFrame:
    """Load college football team ID information and logos

    Example:
        `cfb_df = sportsdataverse.cfb.cfb_teams()`

    Args:

    Returns:
        pd.DataFrame: Pandas dataframe containing teams available for the requested seasons.
    """
    df = pd.read_parquet(CFB_TEAM_LOGO_URL, engine='auto', columns=None)
    
    return df
