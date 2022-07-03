import pyarrow.parquet as pq
import pandas as pd
import json
from tqdm import tqdm
from typing import List, Callable, Iterator, Union, Optional
from sportsdataverse.config import NHL_BASE_URL, NHL_TEAM_BOX_URL, NHL_TEAM_SCHEDULE_URL, NHL_TEAM_LOGO_URL, NHL_PLAYER_BOX_URL
from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.dl_utils import download

def load_nhl_pbp(seasons: List[int]) -> pd.DataFrame:
    """Load NHL play by play data going back to 2011

    Example:
        `nhl_df = sportsdataverse.nhl.load_nhl_pbp(seasons=range(2011,2021))`

    Args:
        seasons (list): Used to define different seasons. 2011 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2011.
    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2011:
            raise SeasonNotFoundError("season cannot be less than 2011")
        i_data = pd.read_parquet(NHL_BASE_URL.format(season=i), engine='auto', columns=None)
        data = data.append(i_data)
    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_nhl_schedule(seasons: List[int]) -> pd.DataFrame:
    """Load NHL schedule data

    Example:
        `nhl_df = sportsdataverse.nhl.load_nhl_schedule(seasons=range(2002,2021))`

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
        i_data = pd.read_parquet(NHL_TEAM_SCHEDULE_URL.format(season = i), engine='auto', columns=None)
        data = data.append(i_data)
    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)

    return data

def load_nhl_team_boxscore(seasons: List[int]) -> pd.DataFrame:
    """Load NHL team boxscore data

    Example:
        `nhl_df = sportsdataverse.nhl.load_nhl_team_boxscore(seasons=range(2011,2022))`

    Args:
        seasons (list): Used to define different seasons. 2011 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing the
        team boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2011.
    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2011:
            raise SeasonNotFoundError("season cannot be less than 2011")
        i_data = pd.read_parquet(NHL_TEAM_BOX_URL.format(season = i), engine='auto', columns=None)
        data = data.append(i_data)
    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)

    return data

def load_nhl_player_boxscore(seasons: List[int]) -> pd.DataFrame:
    """Load NHL player boxscore data

    Example:
        `nhl_df = sportsdataverse.nhl.load_nhl_player_boxscore(seasons=range(2011,2022))`

    Args:
        seasons (list): Used to define different seasons. 2011 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing the
        player boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2011.
    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2011:
            raise SeasonNotFoundError("season cannot be less than 2011")
        i_data = pd.read_parquet(NHL_PLAYER_BOX_URL.format(season = i), engine='auto', columns=None)
        data = data.append(i_data)
    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)

    return data

def nhl_teams() -> pd.DataFrame:
    """Load NHL team ID information and logos

    Example:
        `nhl_df = sportsdataverse.nhl.nhl_teams()`

    Args:

    Returns:
        pd.DataFrame: Pandas dataframe containing teams available for the requested seasons.
    """
    df = pd.read_csv(NHL_TEAM_LOGO_URL, low_memory=False)
    return df