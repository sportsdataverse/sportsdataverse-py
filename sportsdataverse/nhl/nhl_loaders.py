import pyarrow.parquet as pq
import pandas as pd
import json
from typing import List, Callable, Iterator, Union, Optional
from sportsdataverse.config import NHL_BASE_URL, NHL_ROSTER_URL, NHL_TEAM_LOGO_URL, NHL_TEAM_SCHEDULE_URL, NHL_PLAYER_STATS_URL
from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.dl_utils import download

def load_nhl_pbp(seasons: List[int]) -> pd.DataFrame:
    """Load NHL play by play data going back to 1999

    Example:
        `nhl_df = sportsdataverse.nhl.load_nhl_pbp(seasons=range(1999,2021))`

    Args:
        seasons (list): Used to define different seasons. 1999 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 1999.
    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in seasons:
        if int(i) < 1999:
            raise SeasonNotFoundError("season cannot be less than 1999")
        i_data = pd.read_parquet(NHL_BASE_URL.format(season=i), engine='auto', columns=None)
        data = data.append(i_data)
    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)
    return data

def load_nhl_schedule(seasons: List[int]) -> pd.DataFrame:
    """Load NHL schedule data

    Example:
        `nhl_df = sportsdataverse.nhl.load_nhl_schedule(seasons=range(1999,2021))`

    Args:
        seasons (list): Used to define different seasons. 1999 is the earliest available season.

    Returns:
        pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

    Raises:
        ValueError: If `season` is less than 1999.
    """
    data = pd.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in seasons:
        if int(i) < 1999:
            raise SeasonNotFoundError("season cannot be less than 1999")
        i_data = pd.read_parquet(NHL_TEAM_SCHEDULE_URL.format(season = i), engine='auto', columns=None)
        data = data.append(i_data)
    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)

    return data

def load_nhl_player_stats() -> pd.DataFrame:
    """Load NHL player stats data

    Example:
        `nhl_df = sportsdataverse.nhl.load_nhl_player_stats()`

    Args:

    Returns:
        pd.DataFrame: Pandas dataframe containing player stats.
    """
    data = pd.DataFrame()
    i_data = pd.read_parquet(NHL_PLAYER_STATS_URL.format(season = i), engine='auto', columns=None)
    data = data.append(i_data)
    #Give each row a unique index
    data.reset_index(drop=True, inplace=True)

    return data

def load_nhl_rosters() -> pd.DataFrame:
    """Load NHL roster data for all seasons

    Example:
        `nhl_df = sportsdataverse.nhl.load_nhl_rosters(seasons=range(1999,2021))`

    Returns:
        pd.DataFrame: Pandas dataframe containing rosters available for the requested seasons.

    """
    data = pd.DataFrame()

    data = pd.read_csv(NHL_ROSTER_URL, compression='gzip', error_bad_lines=False, low_memory=False)
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