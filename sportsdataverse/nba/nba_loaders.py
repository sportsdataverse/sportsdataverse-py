import pandas as pd
import polars as pl
import json
from tqdm import tqdm
from typing import List, Callable, Iterator, Union, Optional
from sportsdataverse.config import NBA_BASE_URL, NBA_TEAM_BOX_URL, NBA_PLAYER_BOX_URL, NBA_TEAM_SCHEDULE_URL
from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.dl_utils import download

def load_nba_pbp(seasons: List[int], return_as_pandas = True) -> pd.DataFrame:
    """Load NBA play by play data going back to 2002

    Example:
        `nba_df = sportsdataverse.nba.load_nba_pbp(seasons=range(2002,2022))`

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pd.DataFrame: Pandas dataframe containing the
        play-by-plays available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        i_data = pl.read_parquet(NBA_BASE_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how = 'vertical')
    return data.to_pandas(use_pyarrow_extension_array = True) if return_as_pandas else data

def load_nba_team_boxscore(seasons: List[int], return_as_pandas = True) -> pd.DataFrame:
    """Load NBA team boxscore data

    Example:
        `nba_df = sportsdataverse.nba.load_nba_team_boxscore(seasons=range(2002,2022))`

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pd.DataFrame: Pandas dataframe containing the
        team boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        i_data = pl.read_parquet(NBA_TEAM_BOX_URL.format(season = i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how = 'vertical')
    return data.to_pandas(use_pyarrow_extension_array = True) if return_as_pandas else data

def load_nba_player_boxscore(seasons: List[int], return_as_pandas = True) -> pd.DataFrame:
    """Load NBA player boxscore data

    Example:
        `nba_df = sportsdataverse.nba.load_nba_player_boxscore(seasons=range(2002,2022))`

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pd.DataFrame: Pandas dataframe containing the
        player boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        i_data = pl.read_parquet(NBA_PLAYER_BOX_URL.format(season = i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how = 'vertical')
    return data.to_pandas(use_pyarrow_extension_array = True) if return_as_pandas else data

def load_nba_schedule(seasons: List[int], return_as_pandas = True) -> pd.DataFrame:
    """Load NBA schedule data

    Example:
        `nba_df = sportsdataverse.nba.load_nba_schedule(seasons=range(2002,2022))`

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pd.DataFrame: Pandas dataframe containing the
        schedule for  the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        i_data = pl.read_parquet(NBA_TEAM_SCHEDULE_URL.format(season = i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how = 'vertical')
    return data.to_pandas(use_pyarrow_extension_array = True) if return_as_pandas else data
