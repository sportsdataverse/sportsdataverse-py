from typing import List

import polars as pl
from tqdm import tqdm

from sportsdataverse.config import (
    WNBA_BASE_URL,
    WNBA_PLAYER_BOX_URL,
    WNBA_TEAM_BOX_URL,
    WNBA_TEAM_SCHEDULE_URL,
)
from sportsdataverse.errors import SeasonNotFoundError


def load_wnba_pbp(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load WNBA play by play data going back to 2002

    Example:
        `wnba_df = sportsdataverse.wnba.load_wnba_pbp(seasons=range(2002,2022))`

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
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
        i_data = pl.read_parquet(WNBA_BASE_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_wnba_team_boxscore(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load WNBA team boxscore data

    Example:
        `wnba_df = sportsdataverse.wnba.load_wnba_team_boxscore(seasons=range(2002,2022))`

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        team boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise ValueError("season cannot be less than 2002")
        i_data = pl.read_parquet(WNBA_TEAM_BOX_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_wnba_player_boxscore(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load WNBA player boxscore data

    Example:
        `wnba_df = sportsdataverse.wnba.load_wnba_player_boxscore(seasons=range(2002,2022))`

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        player boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise ValueError("season cannot be less than 2002")
        i_data = pl.read_parquet(WNBA_PLAYER_BOX_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_wnba_schedule(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load WNBA schedule data

    Example:
        `wnba_df = sportsdataverse.wnba.load_wnba_schedule(seasons=range(2002,2022))`

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        schedule for  the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise ValueError("season cannot be less than 2002")
        i_data = pl.read_parquet(WNBA_TEAM_SCHEDULE_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data
