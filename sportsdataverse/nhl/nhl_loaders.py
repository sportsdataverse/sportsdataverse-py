from typing import List

import polars as pl
from tqdm import tqdm

from sportsdataverse.config import (
    NHL_BASE_URL,
    NHL_PLAYER_BOX_URL,
    NHL_TEAM_BOX_URL,
    NHL_TEAM_LOGO_URL,
    NHL_TEAM_SCHEDULE_URL,
)
from sportsdataverse.errors import SeasonNotFoundError


def load_nhl_pbp(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NHL play by play data going back to 2011

    Example:
        `nhl_df = sportsdataverse.nhl.load_nhl_pbp(seasons=range(2011,2021))`

    Args:
        seasons (list): Used to define different seasons. 2011 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the play-by-plays available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2011.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2011:
            raise SeasonNotFoundError("season cannot be less than 2011")
        i_data = pl.read_parquet(NHL_BASE_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nhl_schedule(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NHL schedule data

    Example:
        `nhl_df = sportsdataverse.nhl.load_nhl_schedule(seasons=range(2002,2021))`

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the schedule for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        i_data = pl.read_parquet(NHL_TEAM_SCHEDULE_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nhl_team_boxscore(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NHL team boxscore data

    Example:
        `nhl_df = sportsdataverse.nhl.load_nhl_team_boxscore(seasons=range(2011,2022))`

    Args:
        seasons (list): Used to define different seasons. 2011 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        team boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2011.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2011:
            raise SeasonNotFoundError("season cannot be less than 2011")
        i_data = pl.read_parquet(NHL_TEAM_BOX_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nhl_player_boxscore(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NHL player boxscore data

    Example:
        `nhl_df = sportsdataverse.nhl.load_nhl_player_boxscore(seasons=range(2011,2022))`

    Args:
        seasons (list): Used to define different seasons. 2011 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        player boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2011.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2011:
            raise SeasonNotFoundError("season cannot be less than 2011")
        i_data = pl.read_parquet(NHL_PLAYER_BOX_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def nhl_teams(return_as_pandas=False) -> pl.DataFrame:
    """Load NHL team ID information and logos

    Example:
        `nhl_df = sportsdataverse.nhl.nhl_teams()`

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing teams available for the requested seasons.
    """
    return pl.read_csv(NHL_TEAM_LOGO_URL).to_pandas if return_as_pandas else pl.read_csv(NHL_TEAM_LOGO_URL)
