from typing import List

import polars as pl
from tqdm import tqdm

from sportsdataverse.config import (
    CFB_BASE_URL,
    CFB_BETTING_LINES_URL,
    CFB_ROSTER_URL,
    CFB_TEAM_INFO_URL,
    CFB_TEAM_LOGO_URL,
    CFB_TEAM_SCHEDULE_URL,
)
from sportsdataverse.errors import SeasonNotFoundError


def load_cfb_pbp(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load college football play by play data going back to 2003

    Example:
        `cfb_df = sportsdataverse.cfb.load_cfb_pbp(seasons=range(2003,2021))`

    Args:
        seasons (list): Used to define different seasons. 2003 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the play-by-plays available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2003.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2003:
            raise SeasonNotFoundError("season cannot be less than 2003")
        i_data = pl.read_parquet(CFB_BASE_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_cfb_schedule(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load college football schedule data

    Example:
        `cfb_df = sportsdataverse.cfb.load_cfb_schedule(seasons=range(2002,2021))`

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
        i_data = pl.read_parquet(CFB_TEAM_SCHEDULE_URL.format(season=i), use_pyarrow=True, columns=None)
        # data = data.append(i_data)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_cfb_rosters(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load roster data

    Example:
        `cfb_df = sportsdataverse.cfb.load_cfb_rosters(seasons=range(2014,2021))`

    Args:
        seasons (list): Used to define different seasons. 2014 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing rosters available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2014.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2004:
            raise SeasonNotFoundError("season cannot be less than 2004")
        i_data = pl.read_parquet(CFB_ROSTER_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_cfb_team_info(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load college football team info

    Example:
        `cfb_df = sportsdataverse.cfb.load_cfb_team_info(seasons=range(2002,2021))`

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the team info available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        try:
            i_data = pl.read_parquet(CFB_TEAM_INFO_URL.format(season=i), use_pyarrow=True, columns=None)
        except Exception:
            print(f"We don't seem to have data for the {i} season.")
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_cfb_betting_lines(return_as_pandas=False) -> pl.DataFrame:
    """Load college football betting lines information

    Example:
        `cfb_df = sportsdataverse.cfb.load_cfb_betting_lines()`

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing betting lines available for the available seasons.
    """

    return (
        pl.read_parquet(CFB_BETTING_LINES_URL, use_pyarrow=True, columns=None).to_pandas(
            use_pyarrow_extension_array=True
        )
        if return_as_pandas
        else pl.read_parquet(CFB_BETTING_LINES_URL, use_pyarrow=True, columns=None)
    )


def get_cfb_teams(return_as_pandas=False) -> pl.DataFrame:
    """Load college football team ID information and logos

    Example:
        `cfb_df = sportsdataverse.cfb.get_cfb_teams()`

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing teams available.
    """

    return (
        pl.read_parquet(CFB_TEAM_LOGO_URL, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(CFB_TEAM_LOGO_URL, use_pyarrow=True, columns=None)
    )
