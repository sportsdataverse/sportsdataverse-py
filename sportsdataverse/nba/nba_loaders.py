from __future__ import annotations

from typing import List

import polars as pl
from tqdm import tqdm

from sportsdataverse.config import (
    NBA_BASE_URL,
    NBA_PLAYER_BOX_URL,
    NBA_TEAM_BOX_URL,
    NBA_TEAM_SCHEDULE_URL,
)
from sportsdataverse.errors import SeasonNotFoundError


def load_nba_pbp(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NBA play by play data going back to 2002

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        play-by-plays available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_pbp
            pbp = load_nba_pbp(seasons=[2023])
            print(pbp.shape)

        Multi-season pull as pandas::

            pbp_pd = load_nba_pbp(seasons=range(2020, 2024), return_as_pandas=True)
            pbp_pd.head()

        Pipeline next step (filter to made 3-pointers)::

            import polars as pl
            threes = load_nba_pbp(seasons=[2023]).filter(
                pl.col("type_text") == "3PT Field Goal"
            )

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for NBA data
            * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball parallel
            * `nba_api <https://github.com/swar/nba_api>`_ -- Python alternative to the NBA Stats API
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        i_data = pl.read_parquet(NBA_BASE_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nba_team_boxscore(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NBA team boxscore data

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        team boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_team_boxscore
            box = load_nba_team_boxscore(seasons=[2023])
            print(box.shape)

        Pandas round-trip::

            box_pd = load_nba_team_boxscore(seasons=[2023], return_as_pandas=True)
            box_pd.head()

        Pipeline next step (compute average team OFF rating)::

            import polars as pl
            avg = (
                load_nba_team_boxscore(seasons=[2023])
                .group_by("team_display_name")
                .agg(pl.col("offensive_rating").mean())
            )

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for NBA data
            * `nba_api <https://github.com/swar/nba_api>`_ -- Python alternative to the NBA Stats API
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        i_data = pl.read_parquet(NBA_TEAM_BOX_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nba_player_boxscore(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NBA player boxscore data

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        player boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_player_boxscore
            box = load_nba_player_boxscore(seasons=[2023])
            print(box.shape)

        Pandas round-trip::

            box_pd = load_nba_player_boxscore(seasons=[2023], return_as_pandas=True)
            box_pd.head()

        Pipeline next step (top season scorers)::

            import polars as pl
            top = (
                load_nba_player_boxscore(seasons=[2023])
                .group_by("athlete_display_name")
                .agg(pl.col("points").sum())
                .sort("points", descending=True)
                .head(10)
            )

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for NBA data
            * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball parallel
            * `nba_api <https://github.com/swar/nba_api>`_ -- Python alternative to the NBA Stats API
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        i_data = pl.read_parquet(NBA_PLAYER_BOX_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nba_schedule(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NBA schedule data

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        schedule for  the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_schedule
            sched = load_nba_schedule(seasons=[2023])
            print(sched.shape)

        Pandas round-trip::

            sched_pd = load_nba_schedule(seasons=range(2020, 2024), return_as_pandas=True)
            sched_pd.head()

        Pipeline next step (filter to playoff games)::

            import polars as pl
            playoffs = load_nba_schedule(seasons=[2023]).filter(pl.col("season_type") == 3)

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for NBA data
            * `nba_api <https://github.com/swar/nba_api>`_ -- Python alternative to the NBA Stats API
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        i_data = pl.read_parquet(NBA_TEAM_SCHEDULE_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data
