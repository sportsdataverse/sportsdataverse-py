from __future__ import annotations

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

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        play-by-plays available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Pull a single season's play-by-play parquet::

            from sportsdataverse.wnba import load_wnba_pbp
            pbp = load_wnba_pbp(seasons=2024)
            print(pbp.shape)

        Pull a range of seasons (closed-open like Python ``range``)::

            pbp = load_wnba_pbp(seasons=range(2020, 2025))
            pbp.group_by("season").len().sort("season")

        Pandas round-trip and a quick filter on play type::

            pbp_pd = load_wnba_pbp(seasons=[2024], return_as_pandas=True)
            pbp_pd[pbp_pd["type_text"] == "JumpShot"].head()

        See Also:
            * `wehoop`_ — R sister package; mirrors this surface
            * `nba_api`_ — alternative Python source for NBA/WNBA stats endpoints
            * `hoopR`_ — companion R package for men's basketball

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
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

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        team boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Pull team box scores for a single season::

            from sportsdataverse.wnba import load_wnba_team_boxscore
            tb = load_wnba_team_boxscore(seasons=2024)
            print(tb.shape)

        Pull a range of seasons::

            tb = load_wnba_team_boxscore(seasons=range(2020, 2025))
            tb.group_by("season").len().sort("season")

        Aces (team_id 17) game-by-game scoring::

            import polars as pl
            tb.filter(pl.col("team_id") == 17).select(["game_id", "team_score", "opponent_team_score"]).head()

        See Also:
            * `wehoop`_ — R sister package; mirrors this surface
            * `nba_api`_ — alternative Python source for NBA/WNBA stats endpoints
            * `hoopR`_ — companion R package for men's basketball

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
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

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        player boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Pull player box scores for a single season::

            from sportsdataverse.wnba import load_wnba_player_boxscore
            pb = load_wnba_player_boxscore(seasons=2024)
            print(pb.shape)

        A'ja Wilson (athlete_id 3149391) game-by-game scoring::

            import polars as pl
            wilson = pb.filter(pl.col("athlete_id") == 3149391)
            wilson.select(["game_id", "minutes", "points", "rebounds", "assists"]).head()

        Pandas round-trip across multiple seasons::

            pb_pd = load_wnba_player_boxscore(seasons=range(2022, 2025), return_as_pandas=True)
            pb_pd.groupby("season")["points"].mean()

        See Also:
            * `wehoop`_ — R sister package; mirrors this surface
            * `nba_api`_ — alternative Python source for NBA/WNBA stats endpoints
            * `hoopR`_ — companion R package for men's basketball

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
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

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        schedule for  the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Pull a single season's schedule::

            from sportsdataverse.wnba import load_wnba_schedule
            sched = load_wnba_schedule(seasons=2024)
            print(sched.shape)

        Pull a range of seasons and count by status::

            sched = load_wnba_schedule(seasons=range(2020, 2025))
            sched.group_by(["season", "status_type_description"]).len().sort(["season", "len"])

        Pandas round-trip with a single season::

            sched_pd = load_wnba_schedule(seasons=[2024], return_as_pandas=True)
            sched_pd[["game_id", "home_name", "away_name", "game_date"]].head()

        See Also:
            * `wehoop`_ — R sister package; mirrors this surface
            * `nba_api`_ — alternative Python source for NBA/WNBA stats endpoints
            * `hoopR`_ — companion R package for men's basketball

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
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
