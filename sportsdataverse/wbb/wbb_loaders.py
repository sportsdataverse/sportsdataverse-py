from __future__ import annotations

from typing import List

import polars as pl
from tqdm import tqdm

from sportsdataverse.config import (
    WBB_BASE_URL,
    WBB_PLAYER_BOX_URL,
    WBB_TEAM_BOX_URL,
    WBB_TEAM_SCHEDULE_URL,
)
from sportsdataverse.errors import SeasonNotFoundError


def load_wbb_pbp(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load women's college basketball play by play data going back to 2002

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        play-by-plays available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Single season::

            from sportsdataverse.wbb import load_wbb_pbp
            pbp = load_wbb_pbp(seasons=[2024])
            print(pbp.shape)

        Range of seasons::

            pbp_multi = load_wbb_pbp(seasons=range(2022, 2025))
            print(pbp_multi["season"].unique().sort())

        Pandas round-trip::

            pbp_pd = load_wbb_pbp(seasons=[2024], return_as_pandas=True)
            pbp_pd.head()

        See Also:
            * `wehoop`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        i_data = pl.read_parquet(WBB_BASE_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_wbb_team_boxscore(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load women's college basketball team boxscore data

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        team boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Single season::

            from sportsdataverse.wbb import load_wbb_team_boxscore
            tb = load_wbb_team_boxscore(seasons=[2024])
            print(tb.shape)

        Range of seasons + filter to a specific team::

            import polars as pl
            tb_multi = load_wbb_team_boxscore(seasons=range(2022, 2025))
            uconn = tb_multi.filter(pl.col("team_id") == 41)  # team_id 41 = UConn

        Pandas round-trip::

            tb_pd = load_wbb_team_boxscore(seasons=[2024], return_as_pandas=True)
            tb_pd.head()

        See Also:
            * `wehoop`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise ValueError("season cannot be less than 2002")
        i_data = pl.read_parquet(WBB_TEAM_BOX_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_wbb_player_boxscore(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load women's college basketball player boxscore data

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        player boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Single season::

            from sportsdataverse.wbb import load_wbb_player_boxscore
            pb = load_wbb_player_boxscore(seasons=[2024])
            print(pb.shape)

        Range of seasons + top scorers::

            import polars as pl
            pb_multi = load_wbb_player_boxscore(seasons=range(2022, 2025))
            top = (
                pb_multi
                .group_by("athlete_display_name")
                .agg(pl.col("points").sum().alias("total_points"))
                .sort("total_points", descending=True)
                .head(10)
            )

        Pandas round-trip::

            pb_pd = load_wbb_player_boxscore(seasons=[2024], return_as_pandas=True)
            pb_pd.head()

        See Also:
            * `wehoop`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise ValueError("season cannot be less than 2002")
        i_data = pl.read_parquet(WBB_PLAYER_BOX_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_wbb_schedule(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load women's college basketball schedule data

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        schedule for  the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Single season::

            from sportsdataverse.wbb import load_wbb_schedule
            sched = load_wbb_schedule(seasons=[2024])
            print(sched.shape)

        Range of seasons::

            sched_multi = load_wbb_schedule(seasons=range(2022, 2025))
            print(sched_multi["season"].unique().sort())

        Pandas round-trip::

            sched_pd = load_wbb_schedule(seasons=[2024], return_as_pandas=True)
            sched_pd.head()

        See Also:
            * `wehoop`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise ValueError("season cannot be less than 2002")
        i_data = pl.read_parquet(WBB_TEAM_SCHEDULE_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data
