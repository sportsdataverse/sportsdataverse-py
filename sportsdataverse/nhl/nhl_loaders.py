from __future__ import annotations

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

    Args:
        seasons (list): Used to define different seasons. 2011 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the play-by-plays available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2011.

    Example:
        Pull a single season's play-by-play parquet::

            from sportsdataverse.nhl import load_nhl_pbp
            pbp = load_nhl_pbp(seasons=2023)
            print(pbp.shape)

        Pull a range of seasons::

            pbp = load_nhl_pbp(seasons=range(2018, 2024))
            pbp.group_by("season").len().sort("season")

        Filter to goal events and round-trip to pandas::

            import polars as pl
            goals = pbp.filter(pl.col("type_text") == "Goal")
            goals_pd = goals.to_pandas()
            goals_pd[["season", "period", "time", "text"]].head()

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2011:
            raise SeasonNotFoundError("season cannot be less than 2011")
        i_data = pl.read_parquet(NHL_BASE_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nhl_schedule(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NHL schedule data

    Args:
        seasons (list): Used to define different seasons. 2002 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the schedule for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Pull a single season's schedule::

            from sportsdataverse.nhl import load_nhl_schedule
            sched = load_nhl_schedule(seasons=2023)
            print(sched.shape)

        Pull a range of seasons and count by status::

            sched = load_nhl_schedule(seasons=range(2018, 2024))
            sched.group_by(["season", "status_type_description"]).len().sort(["season", "len"])

        Pandas round-trip with a single season::

            sched_pd = load_nhl_schedule(seasons=[2023], return_as_pandas=True)
            sched_pd[["game_id", "home_name", "away_name", "game_date"]].head()

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        i_data = pl.read_parquet(NHL_TEAM_SCHEDULE_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nhl_team_boxscore(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NHL team boxscore data

    Args:
        seasons (list): Used to define different seasons. 2011 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        team boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2011.

    Example:
        Pull team box scores for a single season::

            from sportsdataverse.nhl import load_nhl_team_boxscore
            tb = load_nhl_team_boxscore(seasons=2023)
            print(tb.shape)

        Pull a range of seasons::

            tb = load_nhl_team_boxscore(seasons=range(2018, 2024))
            tb.group_by("season").len().sort("season")

        Tampa Bay Lightning (team_id 14) game-by-game scoring::

            import polars as pl
            tb.filter(pl.col("team_id") == 14).select(["game_id", "team_score", "opponent_team_score"]).head()

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2011:
            raise SeasonNotFoundError("season cannot be less than 2011")
        i_data = pl.read_parquet(NHL_TEAM_BOX_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nhl_player_boxscore(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NHL player boxscore data

    Args:
        seasons (list): Used to define different seasons. 2011 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the
        player boxscores available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2011.

    Example:
        Pull player box scores for a single season::

            from sportsdataverse.nhl import load_nhl_player_boxscore
            pb = load_nhl_player_boxscore(seasons=2023)
            print(pb.shape)

        Top 10 single-game point performers::

            import polars as pl
            pb.with_columns(points=pl.col("goals") + pl.col("assists")).sort(
                "points", descending=True
            ).select(["game_id", "athlete_display_name", "goals", "assists", "points"]).head(10)

        Pandas round-trip across multiple seasons::

            pb_pd = load_nhl_player_boxscore(seasons=range(2020, 2024), return_as_pandas=True)
            pb_pd.groupby("season")[["goals", "assists"]].sum()

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]
    for i in tqdm(seasons):
        if int(i) < 2011:
            raise SeasonNotFoundError("season cannot be less than 2011")
        i_data = pl.read_parquet(NHL_PLAYER_BOX_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def nhl_teams(return_as_pandas=False) -> pl.DataFrame:
    """Load NHL team ID information and logos

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing teams available for the requested seasons.

    Example:
        Pull the static teams + logos table::

            from sportsdataverse.nhl import nhl_teams
            teams = nhl_teams()
            print(teams.shape)
            teams.head()

        Pandas round-trip — convenient for joining against your own roster table::

            teams_pd = nhl_teams(return_as_pandas=True)
            list(teams_pd.columns)[:10]

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    return pl.read_csv(NHL_TEAM_LOGO_URL).to_pandas if return_as_pandas else pl.read_csv(NHL_TEAM_LOGO_URL)
