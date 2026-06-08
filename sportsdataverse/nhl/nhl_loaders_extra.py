"""Hand-written NHL loaders preserved alongside the generated
`sportsdataverse.nhl.nhl_loaders`: naming-parity aliases, a season-less
single-file loader, and a teams helper that the season-loop loader template
can't express.
"""

from __future__ import annotations

from typing import List  # noqa: F401

import polars as pl

from sportsdataverse._codegen_runtime import _read_release_parquet, cli_warn
from sportsdataverse.config import (
    NHL_TEAM_LOGO_URL,
)
from sportsdataverse.nhl.nhl_loaders import (
    load_nhl_goalie_boxscores,
    load_nhl_player_boxscore,
    load_nhl_skater_boxscores,
    load_nhl_team_boxscore,
)

__all__ = [
    "nhl_teams",
    # fastRhockey (R) naming-parity aliases
    "load_nhl_team_box",
    "load_nhl_player_box",
    "load_nhl_skater_box",
    "load_nhl_goalie_box",
    # fastRhockey (R) games-manifest loader
    "load_nhl_games",
]


# ---------------------------------------------------------------------------
# fastRhockey (R) naming-parity aliases
# ---------------------------------------------------------------------------
# fastRhockey (R) uses ``load_nhl_team_box`` / ``load_nhl_player_box`` /
# ``load_nhl_skater_box`` / ``load_nhl_goalie_box`` as canonical names; sdv-py
# generated the plural-and-full ``load_nhl_team_boxscore`` /
# ``load_nhl_player_boxscore`` / ``load_nhl_skater_boxscores`` /
# ``load_nhl_goalie_boxscores`` forms. These thin wrappers bridge the gap so
# R users migrating to Python find the expected names without renaming anything.


def load_nhl_team_box(seasons, return_as_pandas: bool = False):
    """Alias of load_nhl_team_boxscore() for naming parity with fastRhockey (R)."""
    return load_nhl_team_boxscore(seasons, return_as_pandas=return_as_pandas)


def load_nhl_player_box(seasons, return_as_pandas: bool = False):
    """Alias of load_nhl_player_boxscore() for naming parity with fastRhockey (R)."""
    return load_nhl_player_boxscore(seasons, return_as_pandas=return_as_pandas)


def load_nhl_skater_box(seasons, return_as_pandas: bool = False):
    """Alias of load_nhl_skater_boxscores() for naming parity with fastRhockey (R)."""
    return load_nhl_skater_boxscores(seasons, return_as_pandas=return_as_pandas)


def load_nhl_goalie_box(seasons, return_as_pandas: bool = False):
    """Alias of load_nhl_goalie_boxscores() for naming parity with fastRhockey (R)."""
    return load_nhl_goalie_boxscores(seasons, return_as_pandas=return_as_pandas)


# ---------------------------------------------------------------------------
# fastRhockey (R) games-in-data-repo manifest loader
# ---------------------------------------------------------------------------


def load_nhl_games(return_as_pandas: bool = False):
    """Load the NHL games-in-data-repo manifest (no ``seasons`` argument).

    Mirrors fastRhockey (R) ``load_nhl_games()`` which reads a manifest of every
    NHL game that has processed data in the data repository.

    Tries the sportsdataverse-data release asset first; falls back to the raw
    fastRhockey-data GitHub path.

    Args:
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame of all games in the data repository.

    Example:
        >>> load_nhl_games()
    """
    primary = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_schedules/nhl_games_in_data_repo.parquet"
    fallback = (
        "https://raw.githubusercontent.com/sportsdataverse/fastRhockey-data/main/nhl/nhl_games_in_data_repo.parquet"
    )

    df = _read_release_parquet(primary)
    if df is None:
        df = _read_release_parquet(fallback)
    if df is None:
        cli_warn("load_nhl_games: manifest parquet not found at primary or fallback URL")
        df = pl.DataFrame()

    return df.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else df


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
