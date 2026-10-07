"""Hand-written PWHL loaders preserved alongside the generated
`sportsdataverse.pwhl.pwhl_loaders`: naming-parity aliases and a season-less
single-file loader that the season-loop loader template can't express.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse._codegen_runtime import _read_release_parquet, cli_warn
from sportsdataverse.pwhl.pwhl_loaders import (
    load_pwhl_goalie_boxscores,
    load_pwhl_player_boxscores,
    load_pwhl_schedules,
    load_pwhl_skater_boxscores,
    load_pwhl_team_boxscores,
)

__all__ = [
    # fastRhockey (R) naming-parity aliases
    "load_pwhl_team_box",
    "load_pwhl_player_box",
    "load_pwhl_skater_box",
    "load_pwhl_goalie_box",
    "load_pwhl_schedule",
    # fastRhockey (R) games-manifest loader
    "load_pwhl_games",
]


# ---------------------------------------------------------------------------
# fastRhockey (R) naming-parity aliases
# ---------------------------------------------------------------------------
# fastRhockey (R) uses ``load_pwhl_team_box`` / ``load_pwhl_player_box`` /
# ``load_pwhl_skater_box`` / ``load_pwhl_goalie_box`` / ``load_pwhl_schedule``
# as canonical names; sdv-py generated the plural/full ``_boxscores`` /
# ``_schedules`` forms. These thin wrappers bridge the gap so R users
# migrating to Python find the expected names without renaming anything.


def load_pwhl_team_box(seasons, return_as_pandas: bool = False):
    """Alias of load_pwhl_team_boxscores() for naming parity with fastRhockey (R).

    Args:
        seasons: A season or list of seasons, as END years (2026 = the 2025-26 season).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: Exactly what :func:`load_pwhl_team_boxscores` returns -- one row per team per game, the
            requested seasons stacked; see its Returns table for the columns. A season with no
            published asset is skipped with a warning. A pandas DataFrame when
            ``return_as_pandas`` is True.
    """
    return load_pwhl_team_boxscores(seasons, return_as_pandas=return_as_pandas)


def load_pwhl_player_box(seasons, return_as_pandas: bool = False):
    """Alias of load_pwhl_player_boxscores() for naming parity with fastRhockey (R).

    Args:
        seasons: A season or list of seasons, as END years (2026 = the 2025-26 season).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: Exactly what :func:`load_pwhl_player_boxscores` returns -- one row per player per game, the
            requested seasons stacked; see its Returns table for the columns. A season with no
            published asset is skipped with a warning. A pandas DataFrame when
            ``return_as_pandas`` is True.
    """
    return load_pwhl_player_boxscores(seasons, return_as_pandas=return_as_pandas)


def load_pwhl_skater_box(seasons, return_as_pandas: bool = False):
    """Alias of load_pwhl_skater_boxscores() for naming parity with fastRhockey (R).

    Args:
        seasons: A season or list of seasons, as END years (2026 = the 2025-26 season).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: Exactly what :func:`load_pwhl_skater_boxscores` returns -- one row per skater per game, the
            requested seasons stacked; see its Returns table for the columns. A season with no
            published asset is skipped with a warning. A pandas DataFrame when
            ``return_as_pandas`` is True.
    """
    return load_pwhl_skater_boxscores(seasons, return_as_pandas=return_as_pandas)


def load_pwhl_goalie_box(seasons, return_as_pandas: bool = False):
    """Alias of load_pwhl_goalie_boxscores() for naming parity with fastRhockey (R).

    Args:
        seasons: A season or list of seasons, as END years (2026 = the 2025-26 season).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: Exactly what :func:`load_pwhl_goalie_boxscores` returns -- one row per goalie per game, the
            requested seasons stacked; see its Returns table for the columns. A season with no
            published asset is skipped with a warning. A pandas DataFrame when
            ``return_as_pandas`` is True.
    """
    return load_pwhl_goalie_boxscores(seasons, return_as_pandas=return_as_pandas)


def load_pwhl_schedule(seasons, return_as_pandas: bool = False):
    """Alias of load_pwhl_schedules() for naming parity with fastRhockey (R).

    Args:
        seasons: A season or list of seasons, as END years (2026 = the 2025-26 season).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: Exactly what :func:`load_pwhl_schedules` returns -- one row per game, the
            requested seasons stacked; see its Returns table for the columns. A season with no
            published asset is skipped with a warning. A pandas DataFrame when
            ``return_as_pandas`` is True.
    """
    return load_pwhl_schedules(seasons, return_as_pandas=return_as_pandas)


# ---------------------------------------------------------------------------
# fastRhockey (R) games-in-data-repo manifest loader
# ---------------------------------------------------------------------------


def load_pwhl_games(return_as_pandas: bool = False):
    """Load the PWHL games-in-data-repo manifest (no ``seasons`` argument).

    Mirrors fastRhockey (R) ``load_pwhl_games()`` which reads a manifest of every
    PWHL game that has processed data in the data repository.

    Tries the sportsdataverse-data release asset first; falls back to the raw
    fastRhockey-data GitHub path.

    Args:
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame of all games in the data repository.

    Example:
        Quick start::

            load_pwhl_games()
    """
    primary = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/pwhl_schedules/pwhl_games_in_data_repo.parquet"
    fallback = (
        "https://raw.githubusercontent.com/sportsdataverse/fastRhockey-data/main/pwhl/pwhl_games_in_data_repo.parquet"
    )

    df = _read_release_parquet(primary)
    if df is None:
        df = _read_release_parquet(fallback)
    if df is None:
        cli_warn("load_pwhl_games: manifest parquet not found at primary or fallback URL")
        df = pl.DataFrame()

    return df.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else df
