"""Women's college EPV and EPV added (BB-D24): ``wbb.epv_plays`` / ``_player_games`` / ``_lineup_games`` / ``_team_games``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_epv`: ``league="womens"`` switches to the
``ncaa_wbb_pbp`` release and the women's booster.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_epv import wbb_epv_team_games
        teams = wbb_epv_team_games(2026)
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_epv import mbb_epv_lineup_games, mbb_epv_player_games, mbb_epv_plays, mbb_epv_team_games

__all__ = ["wbb_epv_lineup_games", "wbb_epv_player_games", "wbb_epv_plays", "wbb_epv_team_games"]


def wbb_epv_plays(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """EPV and EPV added on every women's college play-by-play event inside a possession.

    Delegates to :func:`sportsdataverse.mbb.mbb_epv.mbb_epv_plays` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            from the booster's first training season (2022) on.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        The columns of :func:`sportsdataverse.mbb.mbb_epv.mbb_epv_plays`.

    Raises:
        ValueError: If a season is before the first one served.

    Example:
        The biggest plays of a season::

            from sportsdataverse.wbb import wbb_epv_plays
            wbb_epv_plays(2026).sort("epv_added", descending=True).head()
    """
    return mbb_epv_plays(seasons, league="womens", return_as_pandas=return_as_pandas)


def wbb_epv_player_games(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Per women's college player and game: EPV added on her own actions and on-court EPV +/-.

    Delegates to :func:`sportsdataverse.mbb.mbb_epv.mbb_epv_player_games` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            from the booster's first training season (2022) on.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        The columns of :func:`sportsdataverse.mbb.mbb_epv.mbb_epv_player_games`.

    Raises:
        ValueError: If a season is before the first one served.

    Example:
        A season's EPV boxes::

            from sportsdataverse.wbb import wbb_epv_player_games
            wbb_epv_player_games(2026).sort("epv_on", descending=True).head()
    """
    return mbb_epv_player_games(seasons, league="womens", return_as_pandas=return_as_pandas)


def wbb_epv_lineup_games(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Per women's college five-woman unit and game: the EPV added with the unit on the floor.

    Delegates to :func:`sportsdataverse.mbb.mbb_epv.mbb_epv_lineup_games` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            from the booster's first training season (2022) on.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        The columns of :func:`sportsdataverse.mbb.mbb_epv.mbb_epv_lineup_games`.

    Raises:
        ValueError: If a season is before the first one served.

    Example:
        A season's units::

            from sportsdataverse.wbb import wbb_epv_lineup_games
            wbb_epv_lineup_games(2026).sort("net_epv", descending=True).head()
    """
    return mbb_epv_lineup_games(seasons, league="womens", return_as_pandas=return_as_pandas)


def wbb_epv_team_games(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Per women's college team and game: EPV added on offense and allowed on defense.

    Delegates to :func:`sportsdataverse.mbb.mbb_epv.mbb_epv_team_games` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            from the booster's first training season (2022) on.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        The columns of :func:`sportsdataverse.mbb.mbb_epv.mbb_epv_team_games`.

    Raises:
        ValueError: If a season is before the first one served.

    Example:
        A season's offenses::

            import polars as pl
            from sportsdataverse.wbb import wbb_epv_team_games
            wbb_epv_team_games(2026).group_by("team_id").agg(pl.col("off_epv_per_poss").mean())
    """
    return mbb_epv_team_games(seasons, league="womens", return_as_pandas=return_as_pandas)
