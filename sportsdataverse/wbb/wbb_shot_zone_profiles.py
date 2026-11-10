"""Women's college season shot profile by zone (BB-D35): ``wbb.player_shot_zones`` / ``team_shot_zones``.

Thin shims over :mod:`sportsdataverse.mbb.mbb_shot_zone_profiles`: ``league="womens"`` switches to
the ``ncaa_wbb_shots`` and ``ncaa_wbb_pbp`` releases.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_shot_zone_profiles import wbb_player_shot_zones
        zones = wbb_player_shot_zones(2025)
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_shot_zone_profiles import mbb_player_shot_zones, mbb_team_shot_zones

__all__ = ["wbb_player_shot_zones", "wbb_team_shot_zones"]


def wbb_player_shot_zones(
    seasons: int | list[int],
    *,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Women's college season shot profile by zone per player: rim / mid / corner 3 / above-the-break 3.

    Delegates to :func:`sportsdataverse.mbb.mbb_shot_zone_profiles.mbb_player_shot_zones` with
    ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, player_id)``: ``games``, ``fga``, ``fgm``,
        ``unlocated_fga``, each zone's attempts, makes, assisted makes, ``_fg_pct`` / ``_freq`` /
        ``_ast_pct`` and the ``_n`` / ``_rank`` / ``_pct`` columns. See the mbb core for the
        definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_player_shot_zones
            wbb_player_shot_zones(2025).sort("rim_fga", descending=True).head()
    """
    return mbb_player_shot_zones(seasons, league="womens", return_as_pandas=return_as_pandas)


def wbb_team_shot_zones(
    seasons: int | list[int],
    *,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Women's college season shot profile by zone per team, for and allowed.

    Delegates to :func:`sportsdataverse.mbb.mbb_shot_zone_profiles.mbb_team_shot_zones` with
    ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id)``: ``games``, the team's zone profile and its opponents'
        (``opp_``), with the ``_n`` / ``_rank`` / ``_pct`` columns. See the mbb core for the
        definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_team_shot_zones
            wbb_team_shot_zones(2025).sort("opp_rim_freq").head()
    """
    return mbb_team_shot_zones(seasons, league="womens", return_as_pandas=return_as_pandas)
