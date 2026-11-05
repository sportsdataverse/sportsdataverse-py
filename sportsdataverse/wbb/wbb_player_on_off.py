"""Women's college player on/off and WOWY pairs (BB-D5): ``wbb.player_on_off`` / ``wbb.player_wowy``.

Thin shims over :mod:`sportsdataverse.mbb.mbb_player_on_off`: ``league="womens"`` switches to the
``ncaa_wbb_pbp`` / ``ncaa_wbb_possessions`` releases.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_player_on_off import wbb_player_on_off
        on_off = wbb_player_on_off(2025)
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_lineup_seasons import MIN_POSS
from sportsdataverse.mbb.mbb_player_on_off import mbb_player_on_off, mbb_player_wowy

__all__ = ["wbb_player_on_off", "wbb_player_wowy"]


def wbb_player_on_off(
    seasons: int | list[int], *, min_poss: int = MIN_POSS, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Women's college player on/off: each player's team with her on the floor and without her.

    Delegates to :func:`sportsdataverse.mbb.mbb_player_on_off.mbb_player_on_off` with
    ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        min_poss: The rank gate, offensive plus defensive possessions both on and off the floor
            (default 200).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, player_id)``: ``games``, ``team_games``, the ``_on`` and ``_off`` totals
        and ratings, ``<rating>_diff``, the ranks and the sample sizes. ``poss_on + poss_off`` is
        the team's possessions. See the mbb core for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_player_on_off
            wbb_player_on_off(2025).sort("net_rtg_diff_rank").head()
    """
    return mbb_player_on_off(seasons, league="womens", min_poss=min_poss, return_as_pandas=return_as_pandas)


def wbb_player_wowy(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Women's college WOWY pairs: a team's season with two teammates together, apart and both off.

    Delegates to :func:`sportsdataverse.mbb.mbb_player_on_off.mbb_player_wowy` with
    ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, player_id_a, player_id_b)``: the ``_both``, ``_a_only``,
        ``_b_only`` and ``_neither`` totals, which add up to the team's season, and ratings (not
        additive: recompute them from summed totals). See the mbb core for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_player_wowy
            wbb_player_wowy(2025).sort("minutes_both", descending=True).head()
    """
    return mbb_player_wowy(seasons, league="womens", return_as_pandas=return_as_pandas)
