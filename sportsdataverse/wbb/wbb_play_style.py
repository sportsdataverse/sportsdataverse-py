"""Women's college player play-style archetypes (BB-D20): ``wbb_play_styles``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_play_style`: ``league="womens"`` reads the women's box and shots
releases and the bundled ``wbb_play_style_player.json`` centers.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_play_style import wbb_play_styles
        styles = wbb_play_styles(2026)
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_play_style import mbb_play_styles

__all__ = ["wbb_play_styles"]


def wbb_play_styles(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Women's college player play-style archetypes: box, shot and lineup-role features (BB-D20).

    Delegates to :func:`sportsdataverse.mbb.mbb_play_style.mbb_play_styles` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons (2025+).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per qualifying ``(player_id, season, team_id)``: the ESPN ids, ``player``, ``min`` and the
        ``archetype``, ``cluster``, ``dist_to_center``, ``second_archetype`` and ``margin``.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_play_styles
            wbb_play_styles(2026).group_by("archetype").len()
    """
    return mbb_play_styles(seasons, league="womens", return_as_pandas=return_as_pandas)
