"""NFL game story: how the winner won it, in expected points.

Binds the football adapter of :mod:`sportsdataverse.cfb.cfb_game_story` (the edge list, the moment
and performer rules are documented there) to the NFL's ESPN-processed play frame: ``NFLPlayProcess``
output and the ``espn_nfl_pbp`` release, which share the college frame's columns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse.cfb.cfb_game_story import _football_game_story

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nfl_game_story"]


def nfl_game_story(
    pbp: pl.DataFrame, *, return_as_pandas: bool = False
) -> dict[str, pl.DataFrame] | dict[str, pd.DataFrame]:
    """How the winner won it, for one or many NFL games: edges in EPA, the moment, periods, runs, performers.

    Args:
        pbp: The ESPN-processed plays of one or many games: the ``espn_nfl_pbp`` release or
            ``NFLPlayProcess(...).run_processing_pipeline()``'s plays frame (the columns
            :func:`~sportsdataverse.cfb.cfb_game_story.cfb_game_story` lists; the NFL frame has no
            ``homeFinalScore``, so the final is the last running score).
        return_as_pandas: Return pandas frames instead of polars.

    Returns:
        ``{"summary", "edges", "moment", "periods", "runs", "performers"}``, the frames of
        :data:`~sportsdataverse._common.game_story.GAME_STORY_SCHEMAS`: the accounting phases
        (``passing``, ``rushing``, ``special_teams``, ``other``) and splits (``turnovers``,
        ``explosive_plays``, ``success_rate``, ``red_zone``, ``late_downs``) in expected points,
        ranked; the largest-``|wpa|`` play; quarters and ``OT``. A tie (``decided`` false) has null
        winner ids.

    Raises:
        polars.exceptions.ColumnNotFoundError: If ``pbp`` lacks a column the football adapter reads.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nfl.nfl_game_story import nfl_game_story
            url = (
                "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
                "espn_nfl_pbp/play_by_play_2025.parquet"
            )
            pbp = pl.read_parquet(url, use_pyarrow=True)
            story = nfl_game_story(pbp.filter(pl.col("game_id") == pbp["game_id"].max()))
            story["edges"].head(3)

        Every game's moment::

            nfl_game_story(pbp)["moment"].select("game_id", "text", "winner_wpa")

        See Also:
            * `nflfastR`_ -- the R companion for NFL play-by-play
            * `nflverse`_ -- the NFL data ecosystem

        .. _nflfastR: https://www.nflfastr.com
        .. _nflverse: https://nflverse.nflverse.com
    """
    story = _football_game_story(pbp)
    if return_as_pandas:
        return {k: v.to_pandas() for k, v in story.items()}
    return story
