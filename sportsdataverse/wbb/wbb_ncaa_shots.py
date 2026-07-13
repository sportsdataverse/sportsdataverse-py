"""Women's college basketball shot locations from stats.ncaa.org.

WBB binding of :mod:`sportsdataverse.mbb.mbb_ncaa_shots` with the quarters
``period_model=(4, 600, 300)``. This surface is a deliberate EXTENSION:
wbigballR ships ``get_shot_locations``/``join_pbp_shots`` but exports
neither (``dev/bigballr_port/design.md``); the shared parser is validated on
real WBB captures (100% chart→pbp join match on the fixture games — only
possible with correct quarter clock math).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional, Sequence, Union

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_shots import (
    _empty_shots,
    ncaa_mbb_join_pbp_shots,
    parse_ncaa_bb_shots,
)

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

logger = logging.getLogger(__name__)

_WBB_PERIOD_MODEL = (4, 600, 300)

__all__ = [
    "ncaa_wbb_join_pbp_shots",
    "ncaa_wbb_shot_locations",
]


def ncaa_wbb_shot_locations(
    game_ids: Sequence[object],
    *,
    fetcher: Optional[Any] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Scrape WBB shot locations for one or more games.

    Same driver as
    :func:`sportsdataverse.mbb.mbb_ncaa_shots.ncaa_mbb_shot_locations` with
    the quarters ``period_model`` bound — see the mbb sibling for the parse
    algorithm and the :data:`~sportsdataverse.mbb.mbb_ncaa_shots.SHOTS_SCHEMA`
    contract.

    Args:
        game_ids: NCAA contest ids; ``None``/NaN entries are dropped.
        fetcher: Optional injected fetcher exposing ``fetch_game_box``
            (tests/offline). Defaults to a fresh
            ``NcaaFetcher.with_browser()`` context per call.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        All games' shots row-bound (zero-row schema frame when none found).

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_shots import ncaa_wbb_shot_locations
            shots = ncaa_wbb_shot_locations(["5722355"])
            print(shots.shape)
    """
    ids = [g for g in game_ids if g is not None and g == g]

    def _run(f: Any) -> "list[pl.DataFrame]":
        frames: "list[pl.DataFrame]" = []
        for gid in ids:
            df = parse_ncaa_bb_shots(f.fetch_game_box(gid), str(gid), period_model=_WBB_PERIOD_MODEL)
            found = sorted({t for t in df["team"].to_list() if t is not None})
            logger.info(
                "Game_ID: %s || %s v. %s || %d shots found",
                gid,
                found[0] if found else None,
                found[1] if len(found) > 1 else None,
                df.height,
            )
            frames.append(df)
        return frames

    if fetcher is None:
        from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

        with NcaaFetcher.with_browser() as browser_fetcher:
            frames = _run(browser_fetcher)
    else:
        frames = _run(fetcher)

    out = pl.concat(frames) if frames else _empty_shots()
    return out.to_pandas() if return_as_pandas else out


def ncaa_wbb_join_pbp_shots(
    pbp: pl.DataFrame,
    shots: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Attach WBB chart shots onto the pbp frame (pure delegation).

    See :func:`sportsdataverse.mbb.mbb_ncaa_shots.ncaa_mbb_join_pbp_shots`
    for the matching rules (FG-only, within-second same-result sequence) and
    the joined 40-column contract.

    Args:
        pbp: 35-column snake_case pbp frame (``ncaa_wbb_play_by_play``).
        shots: Shots frame from :func:`ncaa_wbb_shot_locations`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The pbp frame with shot columns attached (unmatched rows NA-filled).

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_shots import ncaa_wbb_join_pbp_shots
            joined = ncaa_wbb_join_pbp_shots(pbp, shots)
            print(joined.shape)
    """
    return ncaa_mbb_join_pbp_shots(pbp, shots, return_as_pandas=return_as_pandas)
