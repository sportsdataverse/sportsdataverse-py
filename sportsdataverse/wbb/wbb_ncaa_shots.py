"""Women's college basketball shot locations from stats.ncaa.org.

WBB binding of :mod:`sportsdataverse.mbb.mbb_ncaa_shots` with the WBB
``period_model`` from :func:`~sportsdataverse.scrape.ncaa.parse.wbb_period_model` --
quarters by default, halves when ``season=`` names a pre-2016 season
(rules/wbb.yaml#wbb-2016-four-quarters). This surface is a deliberate EXTENSION:
wbigballR ships ``get_shot_locations``/``join_pbp_shots`` but exports
neither (``dev/bigballr_port/design.md``); the shared parser is validated on
real WBB captures (100% chart→pbp join match on the fixture games — only
possible with correct quarter clock math).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional, Sequence, Union

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_shots import _shot_locations, ncaa_mbb_join_pbp_shots
from sportsdataverse.scrape.ncaa.identity import _utf8_id
from sportsdataverse.scrape.ncaa.parse import wbb_period_model

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

logger = logging.getLogger(__name__)

__all__ = [
    "ncaa_wbb_join_pbp_shots",
    "ncaa_wbb_shot_locations",
]


def ncaa_wbb_shot_locations(
    game_ids: Sequence[object],
    *,
    season: Optional[object] = None,
    fetcher: Optional[Any] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Scrape WBB shot locations for one or more games.

    Same driver as
    :func:`sportsdataverse.mbb.mbb_ncaa_shots.ncaa_mbb_shot_locations` with
    the WBB ``period_model`` for the game's era bound — see the mbb sibling for
    the parse algorithm and the :data:`~sportsdataverse.mbb.mbb_ncaa_shots.SHOTS_SCHEMA`
    contract. The default is the quarters model: stats.ncaa.org shot locations
    only exist from 2019 (rules/wbb.yaml#wbb-2019-ncaa-stats-shot-locations),
    so unlike :func:`~sportsdataverse.wbb.wbb_ncaa_game_pbp.ncaa_wbb_game_pbp`
    there is no page inference; ``season`` resolves the model through
    :func:`sportsdataverse.scrape.ncaa.parse.wbb_period_model` (halves through
    2014-15, quarters from 2015-16, rules/wbb.yaml#wbb-2016-four-quarters).

    Args:
        game_ids: NCAA contest ids; ``None``/NaN entries are dropped.
        season: Season as an ending year (``2015``) or span (``"2014-15"``);
            applies to every game in ``game_ids``. ``None`` binds quarters.
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
    # _utf8_id, never str(): season=2015.0 must read "2015" (halves), not "2015.0" (quarters)
    period_model = wbb_period_model(_utf8_id(season))
    return _shot_locations(game_ids, period_model, fetcher, return_as_pandas)


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
