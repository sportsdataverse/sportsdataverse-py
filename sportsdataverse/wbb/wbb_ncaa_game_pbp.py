"""Women's college basketball NCAA play-by-play scrapers (wbigballR port).

Thin shims over :mod:`sportsdataverse.mbb.mbb_ncaa_game_pbp` with the WBB
period model ``(4, 600, 300)`` bound — four 600-second quarters plus
300-second overtimes (2015-16+ format).

**Deliberate fix of wbigballR.** wbigballR ``scrape_game`` applies bigballR's
men's halves math (2 x 1200s) to the women's pages, but the captured WBB pbp
fixtures prove stats.ncaa.org serves one table per QUARTER (line score
``1 2 3 4 S``) — so a regulation WBB game parses as a 2-OT game in R, and
every time-derived column (period, game_seconds, lineups, possessions) is
wrong-by-construction in the R oracle. See
``dev/bigballr_port/design.md`` ("QUARTERS PROVEN from captures") and
``tests/fixtures/ncaa/bigballr/oracle/wbb/README.md``. These shims bind the
correct quarter model; WBB parity is therefore strict on clock-independent
columns and invariant-based on time-derived ones.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence, Union

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_game_pbp import (
    _ncaa_bb_game_pbp,
    _ncaa_bb_play_by_play,
    _SupportsFetchGamePbp,
)

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

__all__ = [
    "ncaa_wbb_game_pbp",
    "ncaa_wbb_play_by_play",
]

#: WBB period model: 4 regulation quarters x 600s, 300s overtimes.
_WBB_PERIOD_MODEL: "tuple[int, int, int]" = (4, 600, 300)


def ncaa_wbb_game_pbp(
    game_id: object,
    *,
    fetcher: Optional[_SupportsFetchGamePbp] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Scrape one WBB game's play-by-play (wbigballR ``scrape_game``, quarters fixed).

    Same engine as :func:`sportsdataverse.mbb.mbb_ncaa_game_pbp.ncaa_mbb_game_pbp`
    with ``period_model=(4, 600, 300)`` bound (see the module docstring for why
    this deliberately diverges from wbigballR's halves math).

    Args:
        game_id: NCAA contest id (e.g. ``"5722355"``).
        fetcher: Optional injected fetcher exposing ``fetch_game_pbp`` (for
            tests/offline use). Defaults to a fresh
            ``NcaaFetcher.with_browser()`` context per call.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The 35-column play-by-play frame (zero rows when the game is not found).

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_game_pbp import ncaa_wbb_game_pbp
            df = ncaa_wbb_game_pbp("5722355")
            print(df.shape)
    """
    return _ncaa_bb_game_pbp(
        game_id,
        fetcher=fetcher,
        period_model=_WBB_PERIOD_MODEL,
        return_as_pandas=return_as_pandas,
    )


def ncaa_wbb_play_by_play(
    game_ids: Sequence[object],
    *,
    fetcher: Optional[_SupportsFetchGamePbp] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Scrape many WBB games' play-by-play (wbigballR ``get_play_by_play``, quarters fixed).

    Same driver as :func:`sportsdataverse.mbb.mbb_ncaa_game_pbp.ncaa_mbb_play_by_play`
    (drop missing ids, shared fetcher session, one retry per empty scrape) with
    the WBB quarter model ``(4, 600, 300)`` bound.

    Args:
        game_ids: NCAA contest ids; ``None``/NaN entries are dropped.
        fetcher: Optional injected fetcher exposing ``fetch_game_pbp``.
            Defaults to one shared ``NcaaFetcher.with_browser()`` context.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Row-bound play-by-play for every game that scraped successfully
        (zero-row contract frame when none did).

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_game_pbp import ncaa_wbb_play_by_play
            df = ncaa_wbb_play_by_play(["5722355", "5732292"])
            print(df.shape)
    """
    return _ncaa_bb_play_by_play(
        game_ids,
        fetcher=fetcher,
        period_model=_WBB_PERIOD_MODEL,
        return_as_pandas=return_as_pandas,
    )
