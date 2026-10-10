"""Women's college basketball NCAA play-by-play scrapers (wbigballR port).

Thin shims over :mod:`sportsdataverse.mbb.mbb_ncaa_game_pbp` that bind the
**right WBB period model for the game's era**:

* ``(2, 1200, 300)`` -- two 20-minute halves, seasons through 2014-15;
* ``(4, 600, 300)`` -- four 10-minute quarters, 2015-16 onward
  (``rules/wbb.yaml#wbb-2016-four-quarters`` in sdv-internal-refs).

Feeding the wrong model to the parser does not raise -- it silently yields a
zero-row frame -- so the era is resolved before parsing, either from the
``season`` the caller passes (:func:`sportsdataverse.scrape.ncaa.parse.wbb_period_model`)
or, when no season is given, from the page itself
(:func:`infer_wbb_period_model`: a first-period clock above 10:00 can only
occur in a halves game).

**Deliberate fix of wbigballR.** wbigballR ``scrape_game`` applies bigballR's
men's halves math (2 x 1200s) to every women's page, so a 2016+ regulation
game parses as a 2-OT game in R and every time-derived column is
wrong-by-construction in the R oracle; see ``dev/bigballr_port/design.md``
and ``tests/fixtures/ncaa/bigballr/oracle/wbb/README.md``. Until 2026-10 these
shims had the inverse defect (quarters bound unconditionally, so every
pre-2016 game parsed to zero rows); the catalog entry above records both.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence, Union

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_game_pbp import (
    PeriodModel,
    _extract_tables,
    _ncaa_bb_game_pbp,
    _ncaa_bb_play_by_play,
    _SupportsFetchGamePbp,
    _time_in_seconds,
)
from sportsdataverse.scrape.ncaa.parse import wbb_period_model

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

__all__ = [
    "infer_wbb_period_model",
    "ncaa_wbb_game_pbp",
    "ncaa_wbb_play_by_play",
]

#: WBB quarters model (2015-16+): 4 regulation quarters x 600s, 300s overtimes.
_WBB_PERIOD_MODEL: "tuple[int, int, int]" = (4, 600, 300)
#: WBB halves model (through 2014-15): 2 regulation halves x 1200s, 300s overtimes.
_WBB_HALVES_MODEL: "tuple[int, int, int]" = (2, 1200, 300)


def infer_wbb_period_model(html: str) -> "tuple[int, int, int]":
    """Tell a halves-era WBB page from a quarters-era one by its first-period clock.

    stats.ncaa.org serves one table per period after the line score; the first
    period table's clock column runs down from 20:00 in a halves game and from
    10:00 in a quarters game, so any clock above 600 seconds identifies the
    halves era. The line-score width alone cannot (halves + 2 OT and quarters
    + 0 OT both have six columns).

    Args:
        html: The raw play-by-play page.

    Returns:
        ``(2, 1200, 300)`` when a first-period clock above 10:00 is present,
        otherwise ``(4, 600, 300)`` (an unreadable page falls through to the
        modern model, matching :func:`~sportsdataverse.scrape.ncaa.parse.wbb_period_model`).

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_game_pbp import infer_wbb_period_model
            infer_wbb_period_model(open("pbp_1613299.html").read())  # (2, 1200, 300)
    """
    tables = _extract_tables(html)
    if len(tables) < 4:
        return _WBB_PERIOD_MODEL
    for row in tables[3][1:]:
        secs = _time_in_seconds((row[0] or "")[:5]) if row else None
        if secs is not None and secs > 600:
            return _WBB_HALVES_MODEL
    return _WBB_PERIOD_MODEL


def _period_model_for(season: Optional[object]) -> PeriodModel:
    return infer_wbb_period_model if season is None else wbb_period_model(str(season))


def ncaa_wbb_game_pbp(
    game_id: object,
    *,
    season: Optional[object] = None,
    fetcher: Optional[_SupportsFetchGamePbp] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Scrape one WBB game's play-by-play (wbigballR ``scrape_game``, era-aware).

    Same engine as :func:`sportsdataverse.mbb.mbb_ncaa_game_pbp.ncaa_mbb_game_pbp`
    with the WBB period model for the game's era bound: halves through 2014-15,
    quarters from 2015-16 (see the module docstring).

    Args:
        game_id: NCAA contest id (e.g. ``"5722355"``).
        season: Season as an ending year (``2015``) or span (``"2014-15"``).
            When omitted the era is inferred from the page's first-period clock
            (:func:`infer_wbb_period_model`).
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

        Pin the era explicitly for a 2014-15 (halves) game::

            df = ncaa_wbb_game_pbp("1613299", season=2015)
    """
    return _ncaa_bb_game_pbp(
        game_id,
        fetcher=fetcher,
        period_model=_period_model_for(season),
        return_as_pandas=return_as_pandas,
    )


def ncaa_wbb_play_by_play(
    game_ids: Sequence[object],
    *,
    season: Optional[object] = None,
    fetcher: Optional[_SupportsFetchGamePbp] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Scrape many WBB games' play-by-play (wbigballR ``get_play_by_play``, era-aware).

    Same driver as :func:`sportsdataverse.mbb.mbb_ncaa_game_pbp.ncaa_mbb_play_by_play`
    (drop missing ids, shared fetcher session, one retry per empty scrape) with
    the WBB period model for each game's era bound.

    Args:
        game_ids: NCAA contest ids; ``None``/NaN entries are dropped.
        season: Season shared by every id, as an ending year or span. When
            omitted each page's era is inferred from its first-period clock, so
            ids from both eras may be mixed.
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
        period_model=_period_model_for(season),
        return_as_pandas=return_as_pandas,
    )
