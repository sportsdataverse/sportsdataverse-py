"""Women's college basketball NCAA dated scoreboard (wbigballR port).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_scoreboard` with
``league="wbb"`` bound — the one league knob is the ``season_divisions``
id table.

**Season ids.** The season id comes from the table every stats.ncaa.org sport shares
(:mod:`sportsdataverse.scrape.ncaa.seasons`), which covers every WBB season the site lists,
2009-10 included (wbigballR's own table started at 2010-11). A season opened after the package
was built is resolved with one live lookup; only a season the site does not list raises
``ValueError``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Union

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_scoreboard import _ncaa_bb_date_games

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

__all__ = [
    "ncaa_wbb_date_games",
]


def ncaa_wbb_date_games(
    date: Optional[str] = None,
    *,
    conference: str = "All",
    conference_id: Optional[int] = None,
    fetcher: Optional["NcaaFetcher"] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Discover every NCAA WBB game played on a date (wbigballR ``get_date_games``).

    Same engine as
    :func:`sportsdataverse.mbb.mbb_ncaa_scoreboard.ncaa_mbb_date_games` with
    the WBB ``season_divisions`` table bound (see the module docstring for
    the 2010-11..2025-26 coverage caveat).

    Args:
        date: ``"MM/DD/YYYY"``. Defaults to yesterday (R default).
        conference: Conference name filter (e.g. ``"SEC"``,
            ``"Summit League"``); case/punctuation-insensitive. Default
            ``"All"`` (every conference). Unknown names raise.
        conference_id: Explicit stats.ncaa.org conference id; overrides
            *conference* when given.
        fetcher: Injectable :class:`~sportsdataverse.mbb.mbb_ncaa_fetch.
            NcaaFetcher` (tests pass an offline fake). ``None`` uses
            ``NcaaFetcher.with_browser()``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per game — see the MBB sibling for the full
        ``SCOREBOARD_SCHEMA`` column contract.

    Raises:
        ValueError: stats.ncaa.org lists no WBB season for the date (a season
            missing from the bundled table is looked up live first, see the
            module docstring), *conference* is not a known conference, the date
            is not ``MM/DD/YYYY``, or the fetched page has no games table.

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_scoreboard import ncaa_wbb_date_games
            games = ncaa_wbb_date_games("12/05/2024")
            print(games.shape)
    """
    df = _ncaa_bb_date_games(
        date,
        conference=conference,
        conference_id=conference_id,
        fetcher=fetcher,
        league="wbb",
    )
    return df.to_pandas() if return_as_pandas else df
