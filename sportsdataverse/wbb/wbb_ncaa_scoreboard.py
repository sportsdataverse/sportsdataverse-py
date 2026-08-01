"""Women's college basketball NCAA dated scoreboard (wbigballR port).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_scoreboard` with
``league="wbb"`` bound — the one league knob is the ``season_divisions``
id table.

**Season coverage caveat.** The WBB season table
(:data:`~sportsdataverse.mbb.mbb_ncaa_scoreboard.NCAA_WBB_SEASON_DIVISIONS`)
covers 2010-11 through 2025-26 — wbigballR is an older fork, so it has NO
2009-10 entry (the MBB table does). 2025-26 was likewise missing upstream
and was discovered live + backfilled here (2026-08-01). Dates in a missing
season raise ``ValueError``; extend the table forward as new ids are
confirmed.
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
        conference: Conference name filter (e.g. ``"SEC"``); default
            ``"All"``. Unknown names warn and fall back to all conferences.
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
        ValueError: The date's season has no WBB ``season_divisions`` id
            (includes 2009-10 — see module docstring), the date is not
            ``MM/DD/YYYY``, or the fetched page has no games table.

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
