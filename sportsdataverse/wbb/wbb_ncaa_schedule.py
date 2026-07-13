"""Women's college basketball NCAA schedule + roster scrapers (wbigballR port).

Thin shims over :mod:`sportsdataverse.mbb.mbb_ncaa_schedule` composing the
same private helpers as its public MBB wrappers, with ``league="wbb"`` bound.

**Deliberate fix of wbigballR.** wbigballR ``get_team_schedule`` /
``get_team_roster`` resolve ``team=`` + ``season=`` through
``bigballR::teamids`` — the MEN'S crosswalk — so women's names resolve to the
men's season-specific team id (or ``NA``). These shims resolve through the
bundled WBB crosswalk (``ncaa_wbb_team_ids``). See
``dev/bigballr_port/design.md`` ("Known R-side breakages ... do NOT port
literally").
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Union

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_schedule import (
    _fetch_html,
    _resolve_id,
    parse_ncaa_bb_team_roster,
    parse_ncaa_bb_team_schedule,
)

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

__all__ = [
    "ncaa_wbb_team_schedule",
    "ncaa_wbb_team_roster",
]


def ncaa_wbb_team_schedule(
    team_id: Optional[int] = None,
    *,
    team: Optional[str] = None,
    season: Optional[str] = None,
    fetcher: Optional["NcaaFetcher"] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Scrape a women's team's season schedule from stats.ncaa.org.

    Port of wbigballR ``get_team_schedule`` with name resolution fixed to the
    WBB crosswalk (see the module docstring). Algorithm detail:
    :func:`sportsdataverse.mbb.mbb_ncaa_schedule.ncaa_mbb_team_schedule`.

    Args:
        team_id: stats.ncaa.org team id (changes every season).
        team: School name, e.g. ``"South Carolina"``.
        season: Season string, e.g. ``"2024-25"``; required with ``team``.
        fetcher: Injectable :class:`~sportsdataverse.mbb.mbb_ncaa_fetch.
            NcaaFetcher`; defaults to a fresh browser-transport fetcher.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per scheduled game — see
        :func:`~sportsdataverse.mbb.mbb_ncaa_schedule.parse_ncaa_bb_team_schedule`
        for the column contract.

    Raises:
        ValueError: Neither ``team_id`` nor a resolvable ``team``/``season``
            pair was given.

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_schedule import ncaa_wbb_team_schedule
            df = ncaa_wbb_team_schedule(team="South Carolina", season="2024-25")
            print(df.shape)
    """
    resolved = _resolve_id(team_id, team, season, "wbb")
    html = _fetch_html(fetcher, f"teams/{resolved}")
    df = parse_ncaa_bb_team_schedule(html, resolved, league="wbb")
    return df.to_pandas() if return_as_pandas else df


def ncaa_wbb_team_roster(
    team_id: Optional[int] = None,
    *,
    team: Optional[str] = None,
    season: Optional[str] = None,
    fetcher: Optional["NcaaFetcher"] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Scrape a women's team roster from stats.ncaa.org.

    Port of wbigballR ``get_team_roster`` with name resolution fixed to the
    WBB crosswalk (see the module docstring). The roster parser itself is
    league-agnostic; algorithm detail:
    :func:`sportsdataverse.mbb.mbb_ncaa_schedule.ncaa_mbb_team_roster`.

    Args:
        team_id: stats.ncaa.org team id (changes every season).
        team: School name, e.g. ``"South Carolina"``.
        season: Season string, e.g. ``"2024-25"``; required with ``team``.
        fetcher: Injectable :class:`~sportsdataverse.mbb.mbb_ncaa_fetch.
            NcaaFetcher`; defaults to a fresh browser-transport fetcher.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per player — see
        :func:`~sportsdataverse.mbb.mbb_ncaa_schedule.parse_ncaa_bb_team_roster`
        for the column contract.

    Raises:
        ValueError: Neither ``team_id`` nor a resolvable ``team``/``season``
            pair was given.

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_schedule import ncaa_wbb_team_roster
            df = ncaa_wbb_team_roster(team="South Carolina", season="2024-25")
            print(df.select("jersey", "player", "ht_inches").head())
    """
    resolved = _resolve_id(team_id, team, season, "wbb")
    html = _fetch_html(fetcher, f"teams/{resolved}/roster")
    df = parse_ncaa_bb_team_roster(html, resolved)
    return df.to_pandas() if return_as_pandas else df
