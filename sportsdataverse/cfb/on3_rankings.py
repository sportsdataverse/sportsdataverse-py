"""Deprecated On3 rankings shim wrappers (continuity for the RDB retarget).

The generated ``on3`` stem now targets the On3 Recruit Database
(``api.on3.com/public/rdb``). Before the retarget it owned four ``on3_*_rankings``
wrapper names backed by the on3.com ``_next/data`` scrape. Those four names keep
working here — each emits a :class:`DeprecationWarning` pointing at the forward
RDB native, then routes through the retained scrape getter
(:func:`sportsdataverse.cfb.on3_runtime._scrape_get`) and the retained rankings
parsers, so no public name was dropped.

Forward path (RDB natives, defined in the generated ``sportsdataverse.cfb.on3``):

============================== ==========================================
Deprecated shim                RDB native
============================== ==========================================
``on3_player_rankings``        ``on3_person_sport_rankings``
``on3_industry_player_rankings`` ``on3_players_industry_comparision``
``on3_team_rankings``          ``on3_team_ranking_team_rankings``
``on3_industry_team_rankings`` ``on3_team_ranking_consensus_team_rankings``
============================== ==========================================
"""

from __future__ import annotations

import warnings
from typing import Any, Dict, Union

import pandas as pd
import polars as pl

from sportsdataverse.cfb.on3_parsers import parse_on3_rankings, parse_on3_team_rankings
from sportsdataverse.cfb.on3_runtime import _scrape_get

__all__ = [
    "on3_industry_player_rankings",
    "on3_industry_team_rankings",
    "on3_player_rankings",
    "on3_team_rankings",
]

_HOST = "https://www.on3.com"


def _scrape_rankings(
    ranking_type: str,
    parser: Any,
    forward: str,
    year: Union[int, str],
    sport_slug: str,
    page: Any,
    return_parsed: bool,
    return_as_pandas: bool,
    **kwargs: Any,
) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
    """Shared body for the four deprecated rankings shims."""
    warnings.warn(
        f"on3_{ranking_type.replace('-', '_')}_rankings scrapes the legacy on3.com "
        f"_next/data route and is deprecated; use the auth-free RDB native "
        f"sportsdataverse.cfb.{forward} instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    url = f"{_HOST}/rivals/rankings/{ranking_type}/{sport_slug}/{year}.json"
    raw = _scrape_get(url, params={"page": page}, **kwargs)
    if not return_parsed:
        return raw
    return parser(raw, return_as_pandas=return_as_pandas)


def on3_player_rankings(
    year: Union[int, str],
    sport_slug: str = "football",
    page: Any = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
    """On3 player rankings for a class year (**deprecated** ``_next/data`` scrape).

    Args:
        year: recruiting class year (e.g. ``2026``).
        sport_slug: On3 sport slug (default ``"football"``).
        page: 1-based page number, or ``None`` for the first page.
        return_parsed: return a tidy frame (default); ``False`` returns the raw dict.
        return_as_pandas: return a pandas DataFrame instead of polars.
        **kwargs: forwarded to the scrape getter.

    Returns:
        One row per ranked recruit (On3 ratings). Zero-row frame on empty payload.

    Example:
        Quick start::

            from sportsdataverse.cfb import on3_person_sport_rankings  # forward RDB native
            df = on3_person_sport_rankings(sport_key=1, year=2026)
            print(df.shape)

    See Also:
        * `recruitR`_ -- college recruiting data in R (CFBD-backed).

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    return _scrape_rankings(
        "player",
        parse_on3_rankings,
        "on3_person_sport_rankings",
        year,
        sport_slug,
        page,
        return_parsed,
        return_as_pandas,
        **kwargs,
    )


def on3_industry_player_rankings(
    year: Union[int, str],
    sport_slug: str = "football",
    page: Any = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
    """On3 Industry Comparison player rankings (**deprecated** ``_next/data`` scrape).

    Args:
        year: recruiting class year (e.g. ``2026``).
        sport_slug: On3 sport slug (default ``"football"``).
        page: 1-based page number, or ``None`` for the first page.
        return_parsed: return a tidy frame (default); ``False`` returns the raw dict.
        return_as_pandas: return a pandas DataFrame instead of polars.
        **kwargs: forwarded to the scrape getter.

    Returns:
        One row per recruit (consensus On3/Rivals/247/ESPN). Zero-row frame on empty.

    Example:
        Quick start::

            from sportsdataverse.cfb import on3_players_industry_comparision  # forward RDB native
            df = on3_players_industry_comparision(sport_key=1, year=2026)
            print(df.shape)

    See Also:
        * `recruitR`_ -- college recruiting data in R (CFBD-backed).

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    return _scrape_rankings(
        "industry-player",
        parse_on3_rankings,
        "on3_players_industry_comparision",
        year,
        sport_slug,
        page,
        return_parsed,
        return_as_pandas,
        **kwargs,
    )


def on3_team_rankings(
    year: Union[int, str],
    sport_slug: str = "football",
    page: Any = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
    """On3 team recruiting-class rankings (**deprecated** ``_next/data`` scrape).

    Args:
        year: recruiting class year (e.g. ``2026``).
        sport_slug: On3 sport slug (default ``"football"``).
        page: 1-based page number, or ``None`` for the first page.
        return_parsed: return a tidy frame (default); ``False`` returns the raw dict.
        return_as_pandas: return a pandas DataFrame instead of polars.
        **kwargs: forwarded to the scrape getter.

    Returns:
        One row per team class (On3 ratings). Zero-row frame on empty payload.

    Example:
        Quick start::

            from sportsdataverse.cfb import on3_team_ranking_team_rankings  # forward RDB native
            df = on3_team_ranking_team_rankings(sport_slug="football", year=2025)
            print(df.shape)

    See Also:
        * `recruitR`_ -- college recruiting data in R (CFBD-backed).

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    return _scrape_rankings(
        "team",
        parse_on3_team_rankings,
        "on3_team_ranking_team_rankings",
        year,
        sport_slug,
        page,
        return_parsed,
        return_as_pandas,
        **kwargs,
    )


def on3_industry_team_rankings(
    year: Union[int, str],
    sport_slug: str = "football",
    page: Any = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
    """On3 Industry Comparison team rankings (**deprecated** ``_next/data`` scrape).

    Args:
        year: recruiting class year (e.g. ``2026``).
        sport_slug: On3 sport slug (default ``"football"``).
        page: 1-based page number, or ``None`` for the first page.
        return_parsed: return a tidy frame (default); ``False`` returns the raw dict.
        return_as_pandas: return a pandas DataFrame instead of polars.
        **kwargs: forwarded to the scrape getter.

    Returns:
        One row per team class (consensus ratings). Zero-row frame on empty payload.

    Example:
        Quick start::

            from sportsdataverse.cfb import on3_team_ranking_consensus_team_rankings  # forward RDB native
            df = on3_team_ranking_consensus_team_rankings(sport_slug="football", year=2025)
            print(df.shape)

    See Also:
        * `recruitR`_ -- college recruiting data in R (CFBD-backed).

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    return _scrape_rankings(
        "industry-team",
        parse_on3_team_rankings,
        "on3_team_ranking_consensus_team_rankings",
        year,
        sport_slug,
        page,
        return_parsed,
        return_as_pandas,
        **kwargs,
    )
