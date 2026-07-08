"""Parsers for the generated ``sports247`` wrappers (247Sports RDB).

Every RDB route resolves to one of three JSON shapes, all handled by
:func:`parse_sports247_result_set`: a bare array (``teams``, the ranking
feeds, ``currentTargetPredictions``), a ``{<key>: [...]}`` envelope where the
row list lives under ``players`` / ``results`` / ``rankings`` / ``list`` /
``items`` (``recruits``, ``coaches``, ``transfers``, ``institutionrankings``,
``transferPortalPlayerfeed``), or a single flat object (``bettinginfo``).
``parse_sports247_teams`` / ``parse_sports247_institution_rankings`` are named
aliases kept for the two public endpoints shipped first. Package parser
contract throughout: polars by default, pandas via ``return_as_pandas=True``,
zero-row frame on empty / malformed payloads, snake-cased columns via
:func:`sportsdataverse.dl_utils.underscore`.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_sports247_result_set",
    "parse_sports247_teams",
    "parse_sports247_institution_rankings",
]

# Envelope keys the RDB uses for the row list, in resolution order.
_LIST_KEYS = ("players", "results", "rankings", "list", "items")


def _rows_to_frame(rows: List[Any]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    # Scalar-array payloads (e.g. ``sports/{k}/year`` -> [2007, 2008, ...]) carry
    # no keys; surface them under a single ``value`` column.
    if not isinstance(rows[0], dict):
        rows = [{"value": r} for r in rows]
    df = pd.json_normalize(rows, sep="_")
    df.columns = [underscore(str(c)) for c in df.columns]
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].map(lambda v: v if v is None or isinstance(v, str) else str(v))
    return pl.from_pandas(df)


def _extract_rows(raw: Any) -> List[Dict[str, Any]]:
    """Pull the row list out of any RDB payload shape (array / envelope / object)."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for k in _LIST_KEYS:
            if isinstance(raw.get(k), list):
                return raw[k]
        # a single flat object (e.g. bettinginfo) -> one row; drop nested
        # pagination/meta-only dicts (no scalar payload) to a zero-row frame.
        if raw and any(not isinstance(v, (dict, list)) for v in raw.values()):
            return [raw]
    return []


def parse_sports247_result_set(
    raw: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse any 247Sports RDB payload into a tidy frame.

    Resolves the row list from a bare array, a ``{players|results|rankings|
    list|items: [...]}`` envelope, or a single flat object, then flattens
    nested objects (``sep="_"``) and snake-cases the columns.

    Args:
        raw: any RDB payload (``recruits``, ``transfers``, ``coaches``, the
            ranking feeds, ``currentTargetPredictions``, ``bettinginfo``, ...).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per result entry; zero-row frame when the payload is empty or
        malformed.

    Example:
        Quick start::

            from sportsdataverse.cfb import sports247_recruits
            df = sports247_recruits(year=2026)
            print(df.shape)

    See Also:
        * `recruitR`_ -- college recruiting data in R (CFBD-backed).

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    df = _rows_to_frame(_extract_rows(raw))
    return df.to_pandas() if return_as_pandas else df


def parse_sports247_teams(
    raw: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse the RDB teams directory (bare JSON array).

    Args:
        raw: ``sports247_teams`` payload (list of team dicts).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per team: ``name``, ``team_id``, ``institution_key``,
        ``conference`` / ``conference_abbreviation``, ``sport``, ``type``.
        Zero-row frame when the payload is empty or malformed.

    Example:
        Quick start::

            from sportsdataverse.cfb import sports247_teams
            df = sports247_teams(sport_key=1)
            print(df.shape)

    See Also:
        * `recruitR`_ -- college recruiting data in R (CFBD-backed).

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    return parse_sports247_result_set(raw, return_as_pandas=return_as_pandas)


def parse_sports247_institution_rankings(
    raw: Optional[Dict[str, Any]],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse the RDB institution (team recruiting-class) rankings feed.

    Args:
        raw: ``sports247_institution_rankings`` payload
            (``{"pagination": ..., "list": [...]}``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per institution: 247Sports ``rank`` / ``rating`` and
        industry-composite ``composite_rank`` / ``composite_rating``, star
        counts (247 + composite), ``commits``, conference ranks, transfer
        points, and ``institution_key`` / ``team_key`` ids. Zero-row frame
        when the payload is empty or malformed.

    Example:
        Quick start::

            from sportsdataverse.cfb import sports247_institution_rankings
            df = sports247_institution_rankings(year=2026)
            print(df.shape)

    See Also:
        * `recruitR`_ -- college recruiting data in R (CFBD-backed).

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    return parse_sports247_result_set(raw, return_as_pandas=return_as_pandas)
