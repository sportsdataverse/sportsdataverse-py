"""Parsers for the generated ``sports247`` wrappers (247Sports RDB public endpoints).

Two payload shapes: the teams directory is a bare JSON array; the
institution-rankings feed is a ``{"pagination": {...}, "list": [...]}``
envelope. Package parser contract throughout: polars by default, pandas via
``return_as_pandas=True``, zero-row frame on empty / malformed payloads,
snake-cased columns via :func:`sportsdataverse.dl_utils.underscore`.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_sports247_teams",
    "parse_sports247_institution_rankings",
]


def _rows_to_frame(rows: List[Dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    df = pd.json_normalize(rows, sep="_")
    df.columns = [underscore(str(c)) for c in df.columns]
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].map(lambda v: v if v is None or isinstance(v, str) else str(v))
    return pl.from_pandas(df)


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
    rows = raw if isinstance(raw, list) else []
    df = _rows_to_frame(rows)
    return df.to_pandas() if return_as_pandas else df


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
    rows = (raw or {}).get("list") if isinstance(raw, dict) else None
    df = _rows_to_frame(rows if isinstance(rows, list) else [])
    return df.to_pandas() if return_as_pandas else df
