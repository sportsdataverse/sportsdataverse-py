"""Tidy parsers for the NFL Pro (Next Gen Stats) wrappers.

Every `/api/secured/stats/*` route returns the same envelope shape -- request
params echoed back alongside one list of records -- so a single parser serves all
of them; only the name of the collection key changes (``passers``, ``rushers``,
``receivers``, ``defenders``, ``offense``, ``defense``, ``players``).

Follows the package parser contract: polars by default, pandas on request, and a
zero-row frame (never an exception) for an empty or malformed payload.
"""

from __future__ import annotations

from typing import Any, Dict, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = ["parse_nfl_pro_stats", "_COLLECTION_KEYS"]

# The envelope echoes request params back next to the data, and some echoes are
# themselves lists, so "the first list value" would pick the wrong key.
_COLLECTION_KEYS = ("passers", "rushers", "receivers", "defenders", "offense", "defense", "players")


def _is_record_list(value: Any) -> bool:
    """True if ``value`` is a list this parser can build a frame from.

    The elements must be dicts. A list of scalars reaches ``json_normalize`` as a
    ``TypeError``, and the shape that produces one is a *successful* request: when
    a query legitimately matches zero rows the API can omit the collection key
    while still echoing ``positionGroup`` back as a one-element list of strings.
    """
    return isinstance(value, list) and (not value or isinstance(value[0], dict))


def _dicts(values: Any) -> list:
    """Keep only the dict elements -- one stray scalar must not lose the whole page."""
    return [v for v in values if isinstance(v, dict)]


def _records(payload: Any) -> list:
    if isinstance(payload, list):
        return _dicts(payload) if _is_record_list(payload) else []
    if not isinstance(payload, dict):
        return []
    for key in _COLLECTION_KEYS:
        value = payload.get(key)
        if _is_record_list(value):
            return _dicts(value)
    lists = [v for v in payload.values() if _is_record_list(v)]
    return _dicts(max(lists, key=len)) if lists else []


def parse_nfl_pro_stats(
    payload: Union[Dict[str, Any], list, None],
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Turn an NFL Pro stats payload into one row per player, team or player-week.

    Args:
        payload: A decoded `pro.nfl.com` `/api/secured/stats/*` response.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of ``polars``.

    Returns:
        One row per record, columns snake_cased. An empty or malformed payload
        yields a zero-row frame rather than raising.

    Example::

        from sportsdataverse.nfl import nfl_pro_players_offense_passing_season

        df = nfl_pro_players_offense_passing_season(season=2024, return_parsed=True)

    See Also:
        :func:`sportsdataverse.nfl.nflpro_runtime.nflpro_headers_gen`: reuse one
        authenticated header dict across many calls.
    """
    records = _records(payload)
    if not records:
        empty = pl.DataFrame()
        return empty.to_pandas() if return_as_pandas else empty
    # sep="_" so a nested object yields `a_b`, not the `a.b` json_normalize
    # defaults to -- underscore() rewrites camel boundaries, not dots.
    frame = pd.json_normalize(records, sep="_")
    frame.columns = [underscore(str(col)) for col in frame.columns]
    if frame.columns.duplicated().any():
        # Two spellings of one field (nflId + nfl_id) collide after snake-casing,
        # and polars rejects duplicate column names -- keep the first.
        frame = frame.loc[:, ~frame.columns.duplicated()]
    # polars rejects list-valued cells from json_normalize; stringify them.
    for col in frame.columns:
        if frame[col].apply(lambda cell: isinstance(cell, (list, dict))).any():
            frame[col] = frame[col].astype(str)
    out = pl.from_pandas(frame)
    return out.to_pandas() if return_as_pandas else out
