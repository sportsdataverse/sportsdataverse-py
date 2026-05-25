"""sportsdataverse.nhl.nhl_records_parsers — polars parsers for the
NHL Records site API at ``records.nhl.com/site/api/``.

**Documentation**:

* NHL Records endpoint reference: https://py.sportsdataverse.org/docs/nhl/records
* Parsers overview: https://py.sportsdataverse.org/docs/parsers/

Every Records endpoint ships its rows under the same top-level
``{data: [...], total: N}`` shape (identical to NHL Stats REST), so
a single generic parser :func:`parse_nhl_records` handles all 50
wrappers in :mod:`sportsdataverse.nhl.nhl_records`.
"""

from __future__ import annotations

from typing import Dict

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [underscore(c).replace(".", "_") for c in df.columns]
    return df


def _to_output(df: pd.DataFrame, return_as_pandas: bool):
    if return_as_pandas:
        return df
    try:
        return pl.from_pandas(df)
    except Exception:
        df2 = df.copy()
        for col in df2.select_dtypes(include="object").columns:
            df2[col] = df2[col].astype(str)
        return pl.from_pandas(df2)


def _empty_frame(return_as_pandas: bool = False):
    df = pd.DataFrame()
    return df if return_as_pandas else pl.DataFrame()


def parse_nhl_records(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Parse any NHL Records response into a tidy frame.

    Every Records endpoint ships ``{data: [{...}, ...], total: N}``.
    This parser unwraps ``data`` and flattens it via
    :func:`pandas.json_normalize`.

    Args:
        payload: Raw JSON dict from any ``nhl_records_*`` wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per record. Zero rows
        when the payload is missing ``data`` or has an empty list.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(rows, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, list)).any():
            df[col] = df[col].apply(lambda v: str(v) if isinstance(v, list) else v)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


def parser_for_nhl_records(fn_name: str):
    """Return the parser for any ``nhl_records_*`` wrapper.

    Because every Records endpoint shares the ``{data: [...]}`` shape,
    this function always returns :func:`parse_nhl_records`. The
    function exists for API symmetry with
    :func:`sportsdataverse.nhl.nhl_stats_rest_parsers.parser_for_nhl_stats_rest`
    and :func:`sportsdataverse.mlb.mlb_api_parsers.parser_for_mlb_api`.

    Args:
        fn_name: The ``__name__`` of any ``nhl_records_*`` wrapper.
            Unused — all names route to the same parser.

    Returns:
        :func:`parse_nhl_records`.
    """
    return parse_nhl_records
