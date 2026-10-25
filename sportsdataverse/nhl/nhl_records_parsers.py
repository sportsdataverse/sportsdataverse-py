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

from sportsdataverse._codegen_runtime import _cast_id_columns, _pandas_ids, _str_or_null
from sportsdataverse._parser_common import _FROM_PANDAS_ERRORS, _empty_frame, _snake_columns
from sportsdataverse.errors import _SHAPE_ERRORS, _object_rows, _warn_schema_drift


def _to_output(df: pd.DataFrame, return_as_pandas: bool):
    try:
        out = _cast_id_columns(pl.from_pandas(df), "NHL Records parser")
    except _FROM_PANDAS_ERRORS:
        # A mixed-type object column fails the Arrow conversion: stringify rather than lose the frame.
        df2 = df.copy()
        for col in [c for c in df2.columns if df2[c].dtype == "object"]:
            df2[col] = df2[col].map(_str_or_null)  # a missing cell stays null, never "nan"
        out = _cast_id_columns(pl.from_pandas(df2), "NHL Records parser")
    # R986-1: the pandas frame carries the same ids, as pandas' nullable Int64 (never float64 / object)
    return _pandas_ids(out, df) if return_as_pandas else out


def parse_nhl_records(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
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
    rows = [row for row in rows if row not in (None, {})] if isinstance(rows, list) else None
    if not rows:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(_object_rows(rows), sep="_")
    except _SHAPE_ERRORS as exc:
        _warn_schema_drift("parse_nhl_records: data", exc)
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
