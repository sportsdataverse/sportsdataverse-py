"""Parser for the College Football Data API.

CFBD is unusually uniform: almost every route returns a flat JSON array of
records, so one parser covers all 58 endpoints rather than one per shape. The few
that return an object are wrapped into a single row.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, snake_cased columns, and an **empty payload yields a
zero-row frame instead of raising**, so callers can chain without null-checks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Union

import polars as pl

from sportsdataverse.dl_utils import underscore

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd

__all__ = ["parse_cfbd_records"]


def _flatten(record: Dict[str, Any]) -> Dict[str, Any]:
    """One level of nesting -> ``parent_child`` columns; deeper values stringified.

    CFBD nests a handful of small objects (a game's ``homeLineScores``, a rating's
    breakdown). Flattening one level keeps the useful scalars addressable, while
    lists become strings so polars accepts a ragged frame rather than erroring on
    a mixed column.
    """
    out: Dict[str, Any] = {}
    for key, value in record.items():
        col = underscore(str(key))
        if isinstance(value, dict):
            for sub, sub_value in value.items():
                out[f"{col}_{underscore(str(sub))}"] = (
                    sub_value if not isinstance(sub_value, (dict, list)) else str(sub_value)
                )
        elif isinstance(value, list):
            out[col] = str(value)
        else:
            out[col] = value
    return out


def parse_cfbd_records(
    raw: Union[List[Any], Dict[str, Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """A CFBD payload -> one tidy frame.

    Args:
        raw: The decoded JSON: a list of records (the usual case), a single object
            (wrapped into one row), or ``None``.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per record, columns snake_cased. An empty or unusable payload
        returns a **zero-row frame**, never an exception.

    Example:
        Quick start::

            from sportsdataverse.cfb import cfbd_teams

            df = cfbd_teams(year=2024)
            print(df.shape)
    """
    if raw is None:
        records: List[Any] = []
    elif isinstance(raw, dict):
        records = [raw]
    else:
        records = list(raw)

    rows = [_flatten(r) for r in records if isinstance(r, dict)]
    # infer_schema_length=None: CFBD leaves optional fields null for long runs of
    # rows, and a short inference window types those columns Null and then fails
    # when a real value finally appears.
    frame = pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()
    return frame.to_pandas() if return_as_pandas else frame
