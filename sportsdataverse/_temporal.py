"""Date coercion for columns that arrive as String from one source and Date/Datetime from another.

polars 2.0 removed the ``String -> Date`` cast (it raises), and ``str.to_date`` only accepts String,
so neither ``.cast(pl.Date)`` nor ``.str.to_date()`` alone is safe on a column whose dtype depends on
which loader produced it. ``as_date`` dispatches on the dtype the column actually has.
"""

from __future__ import annotations

import polars as pl


def _series_as_date(s: pl.Series) -> pl.Series:
    return s.str.to_date() if s.dtype == pl.String else s.cast(pl.Date)


def as_date(expr: pl.Expr) -> pl.Expr:
    """``expr`` as ``pl.Date``: parsed when it is String, cast when it is already temporal."""
    return expr.map_batches(_series_as_date, return_dtype=pl.Date, is_elementwise=True)
