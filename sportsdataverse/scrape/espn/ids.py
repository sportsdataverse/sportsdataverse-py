"""The one id canonicalizer: every id is Int64, cast losslessly or refused.

Ids are join keys, and a join is only as correct as the dtype agreement on both
sides. These archives ship ``game_id``/``athlete_id``/``team_id`` as Int32 in
places while play ids (18 digits) and the wider ecosystem are Int64 -- the same
mismatch class the CFB loader-boundary canonicalization exists to close.

Refusing a lossy cast matters more than performing one: a truncated or
float-rounded id produces a structurally valid frame that joins to the wrong
row, which is strictly worse than an exception.
"""

from __future__ import annotations

import polars as pl

_WIDENABLE = (
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
)


def to_int64(series: pl.Series) -> pl.Series:
    """Canonicalize an id series to Int64, refusing any lossy conversion.

    Args:
        series: Ids as Int*, UInt*, Float*, or numeric Utf8.

    Returns:
        The same values as ``Int64``, nulls preserved.

    Raises:
        ValueError: If a float carries a fractional part, a string is not
            numeric, or the dtype is not an id-shaped type.
    """
    dtype = series.dtype
    if dtype == pl.Int64:
        return series
    if dtype in _WIDENABLE:
        return series.cast(pl.Int64)
    if dtype in (pl.Float32, pl.Float64):
        nonnull = series.drop_nulls()
        if len(nonnull) and (nonnull != nonnull.round(0)).any():
            raise ValueError(f"lossy float->Int64 id cast on {series.name!r}")
        return series.cast(pl.Int64)
    if dtype == pl.Utf8:
        out = series.cast(pl.Int64, strict=False)
        if out.null_count() > series.null_count():
            raise ValueError(f"non-numeric id value in {series.name!r}")
        return out
    raise ValueError(f"unsupported id dtype {dtype} on {series.name!r}")


def with_int64_ids(df: pl.DataFrame, *columns: str) -> pl.DataFrame:
    """Return ``df`` with each named id column canonicalized to Int64.

    Columns absent from the frame are skipped, so one call covers families that
    carry different id sets.
    """
    present = [c for c in columns if c in df.columns]
    if not present:
        return df
    return df.with_columns([to_int64(df[c]).alias(c) for c in present])
