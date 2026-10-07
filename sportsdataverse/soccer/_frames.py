"""Shared frame builders for the soccer flat-API parsers (ASA / MLS / NWSL).

The three soccer providers wrapped by codegen -- American Soccer Analysis, the
official MLS web API and the official NWSL (StatsPerform SDP) API -- all serve
plain JSON objects, so the "list of records -> tidy polars frame" step is
identical for every one of them. It lives here once rather than three times.

Two provider-agnostic rules are enforced in :func:`rows_to_frame` because both
are correctness issues rather than formatting:

* **Id columns stay ``Utf8``.** All three providers use opaque *string* ids
  (ASA base62 ``"9Yqdwg85vJ"``, MLS Sportec ``"MLS-MAT-0009H8"``, NWSL composite
  ``"nwsl::Football_Team::<32-hex>"``). Any column named ``id`` / ``*_id`` /
  ``*_ids`` is pinned to ``Utf8``, and a float-typed one is routed through
  ``Int64`` first so a nullable integer id can never stringify to ``"123.0"``.
* **Nested cells are stringified.** Nested objects flatten to ``_``-joined
  columns in ``pandas.json_normalize`` order (without pandas), but list-valued
  cells would be Python lists, which polars rejects when the element types are
  mixed. Those are JSON-encoded --
  except list-valued *id* cells (ASA serializes ``team_id`` as a list for a
  player who featured for several clubs), which are comma-joined so the column
  stays a readable Utf8 join key.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = ["is_id_name", "rows_to_frame", "to_utf8_ids"]


def is_id_name(name: str) -> bool:
    """True when ``name`` is a join-key column that must stay ``Utf8``.

    Args:
        name: snake_cased column name.

    Returns:
        ``True`` for ``id`` and any ``*_id`` / ``*_ids`` name.

    Example:
        Basic use::

            from sportsdataverse.soccer._frames import is_id_name

            is_id_name("team_id")
            # True
    """
    return name == "id" or name.endswith(("_id", "_ids"))


def to_utf8_ids(df: pl.DataFrame, cols: Optional[Iterable[str]] = None) -> pl.DataFrame:
    """Pin id columns to ``Utf8`` without ever producing a ``"123.0"`` string.

    A nullable integer id read back through pandas arrives as ``Float64``; a
    direct cast to ``Utf8`` would then write ``"123.0"`` and silently break every
    join against the same id read as an integer elsewhere. Float columns are
    therefore cast to ``Int64`` first.

    Args:
        df: frame to normalize (may be empty).
        cols: id column names; when ``None`` every column matching
            :func:`is_id_name` is used.

    Returns:
        The frame with each named column cast to ``Utf8``.

    Example:
        Basic use::

            import polars as pl
            from sportsdataverse.soccer._frames import to_utf8_ids

            to_utf8_ids(pl.DataFrame({"team_id": [123.0]})).item()
            # '123'
    """
    if df.height == 0:
        return df
    names = list(cols) if cols is not None else [c for c in df.columns if is_id_name(c)]
    exprs = []
    for col in names:
        if col not in df.columns or df.schema[col] == pl.String:
            continue
        expr = pl.col(col)
        if df.schema[col].is_float():
            expr = expr.cast(pl.Int64, strict=False)
        exprs.append(expr.cast(pl.String).alias(col))
    return df.with_columns(exprs) if exprs else df


def _flatten(record: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Flatten nested objects to ``parent_child`` keys (``json_normalize(sep="_")`` semantics).

    Lists are kept as values; an empty nested object contributes no key; a
    ``None`` nested object stays a ``None`` cell. Column order is
    ``json_normalize``'s: top-level scalars first, then each nested object's
    keys in document order.
    """
    flat: Dict[str, Any] = {}
    nested: List[Tuple[str, Dict[str, Any]]] = []
    for key, value in record.items():
        name = f"{prefix}{key}"
        if isinstance(value, dict) and not prefix:
            nested.append((name, value))
        elif isinstance(value, dict):
            flat.update(_flatten(value, f"{name}_"))
        else:
            flat[name] = value
    for name, value in nested:
        flat.update(_flatten(value, f"{name}_"))
    return flat


def _encode(name: str, value: Any) -> Any:
    """JSON-encode one list cell (id lists become comma-joined); scalars pass through."""
    if isinstance(value, list) and is_id_name(name):
        return ",".join(str(v) for v in value)
    if isinstance(value, list):
        return json.dumps(value)
    return value


def _kind(value: Any) -> str:
    """Scalar type class: bool / number / str -- mixing classes in one column forces Utf8."""
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "number"
    return "str"


def rows_to_frame(rows: Sequence[Any]) -> pl.DataFrame:
    """Flatten a list of JSON records into a tidy polars frame.

    Missing values stay nulls: a nullable boolean column is ``Boolean`` (never
    the string ``"nan"``), a nullable integer column is ``Int64`` (never
    promoted to ``Float64``), and a column that is null on every row is
    ``Utf8`` (no non-null value means no evidence of a type, and Utf8 is the
    lossless choice for a later concat). A column mixing scalar classes (bool / number / str) is
    stringified so the frame is always buildable.

    Args:
        rows: records from a provider payload. Non-dict entries (a bare scalar
            array) are wrapped as a single ``value`` column.

    Returns:
        One row per record with nested objects flattened to ``parent_child``
        snake_cased columns; list cells stringified and id columns pinned to
        ``Utf8``. A zero-row, zero-column frame when ``rows`` is empty.

    Example:
        Basic use::

            from sportsdataverse.soccer._frames import rows_to_frame

            rows_to_frame([{"teamId": "abc", "team": {"name": "LAFC"}}]).columns
            # ['team_id', 'team_name']
    """
    # A null entry carries nothing; keeping it would emit a row of the string
    # "None", which is worse than no row at all.
    kept = [r for r in rows if r is not None]
    if not kept:
        return pl.DataFrame()
    if not any(isinstance(r, dict) for r in kept):
        return pl.DataFrame({"value": [str(r) for r in kept]})
    flat = [_flatten(r if isinstance(r, dict) else {"value": r}) for r in kept]
    # Column names: union of raw keys in first-seen order, snake_cased, suffixed
    # when two raw keys collapse to one snake name.
    seen: Dict[str, int] = {}
    names: Dict[str, str] = {}
    for record in flat:
        for raw_name in record:
            if raw_name in names:
                continue
            name = underscore(raw_name)
            if name in seen:
                seen[name] += 1
                name = f"{name}_{seen[name]}"
            else:
                seen[name] = 1
            names[raw_name] = name
    columns: Dict[str, List[Any]] = {name: [None] * len(flat) for name in names.values()}
    for i, record in enumerate(flat):
        for raw_name, value in record.items():
            name = names[raw_name]
            columns[name][i] = _encode(name, value)
    for name, values in columns.items():
        kinds = {_kind(v) for v in values if v is not None}
        if len(kinds) > 1:
            columns[name] = [None if v is None else str(v) for v in values]
        elif kinds == {"number"} and any(isinstance(v, float) for v in values):
            columns[name] = [None if v is None else float(v) for v in values]
    df = pl.DataFrame(columns)  # the per-column kind pass above decides the dtypes
    all_null = [c for c, t in df.schema.items() if t == pl.Null]
    if all_null:
        df = df.with_columns(pl.col(all_null).cast(pl.String))
    return to_utf8_ids(df)


def as_output(
    df: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Return ``df`` as polars (default) or pandas.

    Args:
        df: the parsed frame.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        The frame in the requested library.

    Example:
        Basic use::

            import polars as pl
            from sportsdataverse.soccer._frames import as_output

            type(as_output(pl.DataFrame({"a": [1]}), return_as_pandas=True)).__name__
            # 'DataFrame'
    """
    return df.to_pandas() if return_as_pandas else df


def as_tables(
    tables: Dict[str, pl.DataFrame],
    *,
    return_as_pandas: bool = False,
) -> Dict[str, Union[pl.DataFrame, pd.DataFrame]]:
    """Return a multi-table result as polars (default) or pandas frames.

    Args:
        tables: sub-frame name -> parsed frame.
        return_as_pandas: return pandas DataFrames instead of polars.

    Returns:
        The same mapping with every value converted.

    Example:
        Basic use::

            import polars as pl
            from sportsdataverse.soccer._frames import as_tables

            sorted(as_tables({"a": pl.DataFrame()}))
            # ['a']
    """
    if not return_as_pandas:
        return dict(tables)
    return {k: v.to_pandas() for k, v in tables.items()}
