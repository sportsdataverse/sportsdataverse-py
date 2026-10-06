"""Parser for the generated ``f1`` wrappers (Jolpica F1 API, Ergast-compatible).

Every ``api.jolpi.ca/ergast/f1`` body is one envelope::

    {"MRData": {xmlns, series, url, limit, offset, total, <Table>: {...}}}

where ``<Table>`` is ``RaceTable`` / ``StandingsTable`` / ``DriverTable`` /
``ConstructorTable`` / ``CircuitTable`` / ``SeasonTable`` / ``StatusTable`` and the
rows sit at the innermost list (``RaceTable.Races[].Results[]``,
``RaceTable.Races[].Laps[].Timings[]``, ``StandingsTable.StandingsLists[]
.DriverStandings[]``, ``CircuitTable.Circuits[]``, ...). :func:`parse_f1_mrdata`
flattens that per the reference repo's rule (``sdv-internal-refs/f1/f1-returns.md``):

* one row per element of the innermost list, with the scalars of every ancestor
  carried onto each row -- an intermediate list's scalars are prefixed by its
  singular (``Races[].raceName`` -> ``race_name``, ``Laps[].number`` ->
  ``lap_number``), and a scalar already carried unprefixed (``season``,
  ``round``) is not repeated;
* nested objects join with ``_`` in snake_case, and a leaf that repeats its
  parent's name collapses (``Driver.driverId`` -> ``driver_id``,
  ``FastestLap.Time.time`` -> ``fastest_lap_time``, ``FastestLap.lap`` ->
  ``fastest_lap``);
* a list inside a row (a driver standing's ``Constructors``) stays one
  JSON-encoded cell;
* keys are visited in sorted order over the UNION of the rows' shapes (a sprint
  weekend's ``Sprint`` sits between ``SecondPractice`` and ``ThirdPractice`` even when
  no single row has all three), so the column order is deterministic and matches
  the recon's tables.

Ergast serializes **every** value as a string. Columns documented as integers or
numbers (``season``, ``round``, ``position``, ``points``, ``laps``, ``grid``,
``time_millis``, circuit latitude/longitude, ...) are cast to ``Int64`` / ``Float64``
-- a deliberate divergence from f1dataR, which keeps every column character; ids
(``driver_id``, ``constructor_id``, ``circuit_id``, ``status_id``) stay ``Utf8`` join keys.
An empty payload (``Races: []`` on a non-sprint weekend) parses to a zero-row frame
with the documented columns when the caller passes ``columns=`` (the generated
wrappers do, from their returns-schema), so callers can chain without a null-check.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, a zero-row frame (never an exception) on an empty or
malformed payload, and snake_cased columns.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Sequence, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore
from sportsdataverse.soccer._frames import as_output, to_utf8_ids

__all__ = ["parse_f1_mrdata"]

# Lists an element of a row list may hold that are themselves row lists (descend),
# as opposed to a list that is a cell (a driver standing's ``Constructors``).
_DESCEND = frozenset(
    {
        "Races",
        "Results",
        "QualifyingResults",
        "SprintResults",
        "Laps",
        "Timings",
        "PitStops",
        "StandingsLists",
        "DriverStandings",
        "ConstructorStandings",
    },
)

# Columns Ergast documents as integers / numbers (serialized as strings on the wire).
_INT = frozenset(
    {
        "season",
        "round",
        "number",
        "position",
        "grid",
        "laps",
        "lap",
        "lap_number",
        "stop",
        "wins",
        "count",
        "time_millis",
        "fastest_lap",
        "fastest_lap_rank",
        "permanent_number",
        "driver_permanent_number",
    },
)
_FLOAT = frozenset(
    {
        "points",
        "fastest_lap_average_speed",
        "location_lat",
        "location_long",
        "circuit_location_lat",
        "circuit_location_long",
        "race_circuit_location_lat",
        "race_circuit_location_long",
    },
)

# ``id`` / ``*_id`` -- every Ergast join key (``driver_id``, ``circuit_id``, ``status_id``).
_ID = re.compile(r"(^|_)id$")


def _join(prefix: str, key: str) -> str:
    """``_``-join a prefix and a snake leaf, collapsing a leaf that repeats the prefix's last segment."""
    if not prefix:
        return key
    last = prefix.rsplit("_", 1)[-1]
    if key == last:
        return prefix
    if key.startswith(last + "_"):
        return prefix + key[len(last) :]
    return prefix + "_" + key


def _flatten(obj: Dict[str, Any], prefix: str, out: Dict[str, Any], skip: Optional[str] = None) -> None:
    """Flatten ``obj`` into ``out`` (first occurrence of a column wins); lists become JSON cells."""
    for key in sorted(obj):
        if key == skip:
            continue
        value = obj[key]
        col = _join(prefix, underscore(key))
        if isinstance(value, dict):
            _flatten(value, col, out)
        elif col not in out:
            out[col] = json.dumps(value) if isinstance(value, list) else value


def _shape(values: List[Any], shape: Dict[str, Any]) -> Dict[str, Any]:
    """Merge the key structure of every dict in ``values`` into ``shape`` (nested dicts)."""
    for value in values:
        if isinstance(value, dict):
            for key, inner in value.items():
                sub = shape.setdefault(key, {})
                if isinstance(inner, dict):
                    _shape([inner], sub)
    return shape


def _flatten_by(obj: Dict[str, Any], shape: Dict[str, Any], prefix: str, out: Dict[str, Any]) -> None:
    """Flatten ``obj`` along the merged ``shape`` (sorted keys; absent leaves are ``None``)."""
    for key in sorted(shape):
        value = obj.get(key)
        col = _join(prefix, underscore(key))
        if shape[key] and not isinstance(value, list):
            _flatten_by(value if isinstance(value, dict) else {}, shape[key], col, out)
        elif col not in out:
            out[col] = json.dumps(value) if isinstance(value, list) else value


def _singular(list_key: str) -> str:
    return underscore(list_key[:-1] if list_key.endswith("s") else list_key)


def _row_list(node: Dict[str, Any], table: Optional[str]) -> Optional[str]:
    """The key of the row list to descend into at ``node``, or ``None`` when ``node`` is a row."""
    for key in sorted(node):
        if isinstance(node[key], list) and (key in _DESCEND or key == table):
            return key
    return None


def _explode(
    node: Dict[str, Any],
    list_key: str,
    prefix: str,
    carried: Dict[str, Any],
    leaves: set,
    table: Optional[str],
    rows: List[Dict[str, Any]],
) -> None:
    """Carry ``node``'s scalars (under ``prefix``) and recurse into ``node[list_key]``."""
    own: Dict[str, Any] = {}
    _flatten(node, prefix, own, skip=list_key)
    for col, value in own.items():
        leaf = col[len(prefix) :].lstrip("_")
        if leaf not in leaves:
            leaves = leaves | {leaf}
            carried = {**carried, col: value}
    child_prefix = _singular(list_key)
    elements = [e for e in node.get(list_key) or [] if isinstance(e, dict)]
    shape = _shape(elements, {})  # union of the rows' shapes -> one sorted column order
    for element in elements:
        next_key = None if list_key == table else _row_list(element, table)
        if next_key is None:
            row = dict(carried)
            _flatten_by(element, shape, "", row)
            rows.append(row)
        else:
            _explode(element, next_key, child_prefix, carried, leaves, table, rows)


def _rows(raw: Any, table: Optional[str]) -> List[Dict[str, Any]]:
    mrdata = raw.get("MRData") if isinstance(raw, dict) else None
    if not isinstance(mrdata, dict):
        return []
    table_key = next((k for k, v in mrdata.items() if k.endswith("Table") and isinstance(v, dict)), None)
    if table_key is None:
        return []
    node = mrdata[table_key]
    list_key = next((k for k in sorted(node) if isinstance(node[k], list)), None)
    if list_key is None:
        return []
    rows: List[Dict[str, Any]] = []
    _explode(node, list_key, "", {}, set(), table, rows)
    return rows


def parse_f1_mrdata(
    raw: Union[Dict[str, Any], None],
    table: Optional[str] = None,
    *,
    return_as_pandas: bool = False,
    columns: Optional[Sequence[str]] = None,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Flatten a Jolpica / Ergast ``MRData`` envelope into a tidy frame.

    Covers every ``f1`` route: the row list is found by descending from the
    envelope's ``<Table>`` through the known intermediate lists (``Races`` ->
    ``Results`` / ``QualifyingResults`` / ``SprintResults`` / ``PitStops`` /
    ``Laps`` -> ``Timings``; ``StandingsLists`` -> ``DriverStandings`` /
    ``ConstructorStandings``), carrying each ancestor's scalars onto the rows.

    Args:
        raw: a Jolpica JSON body (``{"MRData": {...}}``).
        table: stop descending at this list and make its elements the rows --
            ``"Races"`` on a results payload gives the race header rows instead of
            the classification. Default ``None`` descends to the innermost list.
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the route's documented column names (the generated wrappers pass
            their returns-schema); an empty payload then yields a zero-row frame with
            exactly these columns and their documented dtypes.

    Returns:
        One row per innermost record, snake_cased per ``f1-returns.md``, integer /
        number columns cast, ids pinned to ``Utf8``, list cells JSON-encoded. A
        zero-row frame when the payload is ``None``, empty (``Races: []`` on a
        non-sprint weekend) or malformed -- with the ``columns`` schema when given, so
        callers can chain without a null-check. Never raises on a bad payload.

    Example:
        Quick start::

            from sportsdataverse.f1 import f1_results, parse_f1_mrdata

            raw = f1_results(2024, 1, return_parsed=False)
            df = parse_f1_mrdata(raw)
            print(df.shape)  # (20, 39)

        Race header rows from the same payload::

            parse_f1_mrdata(raw, table="Races").select("season", "round", "race_name")

    See Also:
        * `Jolpica F1 API`_ -- the Ergast-compatible API this parses.

    .. _Jolpica F1 API: https://github.com/jolpica/jolpica-f1
    """
    rows = _rows(raw, table)
    if not rows:
        empty = pl.DataFrame({c: pl.Series(c, [], dtype=pl.String) for c in columns or []})
        return as_output(_typed(empty), return_as_pandas=return_as_pandas)
    cols: List[str] = []
    for row in rows:
        cols.extend(c for c in row if c not in cols)
    df = pl.DataFrame(
        {c: pl.Series(c, [row.get(c) for row in rows], dtype=pl.String) for c in cols},
    )
    return as_output(_typed(df), return_as_pandas=return_as_pandas)


def _typed(df: pl.DataFrame) -> pl.DataFrame:
    """Cast the documented integer / number columns of an all-``Utf8`` frame; pin ids to ``Utf8``."""
    cols = df.columns
    casts = [pl.col(c).cast(pl.Int64, strict=False) for c in cols if c in _INT and not _ID.search(c)]
    casts += [pl.col(c).cast(pl.Float64, strict=False) for c in cols if c in _FLOAT]
    df = df.with_columns(casts) if casts else df
    return to_utf8_ids(df, [c for c in cols if _ID.search(c)])
