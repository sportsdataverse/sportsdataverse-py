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
* keys are visited in sorted order, so the column order is deterministic.

Ergast serializes **every** value as a string. Columns documented as integers or
numbers (``season``, ``round``, ``position``, ``points``, ``laps``, ``grid``,
``time_millis``, circuit latitude/longitude, ...) are cast; ids (``driver_id``,
``constructor_id``, ``circuit_id``, ``status_id``) stay ``Utf8`` join keys.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, a zero-row frame (never an exception) on an empty or
malformed payload, and snake_cased columns.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import polars as pl

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
_CAMEL = re.compile(r"(?<!^)(?=[A-Z])")


def _snake(key: str) -> str:
    return _CAMEL.sub("_", key).lower()


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
        col = _join(prefix, _snake(key))
        if isinstance(value, dict):
            _flatten(value, col, out)
        elif col not in out:
            out[col] = json.dumps(value) if isinstance(value, list) else value


def _singular(list_key: str) -> str:
    return _snake(list_key[:-1] if list_key.endswith("s") else list_key)


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
    for element in node.get(list_key) or []:
        if not isinstance(element, dict):
            continue
        next_key = None if list_key == table else _row_list(element, table)
        if next_key is None:
            row = dict(carried)
            _flatten(element, "", row)
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

    Returns:
        One row per innermost record, snake_cased per ``f1-returns.md``, integer /
        number columns cast, ids pinned to ``Utf8``, list cells JSON-encoded. A
        zero-row frame when the payload is ``None``, empty (``Races: []`` on a
        non-sprint weekend) or malformed -- callers can chain without a null-check.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

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
        return as_output(pl.DataFrame(), return_as_pandas=return_as_pandas)
    # Column order: a column first seen on a later row (a sprint weekend's
    # ``sprint_date`` on the schedule) slots in after its predecessor in that row.
    cols: List[str] = []
    for row in rows:
        prev = ""
        for col in row:
            if col not in cols:
                cols.insert(cols.index(prev) + 1 if prev in cols else len(cols), col)
            prev = col
    df = pl.DataFrame(
        {c: pl.Series(c, [row.get(c) for row in rows], dtype=pl.String) for c in cols},
    )
    casts = [pl.col(c).cast(pl.Int64, strict=False) for c in cols if c in _INT and not _ID.search(c)]
    casts += [pl.col(c).cast(pl.Float64, strict=False) for c in cols if c in _FLOAT]
    df = df.with_columns(casts) if casts else df
    df = to_utf8_ids(df, [c for c in cols if _ID.search(c)])
    return as_output(df, return_as_pandas=return_as_pandas)
