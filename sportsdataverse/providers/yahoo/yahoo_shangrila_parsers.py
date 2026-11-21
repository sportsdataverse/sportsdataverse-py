"""Parsers for the generated Yahoo Sports wrappers (:mod:`sportsdataverse.providers.yahoo.yahoo_shangrila`).

The 105 wrapped Yahoo routes are shangrila GraphQL persisted queries. Every body
is ``{"data": {...}, "extensions": {...}}``. The rows of interest sit under one or
more collections in ``data``, sometimes behind single-key wrapper levels::

    {"data": {"leagues": [{"leaders": [ <row>, <row>, ... ]}]}}
    {"data": {"players":  [ <row>, <row>, ... ]}}
    {"data": {"statTypes": [...], "leagues": [{"leaders": [...]}]}}

:func:`_descend` walks down levels that have exactly ONE key -- and only into a
nested collection/object, never a scalar -- which lands on the row list without
guessing. :func:`parse_yahoo_shangrila` returns the first (usually only)
collection as a frame; :func:`parse_yahoo_shangrila_tables` returns one frame per
``data`` collection for the genuinely multi-table queries. The endpoint YAML picks
between them from the spec, so a caller never has to.

The editorial scoreboard / boxscore feed and its ``parse_yahoo_editorial`` parser
were removed after Yahoo retired the feed on 2026-11-20.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, zero-row frame (or an empty dict, for the dict-valued
parsers) on empty/malformed payloads, snake_cased columns via
:func:`sportsdataverse.dl_utils.underscore`, ``pandas.json_normalize`` for nested
flattening, list/dict cells stringified so polars accepts the frame.

**ID discipline:** Yahoo ships composite string ids (``nfl.p.12345``,
``ncaaf.g.202509200023``). They arrive as strings and are left ``Utf8`` -- no id
column is ever coerced through a float.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_yahoo_shangrila",
    "parse_yahoo_shangrila_tables",
]

Frame = Union[pl.DataFrame, pd.DataFrame]


def _pin_id_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """Force id-shaped columns to ``Utf8``, routing floats through ``Int64`` first.

    Yahoo ships most ids as dotted strings (``ncaaf.g.202509200023``), but a few --
    conference and subdivision ids in the scoreboard's ``divisions`` collection --
    arrive as bare JSON numbers. ``pandas.json_normalize`` types those as
    ``float64`` (numeric with nulls), which the object-column pass above never
    touches, so the SAME conference id landed as ``Float64 1.0`` in ``divisions``
    and ``Utf8 "1"`` in ``teams`` -- two columns that can never join, from one
    payload. Casting the float straight to text would give ``"1.0"``, which is the
    silent id corruption sdv-py's ``CLAUDE.md`` calls out, so an all-integral float
    goes through ``Int64`` on the way.

    A non-integral float under an id-shaped name is left alone: it is not an id,
    and truncating it would destroy data.

    Args:
        frame: A flattened frame from :func:`_rows_to_frame`.

    Returns:
        The frame with every ``id`` / ``*_id`` / ``*_ids`` column as ``Utf8``.
    """
    for col in frame.columns:
        if col != "id" and not col.endswith(("_id", "_ids")):
            continue
        dtype = frame.schema[col]
        if dtype == pl.Utf8:
            continue
        if dtype.is_float():
            values = frame[col].drop_nulls()
            if len(values) and not (values == values.round(0)).all():
                continue  # not an integer id -- leave it rather than truncate
            frame = frame.with_columns(frame[col].cast(pl.Int64).cast(pl.Utf8).alias(col))
        elif dtype.is_integer():
            frame = frame.with_columns(frame[col].cast(pl.Utf8).alias(col))
    return frame


def _rows_to_frame(rows: List[Any]) -> pl.DataFrame:
    """Flatten a list of JSON records into a tidy polars frame.

    Mirrors the On3/247 flatteners: ``json_normalize`` for nested dicts,
    ``underscore``-d column names with ``_2``-style suffixes on collision, and
    list/dict cells JSON-encoded so the pandas -> polars handoff never rejects a
    mixed-type column. Yahoo also uses dotted keys as field NAMES in the boxscore
    stat maps (``ncaaf.stat_type.102``), so non-word characters in a column name
    collapse to ``_`` before snake-casing. Cell VALUES are never rewritten.
    """
    if not rows:
        return pl.DataFrame()
    if not any(isinstance(r, dict) for r in rows):
        return pl.DataFrame({"value": [None if r is None else str(r) for r in rows]})
    normalized = [r if isinstance(r, dict) else {"value": r} for r in rows]
    df = pd.json_normalize(normalized, sep="_")
    seen: Dict[str, int] = {}
    cols: List[str] = []
    for c in df.columns:
        name = underscore(re.sub(r"\W+", "_", str(c))).strip("_")
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 1
        cols.append(name)
    df.columns = cols
    for c in df.columns:
        if df[c].dtype == object:
            # B-M17: a key absent from some rows is pandas' NaN: null, never the string "nan"; and a column
            # of flags stays Boolean instead of becoming "True" / "False" strings.
            cells = df[c].map(lambda v: None if isinstance(v, float) and v != v else v)
            if {type(v) for v in cells if v is not None} <= {bool}:
                df[c] = cells
                continue
            df[c] = cells.map(
                lambda v: (
                    json.dumps(v) if isinstance(v, (list, dict)) else (v if v is None or isinstance(v, str) else str(v))
                ),
            )
    return _pin_id_columns(pl.from_pandas(df))


def _descend(node: Any) -> List[Any]:
    """Walk down single-key wrapper levels to the list of row records.

    Yahoo wraps rows in one or more single-key envelopes: ``leagues`` is a
    one-element list whose only key is ``leaders``. Descending only through levels
    that carry exactly one key -- and only into a nested list/dict, never a scalar
    -- reaches the rows without guessing which key is "the data".

    Args:
        node: a ``data`` collection value (list or dict).

    Returns:
        The row records. A dict that is not a single-key wrapper becomes one row;
        anything unusable becomes ``[]``.
    """
    while True:
        if isinstance(node, list):
            dicts = [n for n in node if isinstance(n, dict)]
            # every element is the SAME single-key wrapper (one league, or several)
            if node and len(dicts) == len(node) and all(len(d) == 1 for d in dicts):
                keys = {next(iter(d)) for d in dicts}
                if len(keys) == 1:
                    inner = [d[next(iter(keys))] for d in dicts]
                    if all(isinstance(v, (list, dict)) for v in inner):
                        node = [row for v in inner for row in (v if isinstance(v, list) else [v])]
                        continue
            return node
        if isinstance(node, dict):
            if len(node) == 1:
                only = next(iter(node.values()))
                if isinstance(only, (list, dict)):
                    node = only
                    continue
            return [node] if node else []
        return []


def _tables(raw: Any) -> Dict[str, pl.DataFrame]:
    """``{snake_cased data key: frame}`` for a shangrila envelope (``{}`` when malformed).

    Keyed by the TOP-LEVEL ``data`` key rather than the key the rows were finally
    found under: top-level keys are unique by construction, whereas the descended
    names can repeat (``navDropdownTray`` reaches ``rankPolls`` three times).
    """
    data = (raw or {}).get("data") if isinstance(raw, dict) else None
    if not isinstance(data, dict):
        return {}
    return {underscore(key): _rows_to_frame(_descend(value)) for key, value in data.items()}


def parse_yahoo_shangrila(
    raw: Any,
    *,
    return_as_pandas: bool = False,
) -> Frame:
    """Parse a Yahoo shangrila persisted-query payload into one tidy frame.

    Args:
        raw: a ``{"data": {...}, "extensions": {...}}`` body as returned by
            :func:`sportsdataverse.providers.yahoo.yahoo_shangrila_runtime._get`.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record in the payload's first (for these endpoints, only)
        ``data`` collection, with ``json_normalize``-flattened snake_cased columns
        and list/dict cells JSON-encoded. Dotted Yahoo ids stay ``Utf8``. A
        zero-row frame when the payload is ``None`` / empty / malformed, or when
        Yahoo answered with a ``{"errors": [...]}`` GraphQL envelope.

    Example:
        Quick start::

            from sportsdataverse.providers.yahoo.yahoo_shangrila import yahoo_league_standings
            df = yahoo_league_standings(league="nfl", season=2024)
            print(df.shape)

        Pandas instead of polars::

            df_pd = yahoo_league_standings(league="nfl", season=2024, return_as_pandas=True)

    See Also:
        * `cfbfastR`_ -- R sister package for college football.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    tables = _tables(raw)
    df = next(iter(tables.values()), pl.DataFrame())
    return df.to_pandas() if return_as_pandas else df


def parse_yahoo_shangrila_tables(
    raw: Any,
    *,
    return_as_pandas: bool = False,
) -> Dict[str, Frame]:
    """Parse a multi-collection shangrila payload into one frame per collection.

    Used by the 25 queries whose ``data`` object carries more than one collection
    -- e.g. the legacy ``seasonStats*`` leaders queries return ``statTypes``
    (the stat dictionary) alongside ``leagues`` (the leaders themselves).

    Args:
        raw: a ``{"data": {...}, "extensions": {...}}`` body.
        return_as_pandas: return a dict of pandas DataFrames instead of polars.

    Returns:
        ``{snake_cased data key: DataFrame}``. Keys are the payload's top-level
        ``data`` keys (unique by construction), even when the rows were found one
        or two single-key wrapper levels below. An empty dict when the payload is
        ``None`` / empty / malformed.

    Example:
        Quick start::

            from sportsdataverse.providers.yahoo.yahoo_shangrila import yahoo_season_stats_football_passing_ncaaf
            tables = yahoo_season_stats_football_passing_ncaaf(season=2024)
            print(sorted(tables))

        Pipeline next step (one line)::

            tables["leagues"].filter(pl.col("player_display_name").is_not_null()).head()

    See Also:
        * `cfbfastR`_ -- R sister package for college football.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    tables = _tables(raw)
    return {k: v.to_pandas() for k, v in tables.items()} if return_as_pandas else dict(tables)
