"""Parser for the generated ``openligadb`` wrappers (api.openligadb.de).

OpenLigaDB is keyless and takes no query parameters; every route answers one of
three shapes, which this parser maps to rows as:

* **list** (leagues, groups, teams, table, goalgetters, season matches, matchday
  matches) -> one row per element;
* **single object** (``/getcurrentgroup/{league}``, ``/getmatchdata/{match_id}``,
  ``/getnextmatchbyleagueteam/...``) -> exactly one row, nested ``team1`` /
  ``team2`` / ``group`` objects flattened to ``team1_team_name``-style columns;
* **bare scalar** (``/getlastchangedate/...`` answers a quoted ISO timestamp)
  -> one row, one ``value`` column.

Nested ``matchResults`` and ``goals`` lists are JSON-encoded cells: their length
varies per match and flattening them would make the column set depend on how
many goals were scored.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, a zero-row frame (never an exception) on an empty or
malformed payload, snake_cased columns, ids pinned to ``Utf8``.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = ["parse_openligadb"]

_ID = re.compile(r"(^|_)(id|code)$")

SCALAR_COLUMN = "value"


def _as_rows(raw: Any) -> List[Dict[str, Any]]:
    """Apply the module row rule; ``[]`` for anything unusable."""
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    if isinstance(raw, dict):
        return [raw] if raw else []
    if isinstance(raw, (str, int, float, bool)) and str(raw).strip():
        # /getlastchangedate answers a bare quoted timestamp.
        return [{SCALAR_COLUMN: raw}]
    return []


def _encode(value: Any) -> Any:
    return json.dumps(value) if isinstance(value, (list, dict)) else value


def _to_frame(rows: List[Dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    pdf = pd.json_normalize(rows, sep="_")
    pdf.columns = [underscore(str(c)) for c in pdf.columns]
    for name in pdf.columns:
        if pdf[name].dtype == object:
            pdf[name] = pdf[name].map(_encode)
    df = pl.from_pandas(pdf)
    ids = []
    for name, dtype in df.schema.items():
        if not _ID.search(name) or dtype == pl.String:
            continue
        expr = pl.col(name)
        if dtype.is_float():
            expr = expr.cast(pl.Int64, strict=False)
        ids.append(expr.cast(pl.String).alias(name))
    return df.with_columns(ids) if ids else df


def parse_openligadb(
    raw: Union[Dict[str, Any], List[Any], str, None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse any OpenLigaDB body into a tidy frame.

    Args:
        raw: a decoded OpenLigaDB body -- a list of records, a single object, or
            the bare timestamp string ``/getlastchangedate`` returns.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record (one row for a single object or a bare scalar, in a
        ``value`` column), snake_cased, with ``id`` / ``*_id`` columns pinned to
        ``Utf8`` and nested ``matchResults`` / ``goals`` lists JSON-encoded. A
        zero-row frame when the payload is ``None``, empty or malformed.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            import polars as pl

            from sportsdataverse.soccer import openligadb_season_matches

            df = openligadb_season_matches(league_slug="bl1", season="2025")
            print(df.shape)

        Pipeline next step (one line)::

            df.select("match_id", "match_date_time", "team1_team_name", "team2_team_name")

    See Also:
        * `OpenLigaDB`_ -- the community API this family wraps.

    .. _OpenLigaDB: https://api.openligadb.de/index.html
    """
    df = _to_frame(_as_rows(raw))
    return df.to_pandas() if return_as_pandas else df
