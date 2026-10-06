"""Parser for the generated ``thesportsdb`` wrappers (TheSportsDB API v1).

Every route answers a **single-key envelope** whose key names the resource and
varies by route -- ``sports``, ``leagues``, ``teams``, ``events``, ``table``, and
inconsistently ``player`` (``/lookup_all_players.php``, ``/searchplayers.php``)
versus ``players`` (``/lookupplayer.php``). Rows are that one list. A lookup with
no results answers the key with ``null`` rather than ``[]``, which is a zero-row
frame, not an error.

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

__all__ = ["parse_thesportsdb"]

_ID = re.compile(r"(^|_)(id|code)$")


def _as_rows(raw: Any) -> List[Dict[str, Any]]:
    """Rows from the single-key envelope; ``[]`` for anything unusable."""
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    if not isinstance(raw, dict) or not raw:
        return []
    for value in raw.values():
        if isinstance(value, list):
            return [r for r in value if isinstance(r, dict)]
        if isinstance(value, dict):
            return [value]
    # Every value was a scalar or null (``{"teams": null}``): nothing to tabulate.
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


def parse_thesportsdb(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse any TheSportsDB v1 body into a tidy frame.

    Args:
        raw: a decoded TheSportsDB body -- the single-key envelope every route
            returns (see the module docstring for the key-per-route list).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record in the envelope's list, snake_cased, with ``id`` /
        ``*_id`` columns pinned to ``Utf8`` and nested lists JSON-encoded. A
        zero-row frame when the payload is ``None``, empty, malformed, or the
        envelope key is ``null`` (a no-results lookup).

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            import polars as pl

            from sportsdataverse.thesportsdb import thesportsdb_league_teams

            df = thesportsdb_league_teams(l="English Premier League")
            print(df.shape)

        Pipeline next step (one line)::

            df.select("id_team", "str_team", "str_stadium")

    See Also:
        * `TheSportsDB API docs`_ -- the route reference this family wraps.

    .. _TheSportsDB API docs: https://www.thesportsdb.com/free_sports_api
    """
    df = _to_frame(_as_rows(raw))
    return df.to_pandas() if return_as_pandas else df
