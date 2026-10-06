"""Parser for the generated ``polymarket`` wrappers (gamma + clob read APIs).

Polymarket serves two keyless read hosts and this one parser covers both:

* **list** (gamma ``/markets``, ``/events``, ``/tags``) -> one row per element;
* **cursor envelope** (clob ``/markets`` is ``{"data": [...], "next_cursor",
  "limit", "count"}``) -> rows from ``data``; the cursor is a parameter the
  caller passes on the next call, never followed here;
* **order book** (clob ``/book`` is ``{"bids": [...], "asks": [...]}`` plus
  scalar siblings) -> one row per price level, tagged ``side`` = ``bid`` /
  ``ask``, with the book's scalars (``market``, ``asset_id``, ``timestamp``,
  ``tick_size``, ...) repeated on every row so a level is self-describing;
* **object** (gamma ``/markets/{id}``) -> exactly one row;
* **scalar dict** (clob ``/midpoint`` is ``{"mid": "0.5"}``, ``/price`` is
  ``{"price": "0.5"}``) -> exactly one row.

Two source quirks are handled here. Gamma's single-market body carries a
``$schema`` key, which is JSON-Schema metadata rather than market data and is
dropped. And Polymarket ids are **long**: ``asset_id`` / ``token_id`` are
77-digit decimal strings and ``market`` / ``conditionId`` are 0x hashes, so
every id column is pinned to ``Utf8`` -- a float cast renders ``1.494e+76``.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, a zero-row frame (never an exception) on an empty or
malformed payload, snake_cased columns.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = ["parse_polymarket"]

_ID = re.compile(r"(^|_)(id|code|ticker)$")

# The one list key in a cursor envelope. Checked before the generic rule so the
# envelope's own scalars (next_cursor, limit, count) never become a row.
_DATA_KEY = "data"
_BOOK_SIDES = {"bids": "bid", "asks": "ask"}


def _book_rows(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    """One row per price level, tagged by side, carrying the book's scalars."""
    scalars = {k: v for k, v in raw.items() if k not in _BOOK_SIDES and not isinstance(v, (list, dict))}
    rows: List[Dict[str, Any]] = []
    for key, side in _BOOK_SIDES.items():
        for level in raw.get(key) or []:
            if isinstance(level, dict):
                rows.append({**scalars, "side": side, **level})
    return rows


def _as_rows(raw: Any) -> List[Dict[str, Any]]:
    """Apply the module row rule; ``[]`` for anything unusable."""
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    if not isinstance(raw, dict) or not raw:
        return []
    body = {k: v for k, v in raw.items() if not k.startswith("$")}
    if set(_BOOK_SIDES) & set(body):
        return _book_rows(body)
    if isinstance(body.get(_DATA_KEY), list):
        return [r for r in body[_DATA_KEY] if isinstance(r, dict)]
    return [body] if body else []


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


def parse_polymarket(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse any Polymarket gamma or clob body into a tidy frame.

    Args:
        raw: a decoded gamma or clob body -- a list, a ``{"data": [...]}`` cursor
            envelope, an order book, a single market object, or a one-value dict
            (see the module docstring for the row rule).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per market / event / tag / price level, snake_cased, with every
        id column pinned to ``Utf8`` and nested lists JSON-encoded. A zero-row
        frame when the payload is ``None``, empty or malformed.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            import polars as pl

            from sportsdataverse.odds import polymarket_gamma_markets

            df = polymarket_gamma_markets(limit="20", closed="false", tag_slug="sports")
            print(df.shape)

        Pipeline next step (one line)::

            df.select("id", "question", "volume24hr", "end_date")

    See Also:
        * `Polymarket docs`_ -- the read APIs this family wraps.

    .. _Polymarket docs: https://docs.polymarket.com/
    """
    df = _to_frame(_as_rows(raw))
    return df.to_pandas() if return_as_pandas else df
