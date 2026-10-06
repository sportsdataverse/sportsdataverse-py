"""Parser for the generated ``kalshi`` wrappers (Kalshi Trade API v2, market data).

Kalshi wraps each resource in a named key beside a paging ``cursor``, and
``/events`` ships a second list (``milestones``) next to the one you asked for,
so "the single list key" is not a usable rule. Rows come from the first key
present in :data:`RESOURCE_KEYS`, in that order:

* **list** under the resource key (``events``, ``markets``, ``trades``,
  ``series``) -> one row per element;
* **object** under the resource key (``market``, ``event``, and ``series`` on the
  single-series route, which reuses the plural key for one object) -> one row;
* **order book** (``{"orderbook_fp": {"yes_dollars": [[price, size], ...],
  "no_dollars": [...]}}``) -> one row per price level, tagged ``side`` = ``yes``
  / ``no``, with ``price`` and ``size`` kept as the **strings** the API sends
  (they are fixed-point dollar amounts; a float would round them);
* **no resource key at all** (``/exchange/status`` is flat scalars) -> one row.

An order-book row carries only ``side`` / ``price`` / ``size``: the market is a path
parameter and appears nowhere in the body, so add the ticker yourself before concatenating
two books. Polymarket's book, by contrast, repeats its own ``market`` / ``asset_id`` scalars
on every level, because that host puts them in the body.

Tickers (``ticker``, ``event_ticker``, ``series_ticker``) are identifiers such as
``KXNFLGAME-26OCT08TBDAL-DAL`` and are pinned to ``Utf8`` alongside the id
columns.

Only market data is wrapped: ``/portfolio/*`` and order placement need an
RSA-signed ``KALSHI-ACCESS-KEY`` and are out of scope.

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

__all__ = ["parse_kalshi"]

_ID = re.compile(r"(^|_)(id|code|ticker)$")

# Checked in order: /events ships `milestones` beside `events`, and
# /events/{event_ticker} ships `markets` beside `event`.
RESOURCE_KEYS = ("events", "event", "markets", "market", "trades", "series")

_BOOK_KEY = "orderbook_fp"
_BOOK_SIDES = {"yes_dollars": "yes", "no_dollars": "no"}


def _book_rows(book: Dict[str, Any]) -> List[Dict[str, Any]]:
    """One row per price level: side, price, size -- prices stay strings."""
    rows: List[Dict[str, Any]] = []
    for key, side in _BOOK_SIDES.items():
        for level in book.get(key) or []:
            if isinstance(level, (list, tuple)) and len(level) >= 2:
                rows.append({"side": side, "price": str(level[0]), "size": str(level[1])})
    return rows


def _as_rows(raw: Any) -> List[Dict[str, Any]]:
    """Apply the module row rule; ``[]`` for anything unusable."""
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    if not isinstance(raw, dict) or not raw:
        return []
    book = raw.get(_BOOK_KEY)
    if isinstance(book, dict):
        return _book_rows(book)
    for key in RESOURCE_KEYS:
        value = raw.get(key)
        if isinstance(value, list):
            return [r for r in value if isinstance(r, dict)]
        if isinstance(value, dict):
            return [value]
    scalars = {k: v for k, v in raw.items() if not isinstance(v, (list, dict))}
    # /exchange/status has no resource key; its own scalars are the row. A body of
    # nothing but a cursor is an exhausted page, not a row.
    if scalars and set(scalars) != {"cursor"}:
        return [raw]
    return []


def _encode(value: Any) -> Any:
    return json.dumps(value) if isinstance(value, (list, dict)) else value


def _to_frame(rows: List[Dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    pdf = pd.json_normalize(rows, sep="_")
    pdf.columns = [underscore(str(c)) for c in pdf.columns]
    # Two source shapes make ``pl.from_pandas`` raise, and this parser promises a frame:
    # two keys that snake_case to one name (``pdf[name]`` would be a DataFrame), and one key
    # the source typed inconsistently across records. A missing key is NOT a second type --
    # json_normalize fills it with NaN, and counting that would restringify every boolean
    # column absent from one record.
    if pdf.columns.duplicated().any():
        pdf = pdf.loc[:, ~pdf.columns.duplicated()]
    for name in pdf.columns:
        col = pdf[name]
        if col.dtype == object:
            col = col.map(_encode)
            present = col[col.notna()]
            if present.map(type).nunique() > 1:
                col = col.map(lambda v: v if v is None or (isinstance(v, float) and v != v) else str(v))
            pdf[name] = col
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


def parse_kalshi(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse any Kalshi market-data body into a tidy frame.

    Args:
        raw: a decoded Kalshi v2 body -- a resource envelope, an order book, or
            the flat ``/exchange/status`` object (see the module docstring).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per event / market / trade / series / price level, snake_cased,
        with id and ticker columns pinned to ``Utf8`` and nested lists
        JSON-encoded. A zero-row frame when the payload is ``None``, empty,
        malformed, or an exhausted page carrying only a cursor.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.odds import kalshi_markets

            df = kalshi_markets(event_ticker="KXNFLGAME-26OCT08TBDAL", limit="100")
            print(df.shape)

        Pipeline next step (one line)::

            df.select("ticker", "title", "yes_bid_dollars", "yes_ask_dollars", "volume_fp")

    See Also:
        * `Kalshi API docs`_ -- the market-data routes this family wraps.

    .. _Kalshi API docs: https://docs.kalshi.com/
    """
    df = _to_frame(_as_rows(raw))
    return df.to_pandas() if return_as_pandas else df
