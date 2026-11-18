"""Parser for the generated ``polymarket`` wrappers (gamma + clob + US read APIs).

Polymarket serves three keyless read hosts and this one parser covers all of them:

* **list** (gamma ``/markets``, ``/events``, ``/tags``) -> one row per element;
* **cursor envelope** (clob ``/markets`` is ``{"data": [...], "next_cursor",
  "limit", "count"}``) -> rows from ``data``; the cursor is a parameter the
  caller passes on the next call, never followed here;
* **order book** (clob ``/book`` is ``{"bids": [...], "asks": [...]}`` plus
  scalar siblings; US ``/v1/markets/{slug}/book`` is ``{"marketData": {"bids":
  [...], "offers": [...], ...}}``) -> one row per price level, tagged ``side`` =
  ``bid`` / ``ask`` (US ``offers`` are asks), with the book's scalars
  (``market``, ``asset_id``, ``timestamp``, ``tick_size``; US ``marketSlug``,
  ``state``) repeated on every row so a level is self-describing. Nested
  objects beside the levels (the US ``stats``) are dropped, and a book with no
  levels is zero rows;
* **single-key envelope** (US ``/v1/markets`` is ``{"markets": [...]}``, an
  ``{"events": [...]}`` list likewise) -> one row per element; US
  ``/v1/markets/{slug}/bbo`` is ``{"marketData": {...}}`` -> exactly one row of
  the inner object, so its ``{value, currency}`` prices flatten to
  ``current_px_value`` / ``current_px_currency``; US ``/v1/market/id/{id}`` and
  ``/v1/market/slug/{slug}`` are ``{"market": {...}}`` -> one row;
* **object** (gamma ``/markets/{id}``) -> exactly one row;
* **scalar dict** (clob ``/midpoint`` is ``{"mid": "0.5"}``, ``/price`` is
  ``{"price": "0.5"}``) -> exactly one row.

Two source quirks are handled here. Gamma's single-market body carries a
``$schema`` key, which is JSON-Schema metadata rather than market data and is
dropped. And Polymarket ids are **long**: ``asset_id`` / ``token_id`` are
77-digit decimal strings and ``market`` / ``conditionId`` are 0x hashes, so
every id column is pinned to ``Utf8``. The pin preserves what arrives: these hosts quote
their ids, and one ever sent unquoted would already have lost precision inside
``response.json()`` before a parser saw it.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, a zero-row frame (never an exception) on an empty or
malformed payload, snake_cased columns.
"""

from __future__ import annotations

import json
import re
import warnings
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore
from sportsdataverse.errors import SchemaDriftWarning, _data_only

__all__ = ["parse_polymarket"]

_ID = re.compile(r"(^|_)(id|code|ticker)$")

# The one list key in a cursor envelope. Checked before the generic rule so the
# envelope's own scalars (next_cursor, limit, count) never become a row.
_DATA_KEY = "data"
# Polymarket US names its asks ``offers``; both books tag them ``ask`` so a clob and a US book stack.
_BOOK_SIDES = {"bids": "bid", "asks": "ask", "offers": "ask"}
# The single keys that wrap a payload, and the container each must hold; only these are unwrapped, so a future
# one-key body is never guessed at.
_ENVELOPE_KEYS = {"markets": list, "events": list, "marketData": dict, "market": dict}
# Decimal columns that arrive as a bare JSON number, pinned Float64 so the dtype never depends on the value. Polymarket
# US's own schema types both decimal: ``/settlement`` answers ``{"settlement": 1}``, and ``minimumTradeQty`` is 1 on
# most markets but 0.01 on the captured expired game.
_FLOAT_COLS = {"settlement", "minimum_trade_qty"}


def _book_rows(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    """One row per price level, tagged by side, carrying the book's scalars."""
    scalars = {k: v for k, v in raw.items() if k not in _BOOK_SIDES and not isinstance(v, (list, dict))}
    rows: List[Dict[str, Any]] = []
    for key, side in _BOOK_SIDES.items():
        levels = raw.get(key)
        for level in levels if isinstance(levels, list) else []:  # a malformed side is no levels, never a raise
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
    if len(body) == 1 and set(body) <= _ENVELOPE_KEYS.keys():
        # A known single-key envelope wraps the payload (US ``{"markets": [...]}``, ``{"marketData": {...}}``).
        # Any other one-key body (clob ``{"mid": "0.5"}``, an ``{"error": {...}}`` object) stays one row.
        ((key, inner),) = body.items()
        # Same rows with or without the envelope: drop the empty ``{}`` records a bare list loses in
        # ``_data_only``. The wrong container (``{"markets": {...}}``, ``{"marketData": [...]}``, ``null``) is
        # malformed: zero rows.
        if not isinstance(inner, _ENVELOPE_KEYS[key]):
            return []
        if isinstance(inner, list):
            return [r for r in inner if isinstance(r, dict) and r]
        if set(_BOOK_SIDES) & set(inner):  # US ``/book``: ``{"marketData": {"bids": [...], "offers": [...], ...}}``
            return _book_rows(inner)
        return [inner] if inner else []
    return [body] if body else []


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
    df = df.with_columns(ids) if ids else df
    return _pin_floats(df)


def _pin_floats(df: pl.DataFrame) -> pl.DataFrame:
    """Cast :data:`_FLOAT_COLS` to Float64; refuse (keep as sent, warn) a column the cast would null or a boolean."""
    pinned, refused = [], []
    for name in sorted(_FLOAT_COLS & set(df.columns)):
        src = df[name]
        out = src.cast(pl.Float64, strict=False)
        if src.dtype == pl.Boolean or out.null_count() != src.null_count():
            refused.append(name)
        else:
            pinned.append(out)
    if refused:
        warnings.warn(
            f"parse_polymarket: columns {refused} hold values that are not numbers; kept as they came",
            SchemaDriftWarning,
            stacklevel=5,  # _pin_floats < _to_frame < parse_polymarket < _data_only < caller
        )
    return df.with_columns(pinned) if pinned else df


@_data_only
def parse_polymarket(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse any Polymarket gamma, clob or US body into a tidy frame.

    Args:
        raw: a decoded gamma, clob or US body -- a list, a ``{"data": [...]}`` cursor
            envelope, a single-key ``{"markets": [...]}`` / ``{"marketData": {...}}``
            envelope, an order book, a single market object, or a one-value dict
            (see the module docstring for the row rule).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per market / event / tag / price level, snake_cased, with every
        id column pinned to ``Utf8`` and nested lists JSON-encoded. A zero-row
        frame when the payload is ``None``, empty or malformed.

    Example:
        Quick start::

            from sportsdataverse.odds import polymarket_gamma_markets

            df = polymarket_gamma_markets(limit="20", closed="false", order="volume24hr")
            print(df.shape)

        Pipeline next step (one line)::

            df.select("id", "question", "volume24hr", "end_date")

    See Also:
        * `Polymarket docs`_ -- the read APIs this family wraps.

    .. _Polymarket docs: https://docs.polymarket.com/
    """
    df = _to_frame(_as_rows(raw))
    return df.to_pandas() if return_as_pandas else df
