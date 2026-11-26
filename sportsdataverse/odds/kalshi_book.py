"""Asks and fill prices from a Kalshi order book.

A Kalshi market is binary: a YES contract pays $1 if the event happens, a NO contract
pays $1 if it does not, so one YES plus one NO always settle to exactly $1. The order
book (``kalshi_orderbook``) therefore lists **bids only**, on both sides: an offer to
sell YES at ``p`` is the same order as a bid for NO at ``1 - p``. So:

* the YES ask ladder is the NO bid ladder at ``1 - price``, same sizes;
* the NO ask ladder is the YES bid ladder at ``1 - price``.

Each side's levels arrive sorted from the lowest price up, so the best bid is the
**last** level, not the first. Prices are fixed-point dollars with four decimals; a
derived ask is rounded back to four decimals so ``1 - 0.51`` reads ``0.49``.

:func:`kalshi_book_quotes` lays out all four ladders. :func:`kalshi_fill_price` walks
one of them for a given number of contracts and returns the average price you would
pay or receive. Neither places or simulates an order on the exchange, and neither
counts Kalshi's trading fee.
"""

from __future__ import annotations

import math
import numbers
from typing import Any, Union

import pandas as pd
import polars as pl

from sportsdataverse.odds.kalshi_parsers import parse_kalshi

__all__ = ["kalshi_book_quotes", "kalshi_fill_price"]

_SIDES = ("yes", "no")
_ACTIONS = ("buy", "sell")
_QUOTES_SCHEMA = {
    "side": pl.String,
    "quote": pl.String,
    "level": pl.Int32,
    "price": pl.Float64,
    "size": pl.Float64,
    "cum_size": pl.Float64,
}
_FILL_SCHEMA = {
    "side": pl.String,
    "action": pl.String,
    "size_requested": pl.Float64,
    "size_filled": pl.Float64,
    "filled": pl.Boolean,
    "avg_price": pl.Float64,
    "best_price": pl.Float64,
    "worst_price": pl.Float64,
    "slippage": pl.Float64,
    "cost": pl.Float64,
    "levels_used": pl.Int32,
}


def _bids(book: Any) -> pl.DataFrame:
    """``side`` / ``price`` / ``size`` bid levels as floats, from a raw book or a parsed one."""
    if isinstance(book, dict):
        book = parse_kalshi(book)
    elif isinstance(book, pd.DataFrame):
        book = pl.from_pandas(book)
    if not isinstance(book, pl.DataFrame):
        raise TypeError(f"book must be a kalshi_orderbook dict or DataFrame, got {type(book).__name__}")
    if book.height == 0:
        return pl.DataFrame(schema={"side": pl.String, "price": pl.Float64, "size": pl.Float64})
    missing = {"side", "price", "size"} - set(book.columns)
    if missing:
        raise ValueError(f"book lacks the order-book columns {sorted(missing)}")
    # A level without a readable price or size is not a level (non-strict cast -> null -> dropped).
    bids = book.select(
        pl.col("side").cast(pl.String),
        pl.col("price").cast(pl.Float64, strict=False),
        pl.col("size").cast(pl.Float64, strict=False),
    ).filter(pl.col("side").is_in(_SIDES) & pl.col("price").is_not_null() & (pl.col("size") > 0))
    if not bids["price"].is_between(0, 1).all():
        raise ValueError("book prices must be dollars from 0 to 1 (Kalshi *_dollars levels), not cents")
    return bids


def kalshi_book_quotes(
    book: Union[dict, pl.DataFrame, pd.DataFrame],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Bid and ask ladders for both sides of a Kalshi market, asks derived from the opposite bids.

    Kalshi's book holds bids only. A NO bid at ``p`` is a YES offer at ``1 - p``, so the
    YES asks are the NO bids flipped (and the NO asks the YES bids flipped), with the
    same sizes.

    Args:
        book: one market's order book: the raw ``kalshi_orderbook(ticker, return_parsed=False)``
            dict, or the parsed frame ``kalshi_orderbook(ticker)`` returns (``side`` /
            ``price`` / ``size``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per price level per side and quote, best level first (bids high to low,
        asks low to high): ``side``, ``quote``, ``level``, ``price``, ``size``,
        ``cum_size``. An empty book gives a zero-row frame with these columns.

    Raises:
        TypeError: ``book`` is neither a dict nor a DataFrame.
        ValueError: a DataFrame ``book`` lacks ``side`` / ``price`` / ``size``, or a price
            falls outside 0 to 1 (a book in cents).

    Example:
        Quick start::

            from sportsdataverse.odds import kalshi_book_quotes, kalshi_orderbook

            book = kalshi_orderbook(ticker="KXNFLGAME-26DEC07DALSEA-SEA")
            quotes = kalshi_book_quotes(book)

        Pipeline next step (one line)::

            quotes.filter(level=1)

    See Also:
        * `Kalshi API docs`_ -- the keyless order-book route.

    .. _Kalshi API docs: https://docs.kalshi.com/
    """
    bids = _bids(book)
    flipped = bids.select(
        pl.when(pl.col("side") == "yes").then(pl.lit("no")).otherwise(pl.lit("yes")).alias("side"),
        (1 - pl.col("price")).round(4).alias("price"),
        "size",
    )
    ladders = pl.concat(
        [
            bids.with_columns(pl.lit("bid").alias("quote")).sort("price", descending=True),
            flipped.with_columns(pl.lit("ask").alias("quote")).sort("price"),
        ]
    )
    out = (
        ladders.with_columns(
            pl.int_range(1, pl.len() + 1, dtype=pl.Int32).over("side", "quote").alias("level"),
            pl.col("size").cum_sum().over("side", "quote").alias("cum_size"),
        )
        .sort("side", "quote", "level", descending=[True, False, False])
        .select(list(_QUOTES_SCHEMA))
        .cast(_QUOTES_SCHEMA)
    )
    return out.to_pandas() if return_as_pandas else out


def kalshi_fill_price(
    book: Union[dict, pl.DataFrame, pd.DataFrame],
    size: float,
    *,
    side: str = "yes",
    action: str = "buy",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Average price to buy or sell ``size`` contracts against a Kalshi order book.

    Buying walks that side's asks from the cheapest up; selling walks its bids from the
    highest down. Each level fills up to its size before the next is touched. When the
    book runs out first, the row reports the part that fills and ``filled`` is False.
    Kalshi's trading fee is not included.

    Args:
        book: one market's order book, raw dict or parsed frame, as for :func:`kalshi_book_quotes`.
        size: contracts to fill; must be positive. Kalshi sizes are fixed-point, so a
            fraction is allowed.
        side: ``yes`` or ``no``.
        action: ``buy`` (lift the asks) or ``sell`` (hit the bids).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row: ``side``, ``action``, ``size_requested``, ``size_filled``, ``filled``,
        ``avg_price``, ``best_price``, ``worst_price``, ``slippage``, ``cost`` and
        ``levels_used``. Prices are null when nothing fills.

    Raises:
        ValueError: ``size`` is not a positive finite number, ``side`` / ``action`` is not
            one of the values above, or ``book`` fails as in :func:`kalshi_book_quotes`.
        TypeError: ``book`` is neither a dict nor a DataFrame.

    Example:
        Quick start::

            from sportsdataverse.odds import kalshi_fill_price, kalshi_orderbook

            book = kalshi_orderbook(ticker="KXNFLGAME-26DEC07DALSEA-SEA")
            fill = kalshi_fill_price(book, 500, side="yes", action="buy")

        Pipeline next step (one line)::

            fill.select("avg_price", "slippage", "filled")

    See Also:
        * `Kalshi API docs`_ -- the keyless order-book route.

    .. _Kalshi API docs: https://docs.kalshi.com/
    """
    if side not in _SIDES:
        raise ValueError(f"side must be 'yes' or 'no', got {side!r}")
    if action not in _ACTIONS:
        raise ValueError(f"action must be 'buy' or 'sell', got {action!r}")
    if isinstance(size, bool) or not isinstance(size, numbers.Real) or not (0 < size < math.inf):
        raise ValueError(f"size must be a positive finite number of contracts, got {size!r}")
    quote = "ask" if action == "buy" else "bid"
    ladder = kalshi_book_quotes(book).filter(side=side, quote=quote)

    left, cost, used, worst = float(size), 0.0, 0, None
    for price, level_size in ladder.select("price", "size").iter_rows():
        take = min(left, level_size)
        cost += take * price
        left -= take
        used, worst = used + 1, price
        if left <= 1e-9:  # float residue from fixed-point sizes; the order is filled
            left = 0.0
            break
    filled_size = float(size) - left
    best: float | None = ladder["price"][0] if ladder.height else None
    avg: float | None = cost / filled_size if filled_size > 0 else None
    slippage = None
    if avg is not None and best is not None:
        # Positive = worse than the touch: paid above the best ask, or sold below the best bid.
        slippage = avg - best if action == "buy" else best - avg
    row = {
        "side": side,
        "action": action,
        "size_requested": float(size),
        "size_filled": filled_size,
        "filled": left <= 0,
        "avg_price": avg,
        "best_price": best,
        "worst_price": worst,
        "slippage": slippage,
        "cost": cost if filled_size > 0 else None,
        "levels_used": used,
    }
    out = pl.DataFrame([row], schema=_FILL_SCHEMA)
    return out.to_pandas() if return_as_pandas else out
