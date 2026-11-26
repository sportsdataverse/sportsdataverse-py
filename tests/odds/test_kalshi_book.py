"""Asks from the opposite bids + fill prices, on full Kalshi books captured 2026-11-26 (tests/fixtures/kalshi/book)."""

import json
import pathlib

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.odds import kalshi_book_quotes, kalshi_fill_price
from sportsdataverse.odds.kalshi_parsers import parse_kalshi

BOOKS = pathlib.Path(__file__).parent.parent / "fixtures" / "kalshi" / "book"
TICKERS = ["KXNFLGAME-26NOV26CHIDET-DET", "KXNFLGAME-26NOV27DENPIT-DEN"]
# /markets reports touch sizes that can lag the live book; this pair was captured with no trade in between.
SIZES_CONSISTENT = "KXNFLGAME-26NOV27DENPIT-DEN"


def _load(ticker, kind):
    return json.loads((BOOKS / f"{ticker}.{kind}.json").read_text(encoding="utf-8"))


@pytest.fixture(params=TICKERS)
def captured(request):
    return _load(request.param, "orderbook"), _load(request.param, "market")["market"]


def _top(quotes, side, quote):
    return quotes.filter(side=side, quote=quote, level=1).row(0, named=True)


def test_top_of_book_matches_the_markets_own_quotes(captured):
    """The derived asks equal the ask Kalshi itself reports on /markets/{ticker}."""
    book, market = captured
    q = kalshi_book_quotes(book)
    yes_ask, no_ask = _top(q, "yes", "ask"), _top(q, "no", "ask")
    yes_bid, no_bid = _top(q, "yes", "bid"), _top(q, "no", "bid")
    assert yes_ask["price"] == float(market["yes_ask_dollars"])
    assert no_ask["price"] == float(market["no_ask_dollars"])
    assert yes_bid["price"] == float(market["yes_bid_dollars"])
    assert no_bid["price"] == float(market["no_bid_dollars"])
    if market["ticker"] == SIZES_CONSISTENT:
        assert yes_ask["size"] == float(market["yes_ask_size_fp"])
        assert yes_bid["size"] == float(market["yes_bid_size_fp"])
    # A YES ask is the NO bid seen from the other side.
    assert round(yes_ask["price"] + no_bid["price"], 4) == 1.0


def test_ladders_keep_every_level_in_best_first_order(captured):
    book, _ = captured
    q = kalshi_book_quotes(book)
    n_yes, n_no = len(book["orderbook_fp"]["yes_dollars"]), len(book["orderbook_fp"]["no_dollars"])
    counts = {(s, k): n for s, k, n in q.group_by("side", "quote").len().iter_rows()}
    assert counts == {("yes", "bid"): n_yes, ("no", "ask"): n_yes, ("no", "bid"): n_no, ("yes", "ask"): n_no}
    for (side, quote), part in q.group_by("side", "quote"):
        prices = part.sort("level")["price"]
        assert prices.is_sorted(descending=quote == "bid")
        ordered = part.sort("level")
        # Running totals restart on every ladder: level 1 holds its own size, the last level the ladder's depth.
        assert ordered["cum_size"][0] == ordered["size"][0]
        assert ordered["cum_size"][-1] == pytest.approx(ordered["size"].sum())
        assert part["level"].min() == 1 and part["level"].max() == part.height
    assert q.schema["level"] == pl.Int32 and q.schema["price"] == pl.Float64


def test_raw_parsed_and_pandas_books_agree(captured):
    book, _ = captured
    raw = kalshi_book_quotes(book)
    assert kalshi_book_quotes(parse_kalshi(book)).equals(raw)
    assert kalshi_book_quotes(parse_kalshi(book, return_as_pandas=True)).equals(raw)
    shuffled = parse_kalshi(book).sample(fraction=1, shuffle=True, seed=0)
    assert kalshi_book_quotes(shuffled).equals(raw)  # level order comes from prices, never arrival order
    out = kalshi_book_quotes(book, return_as_pandas=True)
    assert isinstance(out, pd.DataFrame) and len(out) == raw.height
    fill = kalshi_fill_price(book, 10, return_as_pandas=True)
    assert isinstance(fill, pd.DataFrame) and len(fill) == 1


def _walk(levels, size):
    """Independent walk over the raw JSON levels: (price, size) pairs, best first."""
    left, cost = size, 0.0
    for price, avail in levels:
        take = min(left, avail)
        cost, left = cost + take * price, left - take
        if left <= 0:
            break
    return cost


def test_buy_yes_walks_the_flipped_no_bids(captured):
    book, _ = captured
    no_bids = sorted(((float(p), float(s)) for p, s in book["orderbook_fp"]["no_dollars"]), reverse=True)
    asks = [(round(1 - p, 4), s) for p, s in no_bids]
    size = asks[0][1] + asks[1][1] + asks[2][1] / 2  # two full levels and half of the third
    fill = kalshi_fill_price(book, size, side="yes", action="buy").row(0, named=True)
    assert fill["filled"] is True and fill["levels_used"] == 3
    assert fill["best_price"] == asks[0][0] and fill["worst_price"] == asks[2][0]
    assert fill["cost"] == pytest.approx(_walk(asks, size))
    assert fill["avg_price"] == pytest.approx(fill["cost"] / size)
    assert fill["slippage"] > 0


def test_sell_yes_walks_the_yes_bids_down(captured):
    book, market = captured
    best_size = float(market["yes_bid_size_fp"])
    touch = kalshi_fill_price(book, best_size, side="yes", action="sell").row(0, named=True)
    assert touch["levels_used"] == 1 and touch["slippage"] == pytest.approx(0)
    assert touch["avg_price"] == pytest.approx(float(market["yes_bid_dollars"]))
    deeper = kalshi_fill_price(book, best_size * 2, side="yes", action="sell").row(0, named=True)
    assert deeper["avg_price"] < touch["avg_price"] and deeper["slippage"] > 0


def test_size_past_the_book_fills_partially(captured):
    book, _ = captured
    depth = sum(float(s) for _, s in book["orderbook_fp"]["yes_dollars"])
    fill = kalshi_fill_price(book, depth * 2, side="no", action="buy").row(0, named=True)
    assert fill["filled"] is False
    assert fill["size_filled"] == pytest.approx(depth)
    assert fill["levels_used"] == len(book["orderbook_fp"]["yes_dollars"])
    yes_bids = sorted(((float(p), float(s)) for p, s in book["orderbook_fp"]["yes_dollars"]), reverse=True)
    no_asks = [(round(1 - p, 4), s) for p, s in yes_bids]
    assert fill["cost"] == pytest.approx(_walk(no_asks, depth))
    assert fill["avg_price"] == pytest.approx(fill["cost"] / depth)


def test_fixed_point_sizes_that_sum_with_float_residue_still_fill():
    """0.1 + 0.2 contracts against levels of 0.10 and 0.20 is a full fill, not a 1e-17 shortfall."""
    book = {"orderbook_fp": {"yes_dollars": [], "no_dollars": [["0.4000", "0.20"], ["0.5000", "0.10"]]}}
    fill = kalshi_fill_price(book, 0.1 + 0.2, side="yes", action="buy").row(0, named=True)
    assert fill["filled"] is True and fill["levels_used"] == 2
    assert fill["avg_price"] == pytest.approx((0.1 * 0.5 + 0.2 * 0.6) / 0.3)


def test_depth_limited_book_from_the_route_fixture():
    """The committed depth=3 capture works the same: YES ask = 1 - best NO bid."""
    book = json.loads((BOOKS.parent / "markets__ticker__orderbook.json").read_text(encoding="utf-8"))
    q = kalshi_book_quotes(book)
    assert _top(q, "yes", "ask")["price"] == round(1 - 0.19, 4)
    assert _top(q, "no", "ask")["price"] == round(1 - 0.80, 4)


def test_empty_book_and_bad_arguments():
    q = kalshi_book_quotes({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}})
    assert q.height == 0 and q.columns == ["side", "quote", "level", "price", "size", "cum_size"]
    fill = kalshi_fill_price({}, 10).row(0, named=True)
    assert fill["size_filled"] == 0 and fill["filled"] is False and fill["avg_price"] is None
    book = _load(TICKERS[0], "orderbook")
    with pytest.raises(ValueError, match="side"):
        kalshi_fill_price(book, 1, side="maybe")
    with pytest.raises(ValueError, match="action"):
        kalshi_fill_price(book, 1, action="hold")
    for bad in (0, -1, float("inf"), float("nan"), "5", True):
        with pytest.raises(ValueError, match="size"):
            kalshi_fill_price(book, bad)
    with pytest.raises(ValueError, match="cents"):
        kalshi_book_quotes(pl.DataFrame({"side": ["yes"], "price": ["51"], "size": ["10"]}))
    unreadable = {"orderbook_fp": {"yes_dollars": [[None, "5"], ["0.4000", "5"]], "no_dollars": []}}
    assert kalshi_book_quotes(unreadable).filter(quote="bid")["price"].to_list() == [0.4]
    with pytest.raises(TypeError):
        kalshi_book_quotes("not a book")
    with pytest.raises(ValueError, match="columns"):
        kalshi_book_quotes(pl.DataFrame({"side": ["yes"]}))
