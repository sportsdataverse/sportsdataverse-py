<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Kalshi order-book fixtures](#kalshi-order-book-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Kalshi order-book fixtures

Real, untrimmed responses from the Kalshi Trade API v2 (`https://api.elections.kalshi.com/trade-api/v2`),
captured 2026-11-26 16:56 UTC with plain unauthenticated GETs, for `tests/odds/test_kalshi_book.py`.

| File | Route |
|---|---|
| `<ticker>.orderbook.json` | `/markets/{ticker}/orderbook` (no `depth`: the full book) |
| `<ticker>.market.json` | `/markets/{ticker}`, fetched right after the book |

| Ticker | YES bid levels | NO bid levels |
|---|---|---|
| `KXNFLGAME-26NOV26CHIDET-DET` | 48 | 51 |
| `KXNFLGAME-26NOV27DENPIT-DEN` | 59 | 33 |

In both pairs the market's `yes_bid_dollars` equals the book's best YES bid, so the market's own
`yes_ask_dollars` / `no_ask_dollars` check the asks the helper derives from the opposite side's bids.
The touch sizes on `/markets/{ticker}` can lag the live book: the CHIDET pair (a busy pre-game book) differs
from its book by 33.49 contracts at the YES ask, so the tests compare sizes only on the DENPIT pair, whose
`yes_bid_size_fp` and `yes_ask_size_fp` equal the book's. Do not hand-edit; to refresh, capture both routes
again for an open market and check the pair the same way.
