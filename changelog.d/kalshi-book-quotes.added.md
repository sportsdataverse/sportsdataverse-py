- **Prediction markets:** `kalshi_book_quotes` lays out both sides of a Kalshi order book, each ask derived from the
  opposite side's bid (YES ask = 1 - best NO bid). `kalshi_fill_price` walks that ladder for a number of contracts:
  average price, slippage, cost and whether the book fills it. (#1336)
