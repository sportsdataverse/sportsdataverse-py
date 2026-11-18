- `polymarket_us_market_book(slug)`, `polymarket_us_market_settlement(slug)`, `polymarket_us_market_by_id(id)` and
  `polymarket_us_market_by_slug(slug)` wrap the other four keyless Polymarket US gateway routes. The book is one row per
  bid or ask level (the exchange's `offers`, tagged `ask` like `polymarket_clob_book`); a market that has not settled
  answers 404 on `/settlement`, raised as `NoDataError`.
