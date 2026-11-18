- **Odds:** `polymarket_us_market_book`, `_settlement`, `_by_id` and `_by_slug` wrap the last four keyless Polymarket US
  routes. The book is one row per bid or ask level; an unsettled market's 404 on `/settlement` raises `NoDataError`.
  (#1234)
