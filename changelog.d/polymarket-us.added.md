- **Odds:** `polymarket_us_markets()` and `polymarket_us_market_bbo(slug)` wrap the keyless Polymarket US read API
  (`gateway.polymarket.us/v1`); a game market's slug is `aec-` + a Gamma event's `usId`. `parse_polymarket` now
  unwraps single-key `{"markets": [...]}` / `{"marketData": {...}}` bodies.
