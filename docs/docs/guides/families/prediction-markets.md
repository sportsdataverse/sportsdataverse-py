---
title: Prediction markets
sidebar_label: Prediction markets
sidebar_position: 4
description: "Polymarket and Kalshi market data for sports events through sportsdataverse.odds: markets, prices and history as polars frames."
---

# Prediction markets

`sportsdataverse.odds` wraps the public market-data APIs of two prediction markets,
[Polymarket](https://docs.polymarket.com/) and [Kalshi](https://docs.kalshi.com/),
beside The Odds API's sportsbook lines. Both are read-only and keyless: the wrappers
read events, markets, prices and order books. Trading and account endpoints need
signed keys and are not wrapped.

## Polymarket

Polymarket serves metadata and prices from two hosts, and the function names say
which:

| Function | Host | Returns |
|---|---|---|
| `polymarket_gamma_events(limit=, closed=, tag_slug=, offset=)` | gamma (metadata) | events: groups of related markets |
| `polymarket_gamma_markets(limit=, closed=, order=, ascending=, offset=)` | gamma | markets, orderable by volume or liquidity |
| `polymarket_gamma_market(...)` | gamma | one market |
| `polymarket_gamma_tags(...)` | gamma | the tags (such as `sports`) that group events |
| `polymarket_clob_markets(...)` | clob (order book) | markets as the order book lists them |
| `polymarket_clob_book(...)` | clob | one outcome token's order book |
| `polymarket_clob_price(token_id, side)` | clob | the best price on one side of one token's book |
| `polymarket_clob_midpoint(...)` | clob | one token's midpoint price |

The query parameters are passed as strings, as the API takes them.

```python
from sportsdataverse.odds import polymarket_gamma_events, polymarket_gamma_markets

sports_events = polymarket_gamma_events(closed="false", limit="5", tag_slug="sports")
busiest = polymarket_gamma_markets(closed="false", limit="5", order="volume24hr", ascending="false")
print(sports_events.height, busiest.height)
```

## Kalshi

Kalshi organizes markets as series (a market family, such as every NFL game), events
(one game) and markets (one contract on that event).

| Function | Returns |
|---|---|
| `kalshi_series_list(category=)` | every series in a category, such as `Sports` |
| `kalshi_series(...)` | one series |
| `kalshi_events(limit=, status=, series_ticker=, cursor=)` | events, filterable by series and status; cursor-paged |
| `kalshi_event(...)` | one event |
| `kalshi_markets(limit=, event_ticker=, cursor=)` | markets, filterable by event; cursor-paged |
| `kalshi_market(...)` | one market |
| `kalshi_orderbook(ticker, depth=)` | a market's order book, at fixed-point dollar levels |
| `kalshi_trades(...)` | executed trades |
| `kalshi_exchange_status()` | whether the exchange is open |

```python
from sportsdataverse.odds import kalshi_events, kalshi_series_list

sports_series = kalshi_series_list(category="Sports")
nfl_games = kalshi_events(limit="5", series_ticker="KXNFLGAME", status="open")
print(sports_series.height, nfl_games.height)
```

## Conventions

- Wrappers return a polars DataFrame by default; `return_parsed=False` gives the raw
  JSON and `return_as_pandas=True` gives pandas. `parse_polymarket` and
  `parse_kalshi` parse a raw payload you already have.
- Errors follow the package vocabulary: a 404 raises `NoDataError`, a failed fetch
  `AssetFetchError`, a 400 / 422 `ValueError`.
- A market price is what a contract trades at, and the prices of the two sides need
  not sum to 1 (the bid-ask spread), so they are not de-vigged probabilities. To
  compare them with sportsbook lines or a model, the market helpers in
  [`sportsdataverse.wexp`](../modeling.md#win-expectancy-wexp)
  (`prob_from_american`, `devig_multiplicative`) put both on one scale.

The [Odds page](../../odds/index.md) lists every function with its parameters and
Returns table.
