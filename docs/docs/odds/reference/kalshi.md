---
title: ODDS — Kalshi Trade API v2 market data (api.elections.kalshi.com)
sidebar_label: Kalshi Trade API v2 market data (api.elections.kalshi.com)
description: "ODDS — Kalshi Trade API v2 market data (api.elections.kalshi.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 11
toc_max_heading_level: 2
---
# ODDS — Kalshi Trade API v2 market data (api.elections.kalshi.com)

`sportsdataverse.odds` — 9 endpoints.

## kalshi_event

One event, optionally with its markets nested.

**Endpoint URL:** `GET https://api.elections.kalshi.com/trade-api/v2/events/{event_ticker}`

**Valid URL:** [https://api.elections.kalshi.com/trade-api/v2/events/KXNFLGAME-26OCT08TBDAL?with_nested_markets=true](https://api.elections.kalshi.com/trade-api/v2/events/KXNFLGAME-26OCT08TBDAL?with_nested_markets=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_ticker` | `event_ticker` |  | `Y` |  | event_ticker path parameter. |
| `with_nested_markets` | `with_nested_markets` |  |  | `Y` | true nests the event's markets under event.markets. |

### Returns {#kalshi_event-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | character | Category label. |
| `collateral_return_type` | character |  |
| `event_ticker` | character |  |
| `exchange_index` | integer |  |
| `last_updated_ts` | character |  |
| `markets` | character |  |
| `mutually_exclusive` | logical |  |
| `series_ticker` | character |  |
| `settlement_sources` | character |  |
| `strike_period` | character |  |
| `sub_title` | character |  |
| `title` | character | Specific role title for the assignment. |
| `product_metadata_competition` | character |  |
| `product_metadata_competition_scope` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kalshi_event-example}

```python
kalshi_event(event_ticker='KXNFLGAME-26OCT08TBDAL', with_nested_markets='true')
```

_Last validated n/a._

## kalshi_events

Events, filterable by series and status; cursor-paged.

**Endpoint URL:** `GET https://api.elections.kalshi.com/trade-api/v2/events`

**Valid URL:** [https://api.elections.kalshi.com/trade-api/v2/events?limit=3&status=open&series_ticker=KXNFLGAME](https://api.elections.kalshi.com/trade-api/v2/events?limit=3&status=open&series_ticker=KXNFLGAME)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Rows per page (max 1000 on /markets, 200 on /events; default 100). |
| `status` | `status` |  |  | `Y` | Lifecycle filter: open, closed, settled (events also accept unopened). |
| `series_ticker` | `series_ticker` |  |  | `Y` | Series ticker, e.g. KXNFLGAME (from /series). |
| `cursor` | `cursor` |  |  | `Y` | Opaque page cursor returned in the previous page's `cursor` field (verified 2026-10-06). |

### Returns {#kalshi_events-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | character | Category label. |
| `collateral_return_type` | character |  |
| `event_ticker` | character |  |
| `exchange_index` | integer |  |
| `last_updated_ts` | character |  |
| `mutually_exclusive` | logical |  |
| `series_ticker` | character |  |
| `settlement_sources` | character |  |
| `strike_period` | character |  |
| `sub_title` | character |  |
| `title` | character | Specific role title for the assignment. |
| `product_metadata_competition` | character |  |
| `product_metadata_competition_scope` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kalshi_events-example}

```python
kalshi_events(limit='3', series_ticker='KXNFLGAME', status='open')
```

_Last validated n/a._

## kalshi_exchange_status

Exchange and trading status.

**Endpoint URL:** `GET https://api.elections.kalshi.com/trade-api/v2/exchange/status`

**Valid URL:** [https://api.elections.kalshi.com/trade-api/v2/exchange/status](https://api.elections.kalshi.com/trade-api/v2/exchange/status)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#kalshi_exchange_status-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `exchange_active` | logical |  |
| `exchange_index_statuses` | character |  |
| `intra_exchange_transfers_active` | logical |  |
| `trading_active` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kalshi_exchange_status-example}

```python
kalshi_exchange_status()
```

_Last validated n/a._

## kalshi_market

One market.

**Endpoint URL:** `GET https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}`

**Valid URL:** [https://api.elections.kalshi.com/trade-api/v2/markets/KXNFLGAME-26OCT08TBDAL-DAL](https://api.elections.kalshi.com/trade-api/v2/markets/KXNFLGAME-26OCT08TBDAL-DAL)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ticker` | `ticker` |  | `Y` |  | ticker path parameter. |

### Returns {#kalshi_market-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `can_close_early` | logical |  |
| `close_time` | character |  |
| `created_time` | character |  |
| `early_close_condition` | character |  |
| `event_ticker` | character |  |
| `exchange_index` | integer |  |
| `expected_expiration_time` | character |  |
| `expiration_time` | character |  |
| `expiration_value` | character |  |
| `last_price_dollars` | character |  |
| `latest_expiration_time` | character |  |
| `market_type` | character | Market type code (`winLeague`, `winConference`, `winDivision`, ...). |
| `no_ask_dollars` | character |  |
| `no_bid_dollars` | character |  |
| `no_sub_title` | character |  |
| `notional_value_dollars` | character |  |
| `occurrence_datetime` | character |  |
| `open_interest_fp` | character |  |
| `open_time` | character |  |
| `previous_price_dollars` | character |  |
| `previous_yes_ask_dollars` | character |  |
| `previous_yes_bid_dollars` | character |  |
| `price_level_structure` | character |  |
| `price_ranges` | character |  |
| `result` | character | Result. |
| `rules_primary` | character |  |
| `rules_secondary` | character |  |
| `settlement_bounds_type` | character |  |
| `settlement_timer_seconds` | integer |  |
| `status` | character | Status label. |
| `strike_type` | character |  |
| `ticker` | character |  |
| `title` | character | Specific role title for the assignment. |
| `updated_time` | character |  |
| `volume_24h_fp` | character |  |
| `volume_fp` | character |  |
| `yes_ask_dollars` | character |  |
| `yes_ask_size_fp` | character |  |
| `yes_bid_dollars` | character |  |
| `yes_bid_size_fp` | character |  |
| `yes_sub_title` | character |  |
| `custom_strike_football_team` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kalshi_market-example}

```python
kalshi_market(ticker='KXNFLGAME-26OCT08TBDAL-DAL')
```

_Last validated n/a._

## kalshi_markets

Markets, filterable by event/series/status; cursor-paged.

**Endpoint URL:** `GET https://api.elections.kalshi.com/trade-api/v2/markets`

**Valid URL:** [https://api.elections.kalshi.com/trade-api/v2/markets?limit=3&event_ticker=KXNFLGAME-26OCT08TBDAL](https://api.elections.kalshi.com/trade-api/v2/markets?limit=3&event_ticker=KXNFLGAME-26OCT08TBDAL)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Rows per page (max 1000 on /markets, 200 on /events; default 100). |
| `event_ticker` | `event_ticker` |  |  | `Y` | Event ticker, e.g. KXNFLGAME-26OCT08TBDAL (from /events). |
| `cursor` | `cursor` |  |  | `Y` | Opaque page cursor returned in the previous page's `cursor` field (verified 2026-10-06). |

### Returns {#kalshi_markets-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `can_close_early` | logical |  |
| `close_time` | character |  |
| `created_time` | character |  |
| `early_close_condition` | character |  |
| `event_ticker` | character |  |
| `exchange_index` | integer |  |
| `expected_expiration_time` | character |  |
| `expiration_time` | character |  |
| `expiration_value` | character |  |
| `last_price_dollars` | character |  |
| `latest_expiration_time` | character |  |
| `market_type` | character | Market type code (`winLeague`, `winConference`, `winDivision`, ...). |
| `no_ask_dollars` | character |  |
| `no_bid_dollars` | character |  |
| `no_sub_title` | character |  |
| `notional_value_dollars` | character |  |
| `occurrence_datetime` | character |  |
| `open_interest_fp` | character |  |
| `open_time` | character |  |
| `previous_price_dollars` | character |  |
| `previous_yes_ask_dollars` | character |  |
| `previous_yes_bid_dollars` | character |  |
| `price_level_structure` | character |  |
| `price_ranges` | character |  |
| `result` | character | Result. |
| `rules_primary` | character |  |
| `rules_secondary` | character |  |
| `settlement_bounds_type` | character |  |
| `settlement_timer_seconds` | integer |  |
| `status` | character | Status label. |
| `strike_type` | character |  |
| `ticker` | character |  |
| `title` | character | Specific role title for the assignment. |
| `updated_time` | character |  |
| `volume_24h_fp` | character |  |
| `volume_fp` | character |  |
| `yes_ask_dollars` | character |  |
| `yes_ask_size_fp` | character |  |
| `yes_bid_dollars` | character |  |
| `yes_bid_size_fp` | character |  |
| `yes_sub_title` | character |  |
| `custom_strike_football_team` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kalshi_markets-example}

```python
kalshi_markets(event_ticker='KXNFLGAME-26OCT08TBDAL', limit='3')
```

_Last validated n/a._

## kalshi_orderbook

Order book of a market (fixed-point dollar levels).

**Endpoint URL:** `GET https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}/orderbook`

**Valid URL:** [https://api.elections.kalshi.com/trade-api/v2/markets/KXNFLGAME-26OCT08TBDAL-DAL/orderbook?depth=3](https://api.elections.kalshi.com/trade-api/v2/markets/KXNFLGAME-26OCT08TBDAL-DAL/orderbook?depth=3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ticker` | `ticker` |  | `Y` |  | ticker path parameter. |
| `depth` | `depth` |  |  | `Y` | Price levels per side (omit for the full book). |

### Returns {#kalshi_orderbook-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `side` | character | Side label (e.g. 'home', 'away', or 'overUnder'). |
| `price` | character |  |
| `size` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kalshi_orderbook-example}

```python
kalshi_orderbook(depth='3', ticker='KXNFLGAME-26OCT08TBDAL-DAL')
```

_Last validated n/a._

## kalshi_series

One series.

**Endpoint URL:** `GET https://api.elections.kalshi.com/trade-api/v2/series/{series_ticker}`

**Valid URL:** [https://api.elections.kalshi.com/trade-api/v2/series/KXNFLGAME](https://api.elections.kalshi.com/trade-api/v2/series/KXNFLGAME)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `series_ticker` | `series_ticker` |  | `Y` |  | series_ticker path parameter. |

### Returns {#kalshi_series-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `additional_prohibitions` | character |  |
| `categories` | character |  |
| `category` | character | Category label. |
| `contract_terms_url` | character |  |
| `contract_url` | character |  |
| `exchange_index` | integer |  |
| `fee_multiplier` | integer |  |
| `fee_type` | character |  |
| `frequency` | character |  |
| `last_updated_ts` | character |  |
| `settlement_sources` | character |  |
| `tags` | character |  |
| `ticker` | character |  |
| `title` | character | Specific role title for the assignment. |
| `product_metadata_scope` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kalshi_series-example}

```python
kalshi_series(series_ticker='KXNFLGAME')
```

_Last validated n/a._

## kalshi_series_list

Series (market families) in a category; the whole category in one body.

**Endpoint URL:** `GET https://api.elections.kalshi.com/trade-api/v2/series`

**Valid URL:** [https://api.elections.kalshi.com/trade-api/v2/series?category=Sports](https://api.elections.kalshi.com/trade-api/v2/series?category=Sports)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  |  | `Y` | Series category, e.g. Sports. |

### Returns {#kalshi_series_list-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `additional_prohibitions` | character |  |
| `categories` | character |  |
| `category` | character | Category label. |
| `contract_terms_url` | character |  |
| `contract_url` | character |  |
| `exchange_index` | integer |  |
| `fee_multiplier` | integer |  |
| `fee_type` | character |  |
| `frequency` | character |  |
| `last_updated_ts` | character |  |
| `settlement_sources` | character |  |
| `tags` | character |  |
| `ticker` | character |  |
| `title` | character | Specific role title for the assignment. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kalshi_series_list-example}

```python
kalshi_series_list(category='Sports')
```

_Last validated n/a._

## kalshi_trades

Public trades, filterable by market; cursor-paged.

**Endpoint URL:** `GET https://api.elections.kalshi.com/trade-api/v2/markets/trades`

**Valid URL:** [https://api.elections.kalshi.com/trade-api/v2/markets/trades?limit=3&ticker=KXNFLGAME-26OCT08TBDAL-DAL](https://api.elections.kalshi.com/trade-api/v2/markets/trades?limit=3&ticker=KXNFLGAME-26OCT08TBDAL-DAL)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Rows per page (max 1000 on /markets, 200 on /events; default 100). |
| `ticker` | `ticker` |  |  | `Y` | Market ticker, e.g. KXNFLGAME-26OCT08TBDAL-DAL (from /markets). |
| `cursor` | `cursor` |  |  | `Y` | Opaque page cursor returned in the previous page's `cursor` field (verified 2026-10-06). |

### Returns {#kalshi_trades-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `count_fp` | character |  |
| `created_time` | character |  |
| `is_block_trade` | logical |  |
| `no_price_dollars` | character |  |
| `taker_book_side` | character |  |
| `taker_outcome_side` | character |  |
| `taker_side` | character |  |
| `ticker` | character |  |
| `trade_id` | character | ID of Trade |
| `yes_price_dollars` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kalshi_trades-example}

```python
kalshi_trades(limit='3', ticker='KXNFLGAME-26OCT08TBDAL-DAL')
```

_Last validated n/a._
