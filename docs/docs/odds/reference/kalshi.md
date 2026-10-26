---
title: Betting odds — Kalshi Trade API v2 market data (api.elections.kalshi.com)
sidebar_label: Kalshi Trade API v2 market data (api.elections.kalshi.com)
description: "Betting odds — Kalshi Trade API v2 market data (api.elections.kalshi.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 11
toc_max_heading_level: 2
---
# Betting odds — Kalshi Trade API v2 market data (api.elections.kalshi.com)

`sportsdataverse.odds` — 9 endpoints.

## kalshi_event

One event, optionally with its markets nested.

**Endpoint URL:** `GET https://api.elections.kalshi.com/trade-api/v2/events/{event_ticker}`

**Valid URL:** [https://api.elections.kalshi.com/trade-api/v2/events/KXNFLGAME-26OCT08TBDAL?with_nested_markets=true](https://api.elections.kalshi.com/trade-api/v2/events/KXNFLGAME-26OCT08TBDAL?with_nested_markets=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_ticker` | `event_ticker` |  | `Y` |  | Event ticker, e.g. KXNFLGAME-26OCT08TBDAL (from /events). |
| `with_nested_markets` | `with_nested_markets` |  |  | `Y` | true nests the event's markets under event.markets. |

### Returns {#kalshi_event-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | String |  |
| `collateral_return_type` | String |  |
| `event_ticker` | String |  |
| `exchange_index` | Int64 |  |
| `last_updated_ts` | String |  |
| `markets` | String |  |
| `mutually_exclusive` | Boolean |  |
| `series_ticker` | String |  |
| `settlement_sources` | String |  |
| `strike_period` | String |  |
| `sub_title` | String |  |
| `title` | String |  |
| `product_metadata_competition` | String |  |
| `product_metadata_competition_scope` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kalshi_event-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Kalshi answered 404 (unknown series, event or market ticker). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kalshi_event-example}

```python
kalshi_event(event_ticker='KXNFLGAME-26OCT08TBDAL', with_nested_markets='true')
```

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
| `category` | String |  |
| `collateral_return_type` | String |  |
| `event_ticker` | String |  |
| `exchange_index` | Int64 |  |
| `last_updated_ts` | String |  |
| `mutually_exclusive` | Boolean |  |
| `series_ticker` | String |  |
| `settlement_sources` | String |  |
| `strike_period` | String |  |
| `sub_title` | String |  |
| `title` | String |  |
| `product_metadata_competition` | String |  |
| `product_metadata_competition_scope` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kalshi_events-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Kalshi answered 404 (unknown series, event or market ticker). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kalshi_events-example}

```python
kalshi_events(limit='3', series_ticker='KXNFLGAME', status='open')
```

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
| `exchange_active` | Boolean |  |
| `exchange_index_statuses` | String |  |
| `intra_exchange_transfers_active` | Boolean |  |
| `trading_active` | Boolean |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kalshi_exchange_status-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Kalshi answered 404 (unknown series, event or market ticker). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kalshi_exchange_status-example}

```python
kalshi_exchange_status()
```

## kalshi_market

One market.

**Endpoint URL:** `GET https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}`

**Valid URL:** [https://api.elections.kalshi.com/trade-api/v2/markets/KXNFLGAME-26OCT08TBDAL-DAL](https://api.elections.kalshi.com/trade-api/v2/markets/KXNFLGAME-26OCT08TBDAL-DAL)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ticker` | `ticker` |  | `Y` |  | Market ticker, e.g. KXNFLGAME-26OCT08TBDAL-DAL (from /markets). |

### Returns {#kalshi_market-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `can_close_early` | Boolean |  |
| `close_time` | String |  |
| `created_time` | String |  |
| `early_close_condition` | String |  |
| `event_ticker` | String |  |
| `exchange_index` | Int64 |  |
| `expected_expiration_time` | String |  |
| `expiration_time` | String |  |
| `expiration_value` | String |  |
| `last_price_dollars` | String |  |
| `latest_expiration_time` | String |  |
| `market_type` | String |  |
| `no_ask_dollars` | String |  |
| `no_bid_dollars` | String |  |
| `no_sub_title` | String |  |
| `notional_value_dollars` | String |  |
| `occurrence_datetime` | String |  |
| `open_interest_fp` | String |  |
| `open_time` | String |  |
| `previous_price_dollars` | String |  |
| `previous_yes_ask_dollars` | String |  |
| `previous_yes_bid_dollars` | String |  |
| `price_level_structure` | String |  |
| `price_ranges` | String |  |
| `result` | String |  |
| `rules_primary` | String |  |
| `rules_secondary` | String |  |
| `settlement_bounds_type` | String |  |
| `settlement_timer_seconds` | Int64 |  |
| `status` | String |  |
| `strike_type` | String |  |
| `ticker` | String |  |
| `title` | String |  |
| `updated_time` | String |  |
| `volume_24h_fp` | String |  |
| `volume_fp` | String |  |
| `yes_ask_dollars` | String |  |
| `yes_ask_size_fp` | String |  |
| `yes_bid_dollars` | String |  |
| `yes_bid_size_fp` | String |  |
| `yes_sub_title` | String |  |
| `custom_strike_football_team` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kalshi_market-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Kalshi answered 404 (unknown series, event or market ticker). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kalshi_market-example}

```python
kalshi_market(ticker='KXNFLGAME-26OCT08TBDAL-DAL')
```

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
| `can_close_early` | Boolean |  |
| `close_time` | String |  |
| `created_time` | String |  |
| `early_close_condition` | String |  |
| `event_ticker` | String |  |
| `exchange_index` | Int64 |  |
| `expected_expiration_time` | String |  |
| `expiration_time` | String |  |
| `expiration_value` | String |  |
| `last_price_dollars` | String |  |
| `latest_expiration_time` | String |  |
| `market_type` | String |  |
| `no_ask_dollars` | String |  |
| `no_bid_dollars` | String |  |
| `no_sub_title` | String |  |
| `notional_value_dollars` | String |  |
| `occurrence_datetime` | String |  |
| `open_interest_fp` | String |  |
| `open_time` | String |  |
| `previous_price_dollars` | String |  |
| `previous_yes_ask_dollars` | String |  |
| `previous_yes_bid_dollars` | String |  |
| `price_level_structure` | String |  |
| `price_ranges` | String |  |
| `result` | String |  |
| `rules_primary` | String |  |
| `rules_secondary` | String |  |
| `settlement_bounds_type` | String |  |
| `settlement_timer_seconds` | Int64 |  |
| `status` | String |  |
| `strike_type` | String |  |
| `ticker` | String |  |
| `title` | String |  |
| `updated_time` | String |  |
| `volume_24h_fp` | String |  |
| `volume_fp` | String |  |
| `yes_ask_dollars` | String |  |
| `yes_ask_size_fp` | String |  |
| `yes_bid_dollars` | String |  |
| `yes_bid_size_fp` | String |  |
| `yes_sub_title` | String |  |
| `custom_strike_football_team` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kalshi_markets-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Kalshi answered 404 (unknown series, event or market ticker). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kalshi_markets-example}

```python
kalshi_markets(event_ticker='KXNFLGAME-26OCT08TBDAL', limit='3')
```

## kalshi_orderbook

Order book of a market (fixed-point dollar levels).

**Endpoint URL:** `GET https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}/orderbook`

**Valid URL:** [https://api.elections.kalshi.com/trade-api/v2/markets/KXNFLGAME-26OCT08TBDAL-DAL/orderbook?depth=3](https://api.elections.kalshi.com/trade-api/v2/markets/KXNFLGAME-26OCT08TBDAL-DAL/orderbook?depth=3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ticker` | `ticker` |  | `Y` |  | Market ticker, e.g. KXNFLGAME-26OCT08TBDAL-DAL (from /markets). |
| `depth` | `depth` |  |  | `Y` | Price levels per side (omit for the full book). |

### Returns {#kalshi_orderbook-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `side` | String |  |
| `price` | String |  |
| `size` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kalshi_orderbook-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Kalshi answered 404 (unknown series, event or market ticker). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kalshi_orderbook-example}

```python
kalshi_orderbook(depth='3', ticker='KXNFLGAME-26OCT08TBDAL-DAL')
```

## kalshi_series

One series.

**Endpoint URL:** `GET https://api.elections.kalshi.com/trade-api/v2/series/{series_ticker}`

**Valid URL:** [https://api.elections.kalshi.com/trade-api/v2/series/KXNFLGAME](https://api.elections.kalshi.com/trade-api/v2/series/KXNFLGAME)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `series_ticker` | `series_ticker` |  | `Y` |  | Series ticker, e.g. KXNFLGAME (from /series). |

### Returns {#kalshi_series-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `additional_prohibitions` | String |  |
| `categories` | String |  |
| `category` | String |  |
| `contract_terms_url` | String |  |
| `contract_url` | String |  |
| `exchange_index` | Int64 |  |
| `fee_multiplier` | Int64 |  |
| `fee_type` | String |  |
| `frequency` | String |  |
| `last_updated_ts` | String |  |
| `settlement_sources` | String |  |
| `tags` | String |  |
| `ticker` | String |  |
| `title` | String |  |
| `product_metadata_scope` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kalshi_series-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Kalshi answered 404 (unknown series, event or market ticker). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kalshi_series-example}

```python
kalshi_series(series_ticker='KXNFLGAME')
```

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
| `additional_prohibitions` | String |  |
| `categories` | String |  |
| `category` | String |  |
| `contract_terms_url` | String |  |
| `contract_url` | String |  |
| `exchange_index` | Int64 |  |
| `fee_multiplier` | Int64 |  |
| `fee_type` | String |  |
| `frequency` | String |  |
| `last_updated_ts` | String |  |
| `settlement_sources` | String |  |
| `tags` | String |  |
| `ticker` | String |  |
| `title` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kalshi_series_list-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Kalshi answered 404 (unknown series, event or market ticker). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kalshi_series_list-example}

```python
kalshi_series_list(category='Sports')
```

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
| `count_fp` | String |  |
| `created_time` | String |  |
| `is_block_trade` | Boolean |  |
| `no_price_dollars` | String |  |
| `taker_book_side` | String |  |
| `taker_outcome_side` | String |  |
| `taker_side` | String |  |
| `ticker` | String |  |
| `trade_id` | String |  |
| `yes_price_dollars` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kalshi_trades-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Kalshi answered 404 (unknown series, event or market ticker). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kalshi_trades-example}

```python
kalshi_trades(limit='3', ticker='KXNFLGAME-26OCT08TBDAL-DAL')
```
