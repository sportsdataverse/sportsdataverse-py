---
title: Betting odds — Polymarket read APIs (gamma-api, clob + the US gateway)
sidebar_label: Polymarket read APIs (gamma-api, clob + the US gateway)
description: "Betting odds — Polymarket read APIs (gamma-api, clob + the US gateway) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# Betting odds — Polymarket read APIs (gamma-api, clob + the US gateway)

`sportsdataverse.odds` — 14 endpoints.

## polymarket_gamma_events

Events -- groups of related markets -- on the Gamma metadata host.

**Endpoint URL:** `GET https://gamma-api.polymarket.com/events`

**Valid URL:** [https://gamma-api.polymarket.com/events?limit=3&closed=false&tag_slug=sports](https://gamma-api.polymarket.com/events?limit=3&closed=false&tag_slug=sports)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Page size. |
| `closed` | `closed` |  |  | `Y` | Filter closed markets/events (true/false). |
| `tag_slug` | `tag_slug` |  |  | `Y` | Filter events by tag slug (e.g. sports; see /tags). |
| `offset` | `offset` |  |  | `Y` | Row offset; offset=1 shifts the first row by one (verified 2026-10-06). |

### Returns {#polymarket_gamma_events-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String |  |
| `ticker` | String |  |
| `slug` | String |  |
| `title` | String |  |
| `description` | String |  |
| `resolution_source` | String |  |
| `start_date` | String |  |
| `creation_date` | String |  |
| `end_date` | String |  |
| `image` | String |  |
| `icon` | String |  |
| `active` | Boolean |  |
| `closed` | Boolean |  |
| `archived` | Boolean |  |
| `new` | Boolean |  |
| `featured` | Boolean |  |
| `restricted` | Boolean |  |
| `liquidity` | Float64 |  |
| `volume` | Float64 |  |
| `open_interest` | Float64 |  |
| `sort_by` | String |  |
| `created_at` | String |  |
| `updated_at` | String |  |
| `competitive` | Float64 |  |
| `volume24hr` | Float64 |  |
| `volume1wk` | Float64 |  |
| `volume1mo` | Float64 |  |
| `volume1yr` | Float64 |  |
| `enable_order_book` | Boolean |  |
| `liquidity_clob` | Float64 |  |
| `neg_risk` | Boolean |  |
| `neg_risk_market_id` | String |  |
| `comment_count` | Int64 |  |
| `markets` | String |  |
| `tags` | String |  |
| `cyom` | Boolean |  |
| `show_all_outcomes` | Boolean |  |
| `show_market_images` | Boolean |  |
| `enable_neg_risk` | Boolean |  |
| `automatically_active` | Boolean |  |
| `gmp_chart_mode` | String |  |
| `neg_risk_augmented` | Boolean |  |
| `featured_order` | Float64 |  |
| `estimate_value` | Boolean |  |
| `cumulative_markets` | Boolean |  |
| `pending_deployment` | Boolean |  |
| `deploying` | Boolean |  |
| `deploying_timestamp` | String |  |
| `version` | String |  |
| `event_metadata_context_requires_regen` | Boolean |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_gamma_events-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_gamma_events-example}

```python
polymarket_gamma_events(closed='false', limit='3', tag_slug='sports')
```

## polymarket_gamma_market

One market by its Gamma id.

**Endpoint URL:** `GET https://gamma-api.polymarket.com/markets/{id}`

**Valid URL:** [https://gamma-api.polymarket.com/markets/608565](https://gamma-api.polymarket.com/markets/608565)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  | `Y` |  | Gamma market id (from /markets). |

### Returns {#polymarket_gamma_market-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String |  |
| `question` | String |  |
| `condition_id` | String |  |
| `slug` | String |  |
| `resolution_source` | String |  |
| `end_date` | String |  |
| `liquidity` | String |  |
| `start_date` | String |  |
| `image` | String |  |
| `icon` | String |  |
| `description` | String |  |
| `outcomes` | String |  |
| `outcome_prices` | String |  |
| `volume` | String |  |
| `active` | Boolean |  |
| `closed` | Boolean |  |
| `market_maker_address` | String |  |
| `created_at` | String |  |
| `updated_at` | String |  |
| `new` | Boolean |  |
| `featured` | Boolean |  |
| `submitted_by` | String |  |
| `archived` | Boolean |  |
| `resolved_by` | String |  |
| `restricted` | Boolean |  |
| `group_item_title` | String |  |
| `group_item_threshold` | String |  |
| `question_id` | String |  |
| `enable_order_book` | Boolean |  |
| `order_price_min_tick_size` | Float64 |  |
| `order_min_size` | Int64 |  |
| `volume_num` | Float64 |  |
| `liquidity_num` | Float64 |  |
| `end_date_iso` | String |  |
| `start_date_iso` | String |  |
| `has_reviewed_dates` | Boolean |  |
| `volume24hr` | Float64 |  |
| `volume1wk` | Float64 |  |
| `volume1mo` | Float64 |  |
| `volume1yr` | Float64 |  |
| `clob_token_ids` | String |  |
| `combo_status` | String |  |
| `uma_bond` | String |  |
| `uma_reward` | String |  |
| `volume24hr_clob` | Float64 |  |
| `volume1wk_clob` | Float64 |  |
| `volume1mo_clob` | Float64 |  |
| `volume1yr_clob` | Float64 |  |
| `volume_clob` | Float64 |  |
| `liquidity_clob` | Float64 |  |
| `maker_base_fee` | Int64 |  |
| `taker_base_fee` | Int64 |  |
| `custom_liveness` | Int64 |  |
| `accepting_orders` | Boolean |  |
| `neg_risk` | Boolean |  |
| `neg_risk_market_id` | String |  |
| `neg_risk_request_id` | String |  |
| `ready` | Boolean |  |
| `funded` | Boolean |  |
| `accepting_orders_timestamp` | String |  |
| `cyom` | Boolean |  |
| `competitive` | Float64 |  |
| `approved` | Boolean |  |
| `rewards_min_size` | Int64 |  |
| `rewards_max_spread` | Float64 |  |
| `spread` | Float64 |  |
| `one_day_price_change` | Float64 |  |
| `one_hour_price_change` | Float64 |  |
| `one_week_price_change` | Float64 |  |
| `one_month_price_change` | Float64 |  |
| `last_trade_price` | Float64 |  |
| `best_bid` | Float64 |  |
| `best_ask` | Float64 |  |
| `automatically_active` | Boolean |  |
| `clear_book_on_start` | Boolean |  |
| `series_color` | String |  |
| `show_gmp_series` | Boolean |  |
| `show_gmp_outcome` | Boolean |  |
| `manual_activation` | Boolean |  |
| `neg_risk_other` | Boolean |  |
| `uma_resolution_statuses` | String |  |
| `pending_deployment` | Boolean |  |
| `deploying` | Boolean |  |
| `deploying_timestamp` | String |  |
| `rfq_enabled` | Boolean |  |
| `holding_rewards_enabled` | Boolean |  |
| `fees_enabled` | Boolean |  |
| `fee_type` | String |  |
| `version` | String |  |
| `fee_schedule_exponent` | Int64 |  |
| `fee_schedule_rate` | Float64 |  |
| `fee_schedule_taker_only` | Boolean |  |
| `fee_schedule_rebate_rate` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_gamma_market-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_gamma_market-example}

```python
polymarket_gamma_market(id='608565')
```

## polymarket_gamma_markets

Markets on the Gamma metadata host, orderable by volume or liquidity.

**Endpoint URL:** `GET https://gamma-api.polymarket.com/markets`

**Valid URL:** [https://gamma-api.polymarket.com/markets?limit=3&closed=false&order=volume24hr&ascending=false](https://gamma-api.polymarket.com/markets?limit=3&closed=false&order=volume24hr&ascending=false)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Page size. |
| `closed` | `closed` |  |  | `Y` | Filter closed markets/events (true/false). |
| `order` | `order` |  |  | `Y` | Sort field, e.g. volume24hr, liquidity, startDate. |
| `ascending` | `ascending` |  |  | `Y` | Sort direction (true/false). |
| `offset` | `offset` |  |  | `Y` | Row offset; offset=1 shifts the first row by one (verified 2026-10-06). |

### Returns {#polymarket_gamma_markets-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String |  |
| `question` | String |  |
| `condition_id` | String |  |
| `slug` | String |  |
| `resolution_source` | String |  |
| `end_date` | String |  |
| `liquidity` | String |  |
| `start_date` | String |  |
| `image` | String |  |
| `icon` | String |  |
| `description` | String |  |
| `outcomes` | String |  |
| `outcome_prices` | String |  |
| `volume` | String |  |
| `active` | Boolean |  |
| `closed` | Boolean |  |
| `market_maker_address` | String |  |
| `created_at` | String |  |
| `updated_at` | String |  |
| `new` | Boolean |  |
| `featured` | Boolean |  |
| `submitted_by` | String |  |
| `archived` | Boolean |  |
| `resolved_by` | String |  |
| `restricted` | Boolean |  |
| `question_id` | String |  |
| `enable_order_book` | Boolean |  |
| `order_price_min_tick_size` | Float64 |  |
| `order_min_size` | Int64 |  |
| `uma_resolution_status` | String |  |
| `volume_num` | Float64 |  |
| `liquidity_num` | Float64 |  |
| `end_date_iso` | String |  |
| `start_date_iso` | String |  |
| `has_reviewed_dates` | Boolean |  |
| `volume24hr` | Float64 |  |
| `volume1wk` | Float64 |  |
| `volume1mo` | Float64 |  |
| `volume1yr` | Float64 |  |
| `clob_token_ids` | String |  |
| `combo_status` | String |  |
| `uma_bond` | String |  |
| `uma_reward` | String |  |
| `volume24hr_clob` | Float64 |  |
| `volume1wk_clob` | Float64 |  |
| `volume1mo_clob` | Float64 |  |
| `volume1yr_clob` | Float64 |  |
| `volume_clob` | Float64 |  |
| `liquidity_clob` | Float64 |  |
| `maker_base_fee` | Int64 |  |
| `taker_base_fee` | Int64 |  |
| `custom_liveness` | Int64 |  |
| `accepting_orders` | Boolean |  |
| `neg_risk` | Boolean |  |
| `events` | String |  |
| `ready` | Boolean |  |
| `funded` | Boolean |  |
| `accepting_orders_timestamp` | String |  |
| `cyom` | Boolean |  |
| `competitive` | Float64 |  |
| `approved` | Boolean |  |
| `rewards_min_size` | Int64 |  |
| `rewards_max_spread` | Float64 |  |
| `spread` | Float64 |  |
| `one_day_price_change` | Float64 |  |
| `last_trade_price` | Float64 |  |
| `best_bid` | Float64 |  |
| `best_ask` | Float64 |  |
| `automatically_active` | Boolean |  |
| `clear_book_on_start` | Boolean |  |
| `manual_activation` | Boolean |  |
| `neg_risk_other` | Boolean |  |
| `uma_resolution_statuses` | String |  |
| `pending_deployment` | Boolean |  |
| `deploying` | Boolean |  |
| `deploying_timestamp` | String |  |
| `rfq_enabled` | Boolean |  |
| `event_start_time` | String |  |
| `holding_rewards_enabled` | Boolean |  |
| `fees_enabled` | Boolean |  |
| `fee_type` | String |  |
| `version` | String |  |
| `fee_schedule_exponent` | Int64 |  |
| `fee_schedule_rate` | Float64 |  |
| `fee_schedule_taker_only` | Boolean |  |
| `fee_schedule_rebate_rate` | Float64 |  |
| `group_item_title` | String |  |
| `group_item_threshold` | String |  |
| `neg_risk_market_id` | String |  |
| `neg_risk_request_id` | String |  |
| `clob_rewards` | String |  |
| `one_week_price_change` | Float64 |  |
| `one_month_price_change` | Float64 |  |
| `one_year_price_change` | Float64 |  |
| `series_color` | String |  |
| `show_gmp_series` | Boolean |  |
| `show_gmp_outcome` | Boolean |  |
| `one_hour_price_change` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_gamma_markets-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_gamma_markets-example}

```python
polymarket_gamma_markets(ascending='false', closed='false', limit='3', order='volume24hr')
```

## polymarket_gamma_tags

Tags used to categorise Gamma events and markets.

**Endpoint URL:** `GET https://gamma-api.polymarket.com/tags`

**Valid URL:** [https://gamma-api.polymarket.com/tags?limit=3](https://gamma-api.polymarket.com/tags?limit=3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Page size. |
| `offset` | `offset` |  |  | `Y` | Row offset; offset=1 shifts the first row by one (verified 2026-10-06). |

### Returns {#polymarket_gamma_tags-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String |  |
| `label` | String |  |
| `slug` | String |  |
| `created_at` | String |  |
| `updated_at` | String |  |
| `published_at` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_gamma_tags-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_gamma_tags-example}

```python
polymarket_gamma_tags(limit='3')
```

## polymarket_clob_book

Full order book, bids and asks, for one outcome token.

**Endpoint URL:** `GET https://clob.polymarket.com/book`

**Valid URL:** [https://clob.polymarket.com/book?token_id=14940935985254400645996902276818598247231942791302218826999289776538227075176](https://clob.polymarket.com/book?token_id=14940935985254400645996902276818598247231942791302218826999289776538227075176)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `token_id` | `token_id` |  | `Y` |  | Required. CLOB token id (uint256 as a decimal string) = one entry of a Gamma market's clobTokenIds. |

### Returns {#polymarket_clob_book-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `market` | String |  |
| `asset_id` | String |  |
| `timestamp` | String |  |
| `hash` | String |  |
| `min_order_size` | String |  |
| `tick_size` | String |  |
| `neg_risk` | Boolean |  |
| `last_trade_price` | String |  |
| `side` | String |  |
| `price` | String |  |
| `size` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_clob_book-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_clob_book-example}

```python
polymarket_clob_book(token_id='14940935985254400645996902276818598247231942791302218826999289776538227075176')
```

## polymarket_clob_markets

Markets on the CLOB host, cursor-paged.

**Endpoint URL:** `GET https://clob.polymarket.com/markets`

**Valid URL:** [https://clob.polymarket.com/markets?next_cursor=MA%3D%3D](https://clob.polymarket.com/markets?next_cursor=MA%3D%3D)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `next_cursor` | `next_cursor` |  |  | `Y` | Page cursor from the previous response; MA== (base64 0) or empty is the first page, LTE= means no more pages. |

### Returns {#polymarket_clob_markets-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `enable_order_book` | Boolean |  |
| `active` | Boolean |  |
| `closed` | Boolean |  |
| `archived` | Boolean |  |
| `accepting_orders` | Boolean |  |
| `accepting_order_timestamp` | String |  |
| `minimum_order_size` | Int64 |  |
| `minimum_tick_size` | Float64 |  |
| `condition_id` | String |  |
| `question_id` | String |  |
| `question` | String |  |
| `description` | String |  |
| `market_slug` | String |  |
| `end_date_iso` | String |  |
| `game_start_time` | String |  |
| `seconds_delay` | Int64 |  |
| `fpmm` | String |  |
| `maker_base_fee` | Int64 |  |
| `taker_base_fee` | Int64 |  |
| `notifications_enabled` | Boolean |  |
| `neg_risk` | Boolean |  |
| `neg_risk_market_id` | String |  |
| `neg_risk_request_id` | String |  |
| `icon` | String |  |
| `image` | String |  |
| `is_50_50_outcome` | Boolean |  |
| `tokens` | String |  |
| `tags` | String |  |
| `version` | String |  |
| `rewards_rates` | String |  |
| `rewards_min_size` | Int64 |  |
| `rewards_max_spread` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_clob_markets-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_clob_markets-example}

```python
polymarket_clob_markets(next_cursor='MA==')
```

## polymarket_clob_midpoint

Midpoint between the best bid and the best ask for one outcome token.

**Endpoint URL:** `GET https://clob.polymarket.com/midpoint`

**Valid URL:** [https://clob.polymarket.com/midpoint?token_id=14940935985254400645996902276818598247231942791302218826999289776538227075176](https://clob.polymarket.com/midpoint?token_id=14940935985254400645996902276818598247231942791302218826999289776538227075176)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `token_id` | `token_id` |  | `Y` |  | Required. CLOB token id (uint256 as a decimal string) = one entry of a Gamma market's clobTokenIds. |

### Returns {#polymarket_clob_midpoint-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `mid` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_clob_midpoint-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_clob_midpoint-example}

```python
polymarket_clob_midpoint(token_id='14940935985254400645996902276818598247231942791302218826999289776538227075176')
```

## polymarket_clob_price

Best price on one side of the book for one outcome token.

**Endpoint URL:** `GET https://clob.polymarket.com/price`

**Valid URL:** [https://clob.polymarket.com/price?token_id=14940935985254400645996902276818598247231942791302218826999289776538227075176&side=buy](https://clob.polymarket.com/price?token_id=14940935985254400645996902276818598247231942791302218826999289776538227075176&side=buy)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `token_id` | `token_id` |  | `Y` |  | Required. CLOB token id (uint256 as a decimal string) = one entry of a Gamma market's clobTokenIds. |
| `side` | `side` |  | `Y` |  | Required. buy or sell. |

### Returns {#polymarket_clob_price-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `price` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_clob_price-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_clob_price-example}

```python
polymarket_clob_price(side='buy', token_id='14940935985254400645996902276818598247231942791302218826999289776538227075176')
```

## polymarket_us_market_bbo

Best bid and offer, last trade and volume for one Polymarket US market slug.

**Endpoint URL:** `GET https://gateway.polymarket.us/v1/markets/{slug}/bbo`

**Valid URL:** [https://gateway.polymarket.us/v1/markets/tec-mls-winner-2026-11-07-sea/bbo](https://gateway.polymarket.us/v1/markets/tec-mls-winner-2026-11-07-sea/bbo)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `slug` | `slug` |  | `Y` |  | Exact Polymarket US market slug: the slug field of a /v1/markets row. A game's game-winner (moneyline) market is "aec-" + the Gamma event's usId (aec-nba-sa-sac-2026-11-06) and futures have their own (tec-mls-winner-2026-11-07-sea), so pass the listing's slug rather than building one. |

### Returns {#polymarket_us_market_bbo-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `market_slug` | String | Market slug the quote is for (the `slug` argument). |
| `shares_traded` | String | Contracts traded over the market's life, as a decimal string. |
| `open_interest` | String | Contracts currently held open, as a decimal string; "0.0000" once the market has settled. |
| `ask_depth` | Int64 | Number of price levels on the ask side of the book. |
| `bid_depth` | Int64 | Number of price levels on the bid side of the book. |
| `state` | String | Market state code (e.g. "MARKET_STATE_OPEN", "MARKET_STATE_EXPIRED"). |
| `bid_shares` | String | Contracts resting on the bid side of the book, as a decimal string. |
| `ask_shares` | String | Contracts resting on the ask side of the book, as a decimal string. |
| `current_px_value` | String | Current price, in dollars per contract (0 to 1), as a decimal string. |
| `current_px_currency` | String | Currency of `current_px_value` (e.g. "USD"). |
| `last_trade_px_value` | String | Price of the last trade, in dollars per contract, as a decimal string. |
| `last_trade_px_currency` | String | Currency of `last_trade_px_value` (e.g. "USD"). |
| `settlement_px_value` | String | Settlement price, in dollars per contract, as a decimal string; "1.0000" or "0.0000" once the market has resolved. |
| `settlement_px_currency` | String | Currency of `settlement_px_value` (e.g. "USD"). |
| `best_ask_value` | String | Lowest ask, in dollars per contract, as a decimal string. Absent from an expired market, whose bestAsk is null. |
| `best_ask_currency` | String | Currency of `best_ask_value` (e.g. "USD"). |
| `best_bid_value` | String | Highest bid, in dollars per contract, as a decimal string. Absent from an expired market, whose bestBid is null. |
| `best_bid_currency` | String | Currency of `best_bid_value` (e.g. "USD"). |
| `last_price_sample_long_px_value` | String | Long-side (Yes) price in the latest price sample, as a decimal string. |
| `last_price_sample_long_px_currency` | String | Currency of `last_price_sample_long_px_value` (e.g. "USD"). |
| `last_price_sample_short_px_value` | String | Short-side (No) price in the latest price sample, as a decimal string. |
| `last_price_sample_short_px_currency` | String | Currency of `last_price_sample_short_px_value` (e.g. "USD"). |
| `last_price_sample_ts` | String | ISO-8601 UTC time of the latest price sample, to the nanosecond. |
| `long_quote_value` | String | Price to buy the long (Yes) side now, as a decimal string; only on an open market. |
| `long_quote_currency` | String | Currency of `long_quote_value` (e.g. "USD"). |
| `short_quote_value` | String | Price to buy the short (No) side now, as a decimal string; only on an open market. |
| `short_quote_currency` | String | Currency of `short_quote_value` (e.g. "USD"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_us_market_bbo-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_us_market_bbo-example}

```python
polymarket_us_market_bbo(slug='tec-mls-winner-2026-11-07-sea')
```

## polymarket_us_market_book

Order book for one Polymarket US market slug, one row per bid or offer level.

**Endpoint URL:** `GET https://gateway.polymarket.us/v1/markets/{slug}/book`

**Valid URL:** [https://gateway.polymarket.us/v1/markets/tec-mls-winner-2026-11-07-sea/book](https://gateway.polymarket.us/v1/markets/tec-mls-winner-2026-11-07-sea/book)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `slug` | `slug` |  | `Y` |  | Exact Polymarket US market slug: the slug field of a /v1/markets row. A game's game-winner (moneyline) market is "aec-" + the Gamma event's usId (aec-nba-sa-sac-2026-11-06) and futures have their own (tec-mls-winner-2026-11-07-sea), so pass the listing's slug rather than building one. |

### Returns {#polymarket_us_market_book-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `market_slug` | String | Market slug the book is for (the `slug` argument), repeated on every level. |
| `state` | String | Market state code (e.g. "MARKET_STATE_OPEN"), repeated on every level; an expired market's book has no levels, so it is zero rows. |
| `transact_time` | String | ISO-8601 UTC time of the book snapshot, to the nanosecond. |
| `side` | String | "bid" or "ask" (Polymarket US names its asks offers; "ask" matches `polymarket_clob_book`). |
| `qty` | String | Contracts resting at this price level, as a decimal string. |
| `px_value` | String | Price of the level, in dollars per contract (0 to 1), as a decimal string. |
| `px_currency` | String | Currency of `px_value` (e.g. "USD"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_us_market_book-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_us_market_book-example}

```python
polymarket_us_market_book(slug='tec-mls-winner-2026-11-07-sea')
```

## polymarket_us_market_by_id

One Polymarket US market by its gateway id.

**Endpoint URL:** `GET https://gateway.polymarket.us/v1/market/id/{id}`

**Valid URL:** [https://gateway.polymarket.us/v1/market/id/7984](https://gateway.polymarket.us/v1/market/id/7984)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  | `Y` |  | Polymarket US market id: the id field of a /v1/markets row (its own namespace, not a Gamma id). |

### Returns {#polymarket_us_market_by_id-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | Polymarket US market id, a numeric string (the `id` argument); its own namespace, not a Gamma market id. |
| `question` | String | Market title as Polymarket US displays it (e.g. "Los Angeles vs. Tennessee"). |
| `slug` | String | Market slug, the key for `polymarket_us_market_bbo`, `_book`, `_settlement` and `_by_slug`. A game-winner (moneyline) market's is "aec-" plus the Gamma event's usId (e.g. "aec-nfl-lac-ten-2025-11-02"); futures have their own ("tec-"), so pass the listing's slug rather than building one. |
| `end_date` | String | ISO-8601 UTC time the market is scheduled to close. |
| `category` | String | Top-level market category (e.g. "sports"). |
| `start_date` | String | ISO-8601 UTC time the market opened for trading. |
| `image` | String | URL of the market's subject image (a team logo) on an open market. |
| `description` | String | Resolution rules in prose, including what happens if the game is postponed or canceled. |
| `active` | Boolean | Whether the market is active; the `active` filter matches on it. |
| `market_type` | String | Market type (e.g. "moneyline", "futures"). |
| `closed` | Boolean | Whether the market is closed to trading; the `closed` filter matches on it. |
| `created_at` | String | ISO-8601 UTC time the market record was created. |
| `updated_at` | String | ISO-8601 UTC time the market record was last updated. |
| `archived` | Boolean | Whether the market is archived. |
| `order_price_min_tick_size` | Float64 | Smallest price increment an order may use, in dollars per contract (e.g. 0.001). |
| `game_start_time` | String | ISO-8601 UTC scheduled start of the game the market is on; null for markets not tied to one game. |
| `manual_activation` | Boolean | Whether the market is opened for trading by hand rather than on a schedule. |
| `sports_market_type` | String | Sports market type (e.g. "moneyline", "futures"). |
| `market_sides` | String | JSON-encoded list of the market's two sides, each with its id, description, price, long flag and team (name, abbreviation, league, record, logos, colors and provider ids). |
| `outcomes` | String | JSON array serialised inside a string naming the outcomes (e.g. "[\"Chargers\",\"Titans\"]"). |
| `outcome_prices` | String | JSON array serialised inside a string of each outcome's price, in the order of `outcomes`; a resolved market reads "1" for the winner and "0" for the loser. |
| `ep3_status` | String | Status code from Polymarket US's EP3 system (e.g. "EXPIRED"); the API does not document EP3. |
| `status` | String | Market status code (e.g. "MARKET_STATUS_RESOLVED"). |
| `asset_price_terms` | String | JSON-encoded price terms for markets on an asset price; null for sports markets. |
| `sports_market_type_v2` | String | Sports market type as an enum code (e.g. "SPORTS_MARKET_TYPE_MONEYLINE"). |
| `hidden` | Boolean | Whether the market is hidden from the Polymarket US front end. |
| `tags` | String | JSON-encoded list of the market's tags; empty on the captured sports markets. |
| `title` | String | Display title of the market's subject on an open market (e.g. "Vancouver Whitecaps FC" on an MLS Cup future); closed game markets omit it, like the other open-market display fields. |
| `subtitle` | String | Secondary display line on an open market, such as the team's record ("10-2-2"). |
| `color` | String | Hex display color of the market's subject on an open market (e.g. "#00245E"). |
| `fee_coefficient` | Float64 | Coefficient of the exchange's trading-fee formula for this market (e.g. 0.095). |
| `title_short` | String | Short display title of the market's subject on an open market. |
| `ep3_synced_at` | String | ISO-8601 UTC time the market last synced with Polymarket US's EP3 system. |
| `minimum_trade_qty` | Float64 | Smallest number of contracts a single trade may be for, as Float64 (1 on most markets; 0.01 = 1% of a contract). |
| `combo_enabled` | Boolean | Whether the market can be used as a leg of a combination (parlay) order. |
| `politics` | String | JSON-encoded politics metadata; null for sports markets. |
| `event_subcategory` | String | Sport subcategory of the market's event on an open market (e.g. "SOCCER"). |
| `best_bid_quote_value` | String | Highest bid in dollars per contract, as a decimal string. Present on an open market only. |
| `best_bid_quote_currency` | String | Currency of `best_bid_quote_value` (e.g. "USD"). |
| `best_ask_quote_value` | String | Lowest ask in dollars per contract, as a decimal string. Present on an open market only. |
| `best_ask_quote_currency` | String | Currency of `best_ask_quote_value` (e.g. "USD"). |
| `singles_rfq_config_enabled` | Boolean | Whether single-market request-for-quote trading is enabled. |
| `singles_rfq_config_midpoint_deviation` | Float64 | Largest deviation from the midpoint a request-for-quote reply may have. |
| `singles_rfq_config_min_order_value_usd` | Int64 | Smallest order value, in US dollars, accepted by request for quote. |
| `singles_rfq_config_max_quote_age_seconds` | Int64 | Longest a request-for-quote reply stays valid, in seconds. |
| `singles_rfq_config_quote_windows` | String | JSON-encoded list of the time windows in which request for quote is open. |
| `singles_rfq_config_rule_name` | String | Name of the request-for-quote rule set applied (e.g. "default"). |
| `singles_rfq_config_version` | String | Version label of the request-for-quote configuration (e.g. "singles-half-markets-v2"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_us_market_by_id-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_us_market_by_id-example}

```python
polymarket_us_market_by_id(id='7984')
```

## polymarket_us_market_by_slug

One Polymarket US market by its slug.

**Endpoint URL:** `GET https://gateway.polymarket.us/v1/market/slug/{slug}`

**Valid URL:** [https://gateway.polymarket.us/v1/market/slug/tec-mls-winner-2026-11-07-sea](https://gateway.polymarket.us/v1/market/slug/tec-mls-winner-2026-11-07-sea)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `slug` | `slug` |  | `Y` |  | Exact Polymarket US market slug: the slug field of a /v1/markets row. A game's game-winner (moneyline) market is "aec-" + the Gamma event's usId (aec-nba-sa-sac-2026-11-06) and futures have their own (tec-mls-winner-2026-11-07-sea), so pass the listing's slug rather than building one. |

### Returns {#polymarket_us_market_by_slug-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | Polymarket US market id, a numeric string and the key for `polymarket_us_market_by_id`; not a Gamma market id. |
| `question` | String | Market title as Polymarket US displays it (e.g. "Los Angeles vs. Tennessee"). |
| `slug` | String | Market slug, the key for `polymarket_us_market_bbo`, `_book`, `_settlement` and `_by_slug`. A game-winner (moneyline) market's is "aec-" plus the Gamma event's usId (e.g. "aec-nfl-lac-ten-2025-11-02"); futures have their own ("tec-"), so pass the listing's slug rather than building one. |
| `end_date` | String | ISO-8601 UTC time the market is scheduled to close. |
| `category` | String | Top-level market category (e.g. "sports"). |
| `start_date` | String | ISO-8601 UTC time the market opened for trading. |
| `image` | String | URL of the market's subject image (a team logo) on an open market. |
| `description` | String | Resolution rules in prose, including what happens if the game is postponed or canceled. |
| `active` | Boolean | Whether the market is active; the `active` filter matches on it. |
| `market_type` | String | Market type (e.g. "moneyline", "futures"). |
| `closed` | Boolean | Whether the market is closed to trading; the `closed` filter matches on it. |
| `created_at` | String | ISO-8601 UTC time the market record was created. |
| `updated_at` | String | ISO-8601 UTC time the market record was last updated. |
| `archived` | Boolean | Whether the market is archived. |
| `order_price_min_tick_size` | Float64 | Smallest price increment an order may use, in dollars per contract (e.g. 0.001). |
| `game_start_time` | String | ISO-8601 UTC scheduled start of the game the market is on; null for markets not tied to one game. |
| `manual_activation` | Boolean | Whether the market is opened for trading by hand rather than on a schedule. |
| `sports_market_type` | String | Sports market type (e.g. "moneyline", "futures"). |
| `market_sides` | String | JSON-encoded list of the market's two sides, each with its id, description, price, long flag and team (name, abbreviation, league, record, logos, colors and provider ids). |
| `outcomes` | String | JSON array serialised inside a string naming the outcomes (e.g. "[\"Chargers\",\"Titans\"]"). |
| `outcome_prices` | String | JSON array serialised inside a string of each outcome's price, in the order of `outcomes`; a resolved market reads "1" for the winner and "0" for the loser. |
| `ep3_status` | String | Status code from Polymarket US's EP3 system (e.g. "EXPIRED"); the API does not document EP3. |
| `status` | String | Market status code (e.g. "MARKET_STATUS_RESOLVED"). |
| `asset_price_terms` | String | JSON-encoded price terms for markets on an asset price; null for sports markets. |
| `sports_market_type_v2` | String | Sports market type as an enum code (e.g. "SPORTS_MARKET_TYPE_MONEYLINE"). |
| `hidden` | Boolean | Whether the market is hidden from the Polymarket US front end. |
| `tags` | String | JSON-encoded list of the market's tags; empty on the captured sports markets. |
| `title` | String | Display title of the market's subject on an open market (e.g. "Vancouver Whitecaps FC" on an MLS Cup future); closed game markets omit it, like the other open-market display fields. |
| `subtitle` | String | Secondary display line on an open market, such as the team's record ("10-2-2"). |
| `color` | String | Hex display color of the market's subject on an open market (e.g. "#00245E"). |
| `fee_coefficient` | Float64 | Coefficient of the exchange's trading-fee formula for this market (e.g. 0.095). |
| `title_short` | String | Short display title of the market's subject on an open market. |
| `ep3_synced_at` | String | ISO-8601 UTC time the market last synced with Polymarket US's EP3 system. |
| `minimum_trade_qty` | Float64 | Smallest number of contracts a single trade may be for, as Float64 (1 on most markets; 0.01 = 1% of a contract). |
| `combo_enabled` | Boolean | Whether the market can be used as a leg of a combination (parlay) order. |
| `politics` | String | JSON-encoded politics metadata; null for sports markets. |
| `event_subcategory` | String | Sport subcategory of the market's event on an open market (e.g. "SOCCER"). |
| `best_bid_quote_value` | String | Highest bid in dollars per contract, as a decimal string. Present on an open market only. |
| `best_bid_quote_currency` | String | Currency of `best_bid_quote_value` (e.g. "USD"). |
| `best_ask_quote_value` | String | Lowest ask in dollars per contract, as a decimal string. Present on an open market only. |
| `best_ask_quote_currency` | String | Currency of `best_ask_quote_value` (e.g. "USD"). |
| `singles_rfq_config_enabled` | Boolean | Whether single-market request-for-quote trading is enabled. |
| `singles_rfq_config_midpoint_deviation` | Float64 | Largest deviation from the midpoint a request-for-quote reply may have. |
| `singles_rfq_config_min_order_value_usd` | Int64 | Smallest order value, in US dollars, accepted by request for quote. |
| `singles_rfq_config_max_quote_age_seconds` | Int64 | Longest a request-for-quote reply stays valid, in seconds. |
| `singles_rfq_config_quote_windows` | String | JSON-encoded list of the time windows in which request for quote is open. |
| `singles_rfq_config_rule_name` | String | Name of the request-for-quote rule set applied (e.g. "default"). |
| `singles_rfq_config_version` | String | Version label of the request-for-quote configuration (e.g. "singles-half-markets-v2"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_us_market_by_slug-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_us_market_by_slug-example}

```python
polymarket_us_market_by_slug(slug='tec-mls-winner-2026-11-07-sea')
```

## polymarket_us_market_settlement

Settlement price of one Polymarket US market slug, once the market has settled.

**Endpoint URL:** `GET https://gateway.polymarket.us/v1/markets/{slug}/settlement`

**Valid URL:** [https://gateway.polymarket.us/v1/markets/aec-nba-sa-sac-2026-11-06/settlement](https://gateway.polymarket.us/v1/markets/aec-nba-sa-sac-2026-11-06/settlement)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `slug` | `slug` |  | `Y` |  | Exact Polymarket US market slug: the slug field of a /v1/markets row. A game's game-winner (moneyline) market is "aec-" + the Gamma event's usId (aec-nba-sa-sac-2026-11-06) and futures have their own (tec-mls-winner-2026-11-07-sea), so pass the listing's slug rather than building one. |

### Returns {#polymarket_us_market_settlement-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `slug` | String | Market slug the settlement is for (the `slug` argument). |
| `settlement` | Float64 | Price per contract the market settled at, in dollars, as Float64 (the captured expired game settled at 1, its book's settlementPx). Polymarket's own schema declares a 404 for this route, answered for a market that has not settled (probed 2026-11-18) and raised as NoDataError. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_us_market_settlement-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_us_market_settlement-example}

```python
polymarket_us_market_settlement(slug='aec-nba-sa-sac-2026-11-06')
```

## polymarket_us_markets

Markets on the Polymarket US exchange, offset-paged and filterable by active or closed.

**Endpoint URL:** `GET https://gateway.polymarket.us/v1/markets`

**Valid URL:** [https://gateway.polymarket.us/v1/markets?limit=3&closed=false&active=true](https://gateway.polymarket.us/v1/markets?limit=3&closed=false&active=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Page size. |
| `closed` | `closed` |  |  | `Y` | Filter closed markets/events (true/false). |
| `active` | `active` |  |  | `Y` | Filter active markets (true/false). |
| `offset` | `offset` |  |  | `Y` | Row offset; offset=1 shifts the page (verified 2026-11-18). |

### Returns {#polymarket_us_markets-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | Polymarket US market id, a numeric string and the key for `polymarket_us_market_by_id` (the captured unfiltered page started at "1"); not a Gamma market id. |
| `question` | String | Market title as Polymarket US displays it (e.g. "Los Angeles vs. Tennessee"). |
| `slug` | String | Market slug, the key for `polymarket_us_market_bbo`, `_book`, `_settlement` and `_by_slug`. A game-winner (moneyline) market's is "aec-" plus the Gamma event's usId (e.g. "aec-nfl-lac-ten-2025-11-02"); futures have their own ("tec-"), so pass the listing's slug rather than building one. |
| `end_date` | String | ISO-8601 UTC time the market is scheduled to close. |
| `category` | String | Top-level market category (e.g. "sports"). |
| `start_date` | String | ISO-8601 UTC time the market opened for trading. |
| `image` | String | URL of the market's subject image (a team logo) on an open market. |
| `description` | String | Resolution rules in prose, including what happens if the game is postponed or canceled. |
| `active` | Boolean | Whether the market is active; the `active` filter matches on it. |
| `market_type` | String | Market type (e.g. "moneyline", "futures"). |
| `closed` | Boolean | Whether the market is closed to trading; the `closed` filter matches on it. |
| `created_at` | String | ISO-8601 UTC time the market record was created. |
| `updated_at` | String | ISO-8601 UTC time the market record was last updated. |
| `archived` | Boolean | Whether the market is archived. |
| `order_price_min_tick_size` | Float64 | Smallest price increment an order may use, in dollars per contract (e.g. 0.001). |
| `game_start_time` | String | ISO-8601 UTC scheduled start of the game the market is on; null for markets not tied to one game. |
| `manual_activation` | Boolean | Whether the market is opened for trading by hand rather than on a schedule. |
| `sports_market_type` | String | Sports market type (e.g. "moneyline", "futures"). |
| `market_sides` | String | JSON-encoded list of the market's two sides, each with its id, description, price, long flag and team (name, abbreviation, league, record, logos, colors and provider ids). |
| `outcomes` | String | JSON array serialised inside a string naming the outcomes (e.g. "[\"Chargers\",\"Titans\"]"). |
| `outcome_prices` | String | JSON array serialised inside a string of each outcome's price, in the order of `outcomes`; a resolved market reads "1" for the winner and "0" for the loser. |
| `ep3_status` | String | Status code from Polymarket US's EP3 system (e.g. "EXPIRED"); the API does not document EP3. |
| `status` | String | Market status code (e.g. "MARKET_STATUS_RESOLVED"). |
| `asset_price_terms` | String | JSON-encoded price terms for markets on an asset price; null for sports markets. |
| `sports_market_type_v2` | String | Sports market type as an enum code (e.g. "SPORTS_MARKET_TYPE_MONEYLINE"). |
| `hidden` | Boolean | Whether the market is hidden from the Polymarket US front end. |
| `tags` | String | JSON-encoded list of the market's tags; empty on the captured sports markets. |
| `title` | String | Display title of the market's subject on an open market (e.g. "Vancouver Whitecaps FC" on an MLS Cup future); closed game markets omit it, like the other open-market display fields. |
| `subtitle` | String | Secondary display line on an open market, such as the team's record ("10-2-2"). |
| `color` | String | Hex display color of the market's subject on an open market (e.g. "#00245E"). |
| `fee_coefficient` | Float64 | Coefficient of the exchange's trading-fee formula for this market (e.g. 0.095). |
| `title_short` | String | Short display title of the market's subject on an open market. |
| `ep3_synced_at` | String | ISO-8601 UTC time the market last synced with Polymarket US's EP3 system. |
| `minimum_trade_qty` | Float64 | Smallest number of contracts a single trade may be for, as Float64 (1 on most markets; 0.01 = 1% of a contract). |
| `combo_enabled` | Boolean | Whether the market can be used as a leg of a combination (parlay) order. |
| `politics` | String | JSON-encoded politics metadata; null for sports markets. |
| `event_subcategory` | String | Sport subcategory of the market's event on an open market (e.g. "SOCCER"). |
| `best_bid_quote_value` | String | Highest bid in dollars per contract, as a decimal string. Present on an open market only. |
| `best_bid_quote_currency` | String | Currency of `best_bid_quote_value` (e.g. "USD"). |
| `best_ask_quote_value` | String | Lowest ask in dollars per contract, as a decimal string. Present on an open market only. |
| `best_ask_quote_currency` | String | Currency of `best_ask_quote_value` (e.g. "USD"). |
| `singles_rfq_config_enabled` | Boolean | Whether single-market request-for-quote trading is enabled. |
| `singles_rfq_config_midpoint_deviation` | Float64 | Largest deviation from the midpoint a request-for-quote reply may have. |
| `singles_rfq_config_min_order_value_usd` | Int64 | Smallest order value, in US dollars, accepted by request for quote. |
| `singles_rfq_config_max_quote_age_seconds` | Int64 | Longest a request-for-quote reply stays valid, in seconds. |
| `singles_rfq_config_quote_windows` | String | JSON-encoded list of the time windows in which request for quote is open. |
| `singles_rfq_config_rule_name` | String | Name of the request-for-quote rule set applied (e.g. "default"). |
| `singles_rfq_config_version` | String | Version label of the request-for-quote configuration (e.g. "singles-half-markets-v2"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#polymarket_us_markets-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Polymarket answered 404 (unknown market id, token id or market slug, or a Polymarket US market that has not settled yet). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#polymarket_us_markets-example}

```python
polymarket_us_markets(active='true', closed='false', limit='3')
```
