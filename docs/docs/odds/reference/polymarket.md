---
title: ODDS — Polymarket read APIs (gamma-api + clob.polymarket.com)
sidebar_label: Polymarket read APIs (gamma-api + clob.polymarket.com)
description: "ODDS — Polymarket read APIs (gamma-api + clob.polymarket.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# ODDS — Polymarket read APIs (gamma-api + clob.polymarket.com)

`sportsdataverse.odds` — 8 endpoints.

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
| `id` | character |  |
| `ticker` | character |  |
| `slug` | character |  |
| `title` | character |  |
| `description` | character |  |
| `resolution_source` | character |  |
| `start_date` | character |  |
| `creation_date` | character |  |
| `end_date` | character |  |
| `image` | character |  |
| `icon` | character |  |
| `active` | logical |  |
| `closed` | logical |  |
| `archived` | logical |  |
| `new` | logical |  |
| `featured` | logical |  |
| `restricted` | logical |  |
| `liquidity` | numeric |  |
| `volume` | numeric |  |
| `open_interest` | numeric |  |
| `sort_by` | character |  |
| `created_at` | character |  |
| `updated_at` | character |  |
| `competitive` | numeric |  |
| `volume24hr` | numeric |  |
| `volume1wk` | numeric |  |
| `volume1mo` | numeric |  |
| `volume1yr` | numeric |  |
| `enable_order_book` | logical |  |
| `liquidity_clob` | numeric |  |
| `neg_risk` | logical |  |
| `neg_risk_market_id` | character |  |
| `comment_count` | integer |  |
| `markets` | character |  |
| `tags` | character |  |
| `cyom` | logical |  |
| `show_all_outcomes` | logical |  |
| `show_market_images` | logical |  |
| `enable_neg_risk` | logical |  |
| `automatically_active` | logical |  |
| `gmp_chart_mode` | character |  |
| `neg_risk_augmented` | logical |  |
| `featured_order` | numeric |  |
| `estimate_value` | logical |  |
| `cumulative_markets` | logical |  |
| `pending_deployment` | logical |  |
| `deploying` | logical |  |
| `deploying_timestamp` | character |  |
| `version` | character |  |
| `event_metadata_context_requires_regen` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#polymarket_gamma_events-example}

```python
polymarket_gamma_events(closed='false', limit='3', tag_slug='sports')
```

_Last validated n/a._

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
| `id` | character |  |
| `question` | character |  |
| `condition_id` | character |  |
| `slug` | character |  |
| `resolution_source` | character |  |
| `end_date` | character |  |
| `liquidity` | character |  |
| `start_date` | character |  |
| `image` | character |  |
| `icon` | character |  |
| `description` | character |  |
| `outcomes` | character |  |
| `outcome_prices` | character |  |
| `volume` | character |  |
| `active` | logical |  |
| `closed` | logical |  |
| `market_maker_address` | character |  |
| `created_at` | character |  |
| `updated_at` | character |  |
| `new` | logical |  |
| `featured` | logical |  |
| `submitted_by` | character |  |
| `archived` | logical |  |
| `resolved_by` | character |  |
| `restricted` | logical |  |
| `group_item_title` | character |  |
| `group_item_threshold` | character |  |
| `question_id` | character |  |
| `enable_order_book` | logical |  |
| `order_price_min_tick_size` | numeric |  |
| `order_min_size` | integer |  |
| `volume_num` | numeric |  |
| `liquidity_num` | numeric |  |
| `end_date_iso` | character |  |
| `start_date_iso` | character |  |
| `has_reviewed_dates` | logical |  |
| `volume24hr` | numeric |  |
| `volume1wk` | numeric |  |
| `volume1mo` | numeric |  |
| `volume1yr` | numeric |  |
| `clob_token_ids` | character |  |
| `combo_status` | character |  |
| `uma_bond` | character |  |
| `uma_reward` | character |  |
| `volume24hr_clob` | numeric |  |
| `volume1wk_clob` | numeric |  |
| `volume1mo_clob` | numeric |  |
| `volume1yr_clob` | numeric |  |
| `volume_clob` | numeric |  |
| `liquidity_clob` | numeric |  |
| `maker_base_fee` | integer |  |
| `taker_base_fee` | integer |  |
| `custom_liveness` | integer |  |
| `accepting_orders` | logical |  |
| `neg_risk` | logical |  |
| `neg_risk_market_id` | character |  |
| `neg_risk_request_id` | character |  |
| `ready` | logical |  |
| `funded` | logical |  |
| `accepting_orders_timestamp` | character |  |
| `cyom` | logical |  |
| `competitive` | numeric |  |
| `approved` | logical |  |
| `rewards_min_size` | integer |  |
| `rewards_max_spread` | numeric |  |
| `spread` | numeric |  |
| `one_day_price_change` | numeric |  |
| `one_hour_price_change` | numeric |  |
| `one_week_price_change` | numeric |  |
| `one_month_price_change` | numeric |  |
| `last_trade_price` | numeric |  |
| `best_bid` | numeric |  |
| `best_ask` | numeric |  |
| `automatically_active` | logical |  |
| `clear_book_on_start` | logical |  |
| `series_color` | character |  |
| `show_gmp_series` | logical |  |
| `show_gmp_outcome` | logical |  |
| `manual_activation` | logical |  |
| `neg_risk_other` | logical |  |
| `uma_resolution_statuses` | character |  |
| `pending_deployment` | logical |  |
| `deploying` | logical |  |
| `deploying_timestamp` | character |  |
| `rfq_enabled` | logical |  |
| `holding_rewards_enabled` | logical |  |
| `fees_enabled` | logical |  |
| `fee_type` | character |  |
| `version` | character |  |
| `fee_schedule_exponent` | integer |  |
| `fee_schedule_rate` | numeric |  |
| `fee_schedule_taker_only` | logical |  |
| `fee_schedule_rebate_rate` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#polymarket_gamma_market-example}

```python
polymarket_gamma_market(id='608565')
```

_Last validated n/a._

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
| `id` | character |  |
| `question` | character |  |
| `condition_id` | character |  |
| `slug` | character |  |
| `resolution_source` | character |  |
| `end_date` | character |  |
| `liquidity` | character |  |
| `start_date` | character |  |
| `image` | character |  |
| `icon` | character |  |
| `description` | character |  |
| `outcomes` | character |  |
| `outcome_prices` | character |  |
| `volume` | character |  |
| `active` | logical |  |
| `closed` | logical |  |
| `market_maker_address` | character |  |
| `created_at` | character |  |
| `updated_at` | character |  |
| `new` | logical |  |
| `featured` | logical |  |
| `submitted_by` | character |  |
| `archived` | logical |  |
| `resolved_by` | character |  |
| `restricted` | logical |  |
| `question_id` | character |  |
| `enable_order_book` | logical |  |
| `order_price_min_tick_size` | numeric |  |
| `order_min_size` | integer |  |
| `uma_resolution_status` | character |  |
| `volume_num` | numeric |  |
| `liquidity_num` | numeric |  |
| `end_date_iso` | character |  |
| `start_date_iso` | character |  |
| `has_reviewed_dates` | logical |  |
| `volume24hr` | numeric |  |
| `volume1wk` | numeric |  |
| `volume1mo` | numeric |  |
| `volume1yr` | numeric |  |
| `clob_token_ids` | character |  |
| `combo_status` | character |  |
| `uma_bond` | character |  |
| `uma_reward` | character |  |
| `volume24hr_clob` | numeric |  |
| `volume1wk_clob` | numeric |  |
| `volume1mo_clob` | numeric |  |
| `volume1yr_clob` | numeric |  |
| `volume_clob` | numeric |  |
| `liquidity_clob` | numeric |  |
| `maker_base_fee` | integer |  |
| `taker_base_fee` | integer |  |
| `custom_liveness` | integer |  |
| `accepting_orders` | logical |  |
| `neg_risk` | logical |  |
| `events` | character |  |
| `ready` | logical |  |
| `funded` | logical |  |
| `accepting_orders_timestamp` | character |  |
| `cyom` | logical |  |
| `competitive` | numeric |  |
| `approved` | logical |  |
| `rewards_min_size` | integer |  |
| `rewards_max_spread` | numeric |  |
| `spread` | numeric |  |
| `one_day_price_change` | numeric |  |
| `last_trade_price` | numeric |  |
| `best_bid` | numeric |  |
| `best_ask` | numeric |  |
| `automatically_active` | logical |  |
| `clear_book_on_start` | logical |  |
| `manual_activation` | logical |  |
| `neg_risk_other` | logical |  |
| `uma_resolution_statuses` | character |  |
| `pending_deployment` | logical |  |
| `deploying` | logical |  |
| `deploying_timestamp` | character |  |
| `rfq_enabled` | logical |  |
| `event_start_time` | character |  |
| `holding_rewards_enabled` | logical |  |
| `fees_enabled` | logical |  |
| `fee_type` | character |  |
| `version` | character |  |
| `fee_schedule_exponent` | integer |  |
| `fee_schedule_rate` | numeric |  |
| `fee_schedule_taker_only` | logical |  |
| `fee_schedule_rebate_rate` | numeric |  |
| `group_item_title` | character |  |
| `group_item_threshold` | character |  |
| `neg_risk_market_id` | character |  |
| `neg_risk_request_id` | character |  |
| `clob_rewards` | character |  |
| `one_week_price_change` | numeric |  |
| `one_month_price_change` | numeric |  |
| `one_year_price_change` | numeric |  |
| `series_color` | character |  |
| `show_gmp_series` | logical |  |
| `show_gmp_outcome` | logical |  |
| `one_hour_price_change` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#polymarket_gamma_markets-example}

```python
polymarket_gamma_markets(ascending='false', closed='false', limit='3', order='volume24hr')
```

_Last validated n/a._

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
| `id` | character |  |
| `label` | character |  |
| `slug` | character |  |
| `created_at` | character |  |
| `updated_at` | character |  |
| `published_at` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#polymarket_gamma_tags-example}

```python
polymarket_gamma_tags(limit='3')
```

_Last validated n/a._

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
| `market` | character |  |
| `asset_id` | character |  |
| `timestamp` | character |  |
| `hash` | character |  |
| `min_order_size` | character |  |
| `tick_size` | character |  |
| `neg_risk` | logical |  |
| `last_trade_price` | character |  |
| `side` | character |  |
| `price` | character |  |
| `size` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#polymarket_clob_book-example}

```python
polymarket_clob_book(token_id='14940935985254400645996902276818598247231942791302218826999289776538227075176')
```

_Last validated n/a._

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
| `enable_order_book` | logical |  |
| `active` | logical |  |
| `closed` | logical |  |
| `archived` | logical |  |
| `accepting_orders` | logical |  |
| `accepting_order_timestamp` | character |  |
| `minimum_order_size` | integer |  |
| `minimum_tick_size` | numeric |  |
| `condition_id` | character |  |
| `question_id` | character |  |
| `question` | character |  |
| `description` | character |  |
| `market_slug` | character |  |
| `end_date_iso` | character |  |
| `game_start_time` | character |  |
| `seconds_delay` | integer |  |
| `fpmm` | character |  |
| `maker_base_fee` | integer |  |
| `taker_base_fee` | integer |  |
| `notifications_enabled` | logical |  |
| `neg_risk` | logical |  |
| `neg_risk_market_id` | character |  |
| `neg_risk_request_id` | character |  |
| `icon` | character |  |
| `image` | character |  |
| `is_50_50_outcome` | logical |  |
| `tokens` | character |  |
| `tags` | character |  |
| `version` | character |  |
| `rewards_rates` | character |  |
| `rewards_min_size` | integer |  |
| `rewards_max_spread` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#polymarket_clob_markets-example}

```python
polymarket_clob_markets(next_cursor='MA==')
```

_Last validated n/a._

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
| `mid` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#polymarket_clob_midpoint-example}

```python
polymarket_clob_midpoint(token_id='14940935985254400645996902276818598247231942791302218826999289776538227075176')
```

_Last validated n/a._

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
| `price` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#polymarket_clob_price-example}

```python
polymarket_clob_price(side='buy', token_id='14940935985254400645996902276818598247231942791302218826999289776538227075176')
```

_Last validated n/a._
