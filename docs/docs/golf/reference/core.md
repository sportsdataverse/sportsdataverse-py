---
title: Golf — ESPN core API (v2)
sidebar_label: ESPN core API (v2)
description: "Golf — ESPN core API (v2) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 22
toc_max_heading_level: 2
---
# Golf — ESPN core API (v2)

`sportsdataverse.golf` — 25 endpoints.

## espn_golf_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_golf_league_root-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String |  |
| `color` | String |  |
| `display_name` | String |  |
| `gender` | String |  |
| `guid` | String |  |
| `id` | Int64 |  |
| `is_tournament` | Boolean |  |
| `links` | String |  |
| `logos` | String |  |
| `name` | String |  |
| `short_name` | String |  |
| `slug` | String |  |
| `uid` | String |  |
| `athletes_$ref` | String |  |
| `awards_$ref` | String |  |
| `calendar_$ref` | String |  |
| `draft_$ref` | String |  |
| `events_$ref` | String |  |
| `franchises_$ref` | String |  |
| `group_$ref` | String |  |
| `groups_$ref` | String |  |
| `leaders_$ref` | String |  |
| `notes_$ref` | String |  |
| `rankings_$ref` | String |  |
| `season_$ref` | String |  |
| `season_athletes_$ref` | String |  |
| `season_coaches_$ref` | String |  |
| `season_display_name` | String |  |
| `season_end_date` | String |  |
| `season_futures_$ref` | String |  |
| `season_power_index_leaders_$ref` | String |  |
| `season_power_indexes_$ref` | String |  |
| `season_rankings_$ref` | String |  |
| `season_start_date` | String |  |
| `season_type_$ref` | String |  |
| `season_type_abbreviation` | String |  |
| `season_type_corrections_$ref` | String |  |
| `season_type_end_date` | String |  |
| `season_type_groups_$ref` | String |  |
| `season_type_has_groups` | Boolean |  |
| `season_type_has_legs` | Boolean |  |
| `season_type_has_standings` | Boolean |  |
| `season_type_id` | Int64 |  |
| `season_type_name` | String |  |
| `season_type_slug` | String |  |
| `season_type_start_date` | String |  |
| `season_type_type` | Int64 |  |
| `season_type_weeks_$ref` | String |  |
| `season_type_year` | Int64 |  |
| `season_types_$ref` | String |  |
| `season_types_count` | Int64 |  |
| `season_types_items` | String |  |
| `season_types_page_count` | Int64 |  |
| `season_types_page_index` | Int64 |  |
| `season_types_page_size` | Int64 |  |
| `season_year` | Int64 |  |
| `seasons_$ref` | String |  |
| `teams_$ref` | String |  |
| `transactions_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_league_root-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_league_root-example}

```python
espn_golf_league_root(league='pga')
```

## espn_golf_season_pointer

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/season`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/season](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_golf_season_pointer-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `display_name` | String |  |
| `end_date` | String |  |
| `start_date` | String |  |
| `year` | Int64 |  |
| `athletes_$ref` | String |  |
| `coaches_$ref` | String |  |
| `futures_$ref` | String |  |
| `power_index_leaders_$ref` | String |  |
| `power_indexes_$ref` | String |  |
| `rankings_$ref` | String |  |
| `type_$ref` | String |  |
| `type_abbreviation` | String |  |
| `type_corrections_$ref` | String |  |
| `type_end_date` | String |  |
| `type_groups_$ref` | String |  |
| `type_has_groups` | Boolean |  |
| `type_has_legs` | Boolean |  |
| `type_has_standings` | Boolean |  |
| `type_id` | Int64 |  |
| `type_name` | String |  |
| `type_slug` | String |  |
| `type_start_date` | String |  |
| `type_type` | Int64 |  |
| `type_weeks_$ref` | String |  |
| `type_year` | Int64 |  |
| `types_$ref` | String |  |
| `types_count` | Int64 |  |
| `types_items` | String |  |
| `types_page_count` | Int64 |  |
| `types_page_index` | Int64 |  |
| `types_page_size` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_season_pointer-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_season_pointer-example}

```python
espn_golf_season_pointer(league='pga')
```

## espn_golf_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons?limit=200](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_golf_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_seasons-example}

```python
espn_golf_seasons(league='pga')
```

## espn_golf_season_info

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/2024](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_golf_season_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `display_name` | String |  |
| `end_date` | String |  |
| `start_date` | String |  |
| `year` | Int64 |  |
| `awards_$ref` | String |  |
| `futures_$ref` | String |  |
| `leaders_$ref` | String |  |
| `power_index_leaders_$ref` | String |  |
| `power_indexes_$ref` | String |  |
| `rankings_$ref` | String |  |
| `type_$ref` | String |  |
| `type_abbreviation` | String |  |
| `type_corrections_$ref` | String |  |
| `type_end_date` | String |  |
| `type_groups_$ref` | String |  |
| `type_has_groups` | Boolean |  |
| `type_has_legs` | Boolean |  |
| `type_has_standings` | Boolean |  |
| `type_id` | Int64 |  |
| `type_leaders_$ref` | String |  |
| `type_name` | String |  |
| `type_slug` | String |  |
| `type_start_date` | String |  |
| `type_type` | Int64 |  |
| `type_weeks_$ref` | String |  |
| `type_year` | Int64 |  |
| `types_$ref` | String |  |
| `types_count` | Int64 |  |
| `types_items` | String |  |
| `types_page_count` | Int64 |  |
| `types_page_index` | Int64 |  |
| `types_page_size` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_season_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_season_info-example}

```python
espn_golf_season_info(league='pga', season=2024)
```

## espn_golf_season_types

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/{season}/types`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/2024/types](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/2024/types)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_golf_season_types-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_season_types-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_season_types-example}

```python
espn_golf_season_types(league='pga', season=2024)
```

## espn_golf_season_type

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/{season}/types/{season_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/2024/types/2](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/2024/types/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_golf_season_type-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String |  |
| `end_date` | String |  |
| `has_groups` | Boolean |  |
| `has_legs` | Boolean |  |
| `has_standings` | Boolean |  |
| `id` | Int64 |  |
| `name` | String |  |
| `slug` | String |  |
| `start_date` | String |  |
| `type` | Int64 |  |
| `year` | Int64 |  |
| `corrections_$ref` | String |  |
| `groups_$ref` | String |  |
| `leaders_$ref` | String |  |
| `weeks_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_season_type-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_season_type-example}

```python
espn_golf_season_type(league='pga', season=2024, season_type=2)
```

## espn_golf_season_type_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/{season}/types/{season_type}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/2024/types/2/leaders](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/2024/types/2/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_golf_season_type_leaders-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb, 2026-10-07); its rows sit under keys it does not read (top level: $ref, abbreviation, categories, id, name, type).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_season_type_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_season_type_leaders-example}

```python
espn_golf_season_type_leaders(league='pga', season=2024, season_type=2)
```

## espn_golf_season_players

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/{season}/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/2024/athletes?limit=100&page=1](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/seasons/2024/athletes?limit=100&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_golf_season_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_season_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_season_players-example}

```python
espn_golf_season_players(league='pga', season=2024)
```

## espn_golf_players_index

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/athletes?active=true&limit=100&page=1](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/athletes?active=true&limit=100&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `active` | `active` |  |  | `Y` | active query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_golf_players_index-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_players_index-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_players_index-example}

```python
espn_golf_players_index(league='pga')
```

## espn_golf_player_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/athletes/{athlete_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/athletes/4239](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_golf_player_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `active` | Boolean |  |
| `age` | Int64 |  |
| `date_of_birth` | String |  |
| `debut_year` | Int64 |  |
| `display_height` | String |  |
| `display_name` | String |  |
| `display_weight` | String |  |
| `first_name` | String |  |
| `full_name` | String |  |
| `guid` | String |  |
| `height` | Float64 |  |
| `id` | Int64 |  |
| `jersey` | String |  |
| `last_name` | String |  |
| `linked` | Boolean |  |
| `links` | String |  |
| `short_name` | String |  |
| `slug` | String |  |
| `type` | String |  |
| `uid` | String |  |
| `weight` | Float64 |  |
| `alternate_ids_sdr` | String |  |
| `birth_place_city` | String |  |
| `birth_place_state` | String |  |
| `college_$ref` | String |  |
| `college_athlete_$ref` | String |  |
| `contracts_$ref` | String |  |
| `draft_display_text` | String |  |
| `draft_pick_$ref` | String |  |
| `draft_round` | Int64 |  |
| `draft_selection` | Int64 |  |
| `draft_team_$ref` | String |  |
| `draft_year` | Int64 |  |
| `experience_years` | Int64 |  |
| `hand_abbreviation` | String |  |
| `hand_display_value` | String |  |
| `hand_type` | String |  |
| `headshot_alt` | String |  |
| `headshot_href` | String |  |
| `position_$ref` | String |  |
| `position_abbreviation` | String |  |
| `position_display_name` | String |  |
| `position_id` | Int64 |  |
| `position_leaf` | Boolean |  |
| `position_name` | String |  |
| `seasons_$ref` | String |  |
| `statistics_$ref` | String |  |
| `statisticslog_$ref` | String |  |
| `status_abbreviation` | String |  |
| `status_id` | Int64 |  |
| `status_name` | String |  |
| `status_type` | String |  |
| `team_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_player_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_player_core-example}

```python
espn_golf_player_core(league='pga', athlete_id='4239')
```

## espn_golf_player_statisticslog

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/athletes/{athlete_id}/statisticslog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/athletes/4239/statisticslog](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/athletes/4239/statisticslog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_golf_player_statisticslog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `statistics` | String |  |
| `season_$ref` | String |  |
| `count` | Int64 | Always null, since the statisticslog payload ships its rows under entries with no page envelope. |
| `page_count` | Int64 | Always null, since the statisticslog payload ships its rows under entries with no page envelope. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_player_statisticslog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_player_statisticslog-example}

```python
espn_golf_player_statisticslog(league='pga', athlete_id='4239')
```

## espn_golf_player_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/athletes/{athlete_id}/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/athletes/4239/seasons](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/athletes/4239/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_golf_player_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_player_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_player_seasons-example}

```python
espn_golf_player_seasons(league='pga', athlete_id='4239')
```

## espn_golf_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events?limit=500](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_golf_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_games-example}

```python
espn_golf_games(league='pga')
```

## espn_golf_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_golf_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `competitions` | String |  |
| `date` | String |  |
| `id` | Int64 |  |
| `links` | String |  |
| `name` | String |  |
| `short_name` | String |  |
| `time_valid` | Boolean |  |
| `uid` | String |  |
| `venues` | String |  |
| `league_$ref` | String |  |
| `season_$ref` | String |  |
| `season_type_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_game-example}

```python
espn_golf_game(league='pga', event_id='401584793')
```

## espn_golf_game_competition

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/{event_id}/competitions/{cid}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_golf_game_competition-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `attendance` | Int64 |  |
| `boxscore_available` | Boolean |  |
| `bracket_available` | Boolean |  |
| `commentary_available` | Boolean |  |
| `competitors` | String |  |
| `conference_competition` | Boolean |  |
| `conversation_available` | Boolean |  |
| `date` | String |  |
| `division_competition` | Boolean |  |
| `gamecast_available` | Boolean |  |
| `guid` | String |  |
| `highlights_available` | Boolean |  |
| `id` | Int64 |  |
| `lineup_available` | Boolean |  |
| `links` | String |  |
| `live_available` | Boolean |  |
| `necessary` | Boolean |  |
| `neutral_site` | Boolean |  |
| `notes` | String |  |
| `on_watch_espn` | Boolean |  |
| `pickcenter_available` | Boolean |  |
| `play_by_play_available` | Boolean |  |
| `possession_arrow_available` | Boolean |  |
| `preview_available` | Boolean |  |
| `recap_available` | Boolean |  |
| `recent` | Boolean |  |
| `series` | String |  |
| `shot_chart_available` | Boolean |  |
| `summary_available` | Boolean |  |
| `tickets_available` | Boolean |  |
| `time_valid` | Boolean |  |
| `timeouts_available` | Boolean |  |
| `uid` | String |  |
| `wallclock_available` | Boolean |  |
| `boxscore_source_description` | String |  |
| `boxscore_source_id` | Int64 |  |
| `boxscore_source_state` | String |  |
| `broadcasts_$ref` | String |  |
| `details_$ref` | String |  |
| `format_overtime_clock` | Float64 |  |
| `format_overtime_display_name` | String |  |
| `format_overtime_slug` | String |  |
| `format_regulation_clock` | Float64 |  |
| `format_regulation_display_name` | String |  |
| `format_regulation_periods` | Int64 |  |
| `format_regulation_slug` | String |  |
| `game_source_description` | String |  |
| `game_source_id` | Int64 |  |
| `game_source_state` | String |  |
| `linescore_source_description` | String |  |
| `linescore_source_id` | Int64 |  |
| `linescore_source_state` | String |  |
| `odds_$ref` | String |  |
| `officials_$ref` | String |  |
| `play_by_play_source_description` | String |  |
| `play_by_play_source_id` | Int64 |  |
| `play_by_play_source_state` | String |  |
| `power_indexes_$ref` | String |  |
| `predictor_$ref` | String |  |
| `probabilities_$ref` | String |  |
| `relevancy_$ref` | String |  |
| `situation_$ref` | String |  |
| `stats_source_description` | String |  |
| `stats_source_id` | Int64 |  |
| `stats_source_state` | String |  |
| `status_$ref` | String |  |
| `type_abbreviation` | String |  |
| `type_id` | Int64 |  |
| `type_slug` | String |  |
| `type_text` | String |  |
| `type_type` | String |  |
| `venue_$ref` | String |  |
| `venue_address_city` | String |  |
| `venue_address_state` | String |  |
| `venue_full_name` | String |  |
| `venue_grass` | Boolean |  |
| `venue_guid` | String |  |
| `venue_id` | Int64 |  |
| `venue_images` | String |  |
| `venue_indoor` | Boolean |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_game_competition-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_game_competition-example}

```python
espn_golf_game_competition(league='pga', event_id='401584793')
```

## espn_golf_game_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/{event_id}/competitions/{cid}/competitors`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/competitors](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/competitors)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_golf_game_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `home_away` | String |  |
| `id` | Int64 |  |
| `order` | Int64 |  |
| `type` | String |  |
| `uid` | String |  |
| `winner` | Boolean |  |
| `leaders_$ref` | String |  |
| `linescores_$ref` | String |  |
| `record_$ref` | String |  |
| `roster_$ref` | String |  |
| `score_$ref` | String |  |
| `statistics_$ref` | String |  |
| `team_$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_game_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_game_teams-example}

```python
espn_golf_game_teams(league='pga', event_id='401584793')
```

## espn_golf_game_team

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/{event_id}/competitions/{cid}/competitors/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/competitors/4](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/competitors/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_golf_game_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `home_away` | String |  |
| `id` | Int64 |  |
| `order` | Int64 |  |
| `type` | String |  |
| `uid` | String |  |
| `winner` | Boolean |  |
| `leaders_$ref` | String |  |
| `linescores_$ref` | String |  |
| `record_$ref` | String |  |
| `roster_$ref` | String |  |
| `score_$ref` | String |  |
| `statistics_$ref` | String |  |
| `team_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_game_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_game_team-example}

```python
espn_golf_game_team(league='pga', event_id='401584793', team_id='4')
```

## espn_golf_game_team_linescores

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/competitors/4/linescores](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/competitors/4/linescores)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_golf_game_team_linescores-returns}

**`return_parsed=True`** (default) — the output of `parse_event_competitor_linescores`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_game_team_linescores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_game_team_linescores-example}

```python
espn_golf_game_team_linescores(league='pga', event_id='401584793', team_id='4')
```

## espn_golf_game_team_statistics

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/competitors/4/statistics](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/competitors/4/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_golf_game_team_statistics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `split_name` | String | Name of the stat split (Core v2 answers one, "All Splits"). |
| `category_name` | String | ESPN stat category of the row (e.g., "defensive", "general", "offensive"). |
| `stat_name` | String | ESPN stat key in camelCase (e.g., "blocks", "defensiveRebounds"). |
| `stat_abbreviation` | String | Short label of the stat (e.g., "BLK"). |
| `stat_value` | Float64 | Numeric value of the team's stat in this game. |
| `stat_display_value` | String | The value as ESPN displays it (a string, e.g., "7"). |
| `stat_description` | String | ESPN's one-sentence definition of the stat. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_game_team_statistics-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_game_team_statistics-example}

```python
espn_golf_game_team_statistics(league='pga', event_id='401584793', team_id='4')
```

## espn_golf_game_status

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/{event_id}/competitions/{cid}/status`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/status](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/status)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_golf_game_status-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `clock` | Float64 |  |
| `display_clock` | String |  |
| `period` | Int64 |  |
| `type_completed` | Boolean |  |
| `type_description` | String |  |
| `type_detail` | String |  |
| `type_id` | Int64 |  |
| `type_name` | String |  |
| `type_short_detail` | String |  |
| `type_state` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_game_status-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_game_status-example}

```python
espn_golf_game_status(league='pga', event_id='401584793')
```

## espn_golf_game_broadcasts

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/{event_id}/competitions/{cid}/broadcasts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/broadcasts](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/broadcasts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_golf_game_broadcasts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `channel` | Int64 |  |
| `lang` | String |  |
| `partnered` | Boolean |  |
| `priority` | Int64 |  |
| `region` | String |  |
| `slug` | String |  |
| `station` | String |  |
| `competition_$ref` | String |  |
| `market_id` | Int64 |  |
| `market_type` | String |  |
| `media_$ref` | String |  |
| `media_call_letters` | String |  |
| `media_id` | Int64 |  |
| `media_logos` | String |  |
| `media_name` | String |  |
| `media_short_name` | String |  |
| `media_slug` | String |  |
| `type_id` | Int64 |  |
| `type_long_name` | String |  |
| `type_short_name` | String |  |
| `type_slug` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_game_broadcasts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_game_broadcasts-example}

```python
espn_golf_game_broadcasts(league='pga', event_id='401584793')
```

## espn_golf_game_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/{event_id}/competitions/{cid}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/leaders](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/401584793/competitions/401584793/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_golf_game_leaders-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nfl, mlb, nhl, wnba, cfb, mbb, wbb; 400 in nba with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_game_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_game_leaders-example}

```python
espn_golf_game_leaders(league='pga', event_id='401584793')
```

## espn_golf_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/venues?limit=1000](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/venues?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_golf_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_venues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_venues-example}

```python
espn_golf_venues(league='pga')
```

## espn_golf_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/venues/3663](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_golf_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `full_name` | String |  |
| `grass` | Boolean |  |
| `guid` | String |  |
| `id` | Int64 |  |
| `images` | String |  |
| `indoor` | Boolean |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_venue-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_venue-example}

```python
espn_golf_venue(league='pga', venue_id='3663')
```

## espn_golf_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/tournaments?limit=200](https://sports.core.api.espn.com/v2/sports/golf/leagues/pga/tournaments?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_golf_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_tournaments-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_tournaments-example}

```python
espn_golf_tournaments(league='pga')
```
