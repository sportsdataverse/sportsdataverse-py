---
title: Lacrosse — ESPN core API (v2)
sidebar_label: ESPN core API (v2)
description: "Lacrosse — ESPN core API (v2) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 21
toc_max_heading_level: 2
---
# Lacrosse — ESPN core API (v2)

`sportsdataverse.lacrosse` — 25 endpoints.

## espn_lacrosse_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_lacrosse_league_root-returns}

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

### Raises {#espn_lacrosse_league_root-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_league_root-example}

```python
espn_lacrosse_league_root(league='pll')
```

## espn_lacrosse_season_pointer

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/season`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/season](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_lacrosse_season_pointer-returns}

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

### Raises {#espn_lacrosse_season_pointer-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_season_pointer-example}

```python
espn_lacrosse_season_pointer(league='pll')
```

## espn_lacrosse_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons?limit=200](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_lacrosse_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_seasons-example}

```python
espn_lacrosse_seasons(league='pll')
```

## espn_lacrosse_season_info

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_lacrosse_season_info-returns}

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

### Raises {#espn_lacrosse_season_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_season_info-example}

```python
espn_lacrosse_season_info(league='pll', season=2026)
```

## espn_lacrosse_season_types

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/{season}/types`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/types](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/types)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_lacrosse_season_types-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_season_types-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_season_types-example}

```python
espn_lacrosse_season_types(league='pll', season=2026)
```

## espn_lacrosse_season_type

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/{season}/types/{season_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/types/2](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/types/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_lacrosse_season_type-returns}

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

### Raises {#espn_lacrosse_season_type-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_season_type-example}

```python
espn_lacrosse_season_type(league='pll', season=2026, season_type=2)
```

## espn_lacrosse_season_group

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/{season}/types/{season_type}/groups/{group_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/types/2/groups/118](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/types/2/groups/118)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `group_id` | `group_id` |  | `Y` |  | group_id path parameter. |

### Returns {#espn_lacrosse_season_group-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `id` | Int64 |  |
| `is_conference` | Boolean |  |
| `links` | String |  |
| `midsize_name` | String |  |
| `name` | String |  |
| `short_name` | String |  |
| `slug` | String |  |
| `uid` | String |  |
| `children_$ref` | String |  |
| `parent_$ref` | String |  |
| `season_$ref` | String |  |
| `standings_$ref` | String |  |
| `teams_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_season_group-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_season_group-example}

```python
espn_lacrosse_season_group(league='pll', season=2026, season_type=2, group_id=118)
```

## espn_lacrosse_season_groups

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/{season}/types/{season_type}/groups`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/types/2/groups](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/types/2/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_lacrosse_season_groups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_season_groups-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_season_groups-example}

```python
espn_lacrosse_season_groups(league='pll', season=2026, season_type=2)
```

## espn_lacrosse_season_group_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/{season}/types/{season_type}/groups/{group_id}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/types/2/groups/118/teams?limit=500](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/types/2/groups/118/teams?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `group_id` | `group_id` |  | `Y` |  | group_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_lacrosse_season_group_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_season_group_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_season_group_teams-example}

```python
espn_lacrosse_season_group_teams(league='pll', season=2026, season_type=2, group_id=118)
```

## espn_lacrosse_season_group_children

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/{season}/types/{season_type}/groups/{group_id}/children`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/mens-college-lacrosse/seasons/2026/types/2/groups/90/children?limit=500](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/mens-college-lacrosse/seasons/2026/types/2/groups/90/children?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `group_id` | `group_id` |  | `Y` |  | group_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_lacrosse_season_group_children-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_season_group_children-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_season_group_children-example}

```python
espn_lacrosse_season_group_children(league='mens-college-lacrosse', season=2026, season_type=2, group_id=90)
```

## espn_lacrosse_season_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/{season}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_lacrosse_season_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_season_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_season_teams-example}

```python
espn_lacrosse_season_teams(league='pll', season=2026)
```

## espn_lacrosse_season_team

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/{season}/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/teams/122896](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/2026/teams/122896)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_lacrosse_season_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String |  |
| `alternate_color` | String |  |
| `color` | String |  |
| `display_name` | String |  |
| `guid` | String |  |
| `id` | Int64 |  |
| `is_active` | Boolean |  |
| `is_all_star` | Boolean |  |
| `links` | String |  |
| `location` | String |  |
| `logos` | String |  |
| `name` | String |  |
| `short_display_name` | String |  |
| `slug` | String |  |
| `uid` | String |  |
| `against_the_spread_records_$ref` | String |  |
| `awards_$ref` | String |  |
| `coaches_$ref` | String |  |
| `depth_charts_$ref` | String |  |
| `events_$ref` | String |  |
| `franchise_$ref` | String |  |
| `groups_$ref` | String |  |
| `injuries_$ref` | String |  |
| `leaders_$ref` | String |  |
| `notes_$ref` | String |  |
| `ranks_$ref` | String |  |
| `record_$ref` | String |  |
| `statistics_$ref` | String |  |
| `transactions_$ref` | String |  |
| `venue_$ref` | String |  |
| `venue_address_city` | String |  |
| `venue_address_state` | String |  |
| `venue_full_name` | String |  |
| `venue_grass` | Boolean |  |
| `venue_guid` | String |  |
| `venue_id` | Int64 |  |
| `venue_images` | String |  |
| `venue_indoor` | Boolean |  |
| `venue_short_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_season_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_season_team-example}

```python
espn_lacrosse_season_team(league='pll', season=2026, team_id='122896')
```

## espn_lacrosse_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events?limit=500](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_lacrosse_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_games-example}

```python
espn_lacrosse_games(league='pll')
```

## espn_lacrosse_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_lacrosse_game-returns}

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

### Raises {#espn_lacrosse_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_game-example}

```python
espn_lacrosse_game(league='pll', event_id='401867221')
```

## espn_lacrosse_game_competition

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/{event_id}/competitions/{cid}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221/competitions/401867221](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221/competitions/401867221)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_lacrosse_game_competition-returns}

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

### Raises {#espn_lacrosse_game_competition-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_game_competition-example}

```python
espn_lacrosse_game_competition(league='pll', event_id='401867221')
```

## espn_lacrosse_game_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/{event_id}/competitions/{cid}/competitors`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221/competitions/401867221/competitors](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221/competitions/401867221/competitors)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_lacrosse_game_teams-returns}

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

### Raises {#espn_lacrosse_game_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_game_teams-example}

```python
espn_lacrosse_game_teams(league='pll', event_id='401867221')
```

## espn_lacrosse_game_team

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/{event_id}/competitions/{cid}/competitors/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221/competitions/401867221/competitors/122896](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221/competitions/401867221/competitors/122896)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_lacrosse_game_team-returns}

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

### Raises {#espn_lacrosse_game_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_game_team-example}

```python
espn_lacrosse_game_team(league='pll', event_id='401867221', team_id='122896')
```

## espn_lacrosse_game_team_linescores

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221/competitions/401867221/competitors/122896/linescores](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221/competitions/401867221/competitors/122896/linescores)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_lacrosse_game_team_linescores-returns}

**`return_parsed=True`** (default) — the output of `parse_event_competitor_linescores`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_game_team_linescores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_game_team_linescores-example}

```python
espn_lacrosse_game_team_linescores(league='pll', event_id='401867221', team_id='122896')
```

## espn_lacrosse_game_status

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/{event_id}/competitions/{cid}/status`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221/competitions/401867221/status](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221/competitions/401867221/status)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_lacrosse_game_status-returns}

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

### Raises {#espn_lacrosse_game_status-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_game_status-example}

```python
espn_lacrosse_game_status(league='pll', event_id='401867221')
```

## espn_lacrosse_game_broadcasts

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/{event_id}/competitions/{cid}/broadcasts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221/competitions/401867221/broadcasts](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/events/401867221/competitions/401867221/broadcasts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_lacrosse_game_broadcasts-returns}

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

### Raises {#espn_lacrosse_game_broadcasts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_game_broadcasts-example}

```python
espn_lacrosse_game_broadcasts(league='pll', event_id='401867221')
```

## espn_lacrosse_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_lacrosse_teams_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | String | Short team abbreviation (e.g. "BOS"). |
| `team_alternate_color` | String | Secondary team color as a hex string (no leading '#'). |
| `team_color` | String | Primary team color as a hex string (no leading '#'). |
| `team_display_name` | String | Full team display name (location + nickname). |
| `team_id` | Int64 | ESPN team id (stable join key across ESPN endpoints). |
| `team_is_active` | Boolean | Whether the team is currently active. |
| `team_is_all_star` | Boolean | Whether the entry is an all-star squad rather than a franchise. |
| `team_location` | String | Team location / city (e.g. "Boston"). |
| `team_logos` | String | Pipe-delimited logo image URLs. |
| `team_name` | String | Team nickname/mascot (e.g. "Celtics"). |
| `team_nickname` | String | Team nickname as ESPN labels it (often equals team_name). |
| `team_short_display_name` | String | Abbreviated display name for compact UIs. |
| `team_slug` | String | URL slug used in ESPN web paths. |
| `team_uid` | String | ESPN global UID (encodes sport/league/team). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_teams_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_teams_core-example}

```python
espn_lacrosse_teams_core(league='pll')
```

## espn_lacrosse_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/teams/122896](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/teams/122896)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_lacrosse_team_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String |  |
| `alternate_color` | String |  |
| `color` | String |  |
| `display_name` | String |  |
| `guid` | String |  |
| `id` | Int64 |  |
| `is_active` | Boolean |  |
| `is_all_star` | Boolean |  |
| `links` | String |  |
| `location` | String |  |
| `logos` | String |  |
| `name` | String |  |
| `short_display_name` | String |  |
| `slug` | String |  |
| `uid` | String |  |
| `against_the_spread_records_$ref` | String |  |
| `athletes_$ref` | String |  |
| `awards_$ref` | String |  |
| `coaches_$ref` | String |  |
| `depth_charts_$ref` | String |  |
| `events_$ref` | String |  |
| `franchise_$ref` | String |  |
| `groups_$ref` | String |  |
| `injuries_$ref` | String |  |
| `notes_$ref` | String |  |
| `odds_records_$ref` | String |  |
| `ranks_$ref` | String |  |
| `record_$ref` | String |  |
| `transactions_$ref` | String |  |
| `venue_$ref` | String |  |
| `venue_address_city` | String |  |
| `venue_address_state` | String |  |
| `venue_full_name` | String |  |
| `venue_grass` | Boolean |  |
| `venue_guid` | String |  |
| `venue_id` | Int64 |  |
| `venue_images` | String |  |
| `venue_indoor` | Boolean |  |
| `venue_short_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_team_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_team_core-example}

```python
espn_lacrosse_team_core(league='pll', team_id='122896')
```

## espn_lacrosse_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/franchises?limit=200](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/franchises?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_lacrosse_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_franchises-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_franchises-example}

```python
espn_lacrosse_franchises(league='pll')
```

## espn_lacrosse_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/franchises/122896](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/franchises/122896)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_lacrosse_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String |  |
| `color` | String |  |
| `display_name` | String |  |
| `id` | Int64 |  |
| `is_active` | Boolean |  |
| `location` | String |  |
| `name` | String |  |
| `short_display_name` | String |  |
| `slug` | String |  |
| `uid` | String |  |
| `team_$ref` | String |  |
| `venue_$ref` | String |  |
| `venue_address_city` | String |  |
| `venue_address_state` | String |  |
| `venue_full_name` | String |  |
| `venue_grass` | Boolean |  |
| `venue_guid` | String |  |
| `venue_id` | Int64 |  |
| `venue_images` | String |  |
| `venue_indoor` | Boolean |  |
| `venue_short_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_franchise-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_franchise-example}

```python
espn_lacrosse_franchise(league='pll', franchise_id='122896')
```

## espn_lacrosse_season_week_rankings

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/pll/seasons/{season}/types/{season_type}/weeks/{week}/rankings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/mens-college-lacrosse/seasons/2026/types/2/weeks/1/rankings](https://sports.core.api.espn.com/v2/sports/lacrosse/leagues/mens-college-lacrosse/seasons/2026/types/2/weeks/1/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `week` | `week` |  | `Y` |  | week path parameter. |

### Returns {#espn_lacrosse_season_week_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_lacrosse_season_week_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_lacrosse_season_week_rankings-example}

```python
espn_lacrosse_season_week_rankings(league='mens-college-lacrosse', season=2026, season_type=2, week=1)
```
