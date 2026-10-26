---
title: "MLB — ESPN core API (v2) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "MLB — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — ESPN core API (v2) — Other

## espn_mlb_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_league_root-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String | Short abbreviation. |
| `color` | String | Primary color (hex, no leading '#'). |
| `display_name` | String | Display name. |
| `gender` | String | Player gender. |
| `guid` | String | Stable cross-league team GUID. |
| `id` | Int64 | Id. |
| `is_tournament` | Boolean |  |
| `links` | String |  |
| `logos` | String | Logos. |
| `name` | String | Display name. |
| `short_name` | String | Short display name. |
| `slug` | String | URL-safe identifier. |
| `uid` | String | ESPN UID string. |
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
| `season_end_date` | String | Season end date. |
| `season_futures_$ref` | String |  |
| `season_power_index_leaders_$ref` | String |  |
| `season_power_indexes_$ref` | String |  |
| `season_rankings_$ref` | String |  |
| `season_start_date` | String | Season start date. |
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

### Raises {#espn_mlb_league_root-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_league_root-example}

```python
espn_mlb_league_root()
```

## espn_mlb_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/seasons?limit=200](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/seasons?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_seasons-example}

```python
espn_mlb_seasons()
```

## espn_mlb_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events?limit=500](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_games-example}

```python
espn_mlb_games()
```

## espn_mlb_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_mlb_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `competitions` | String |  |
| `date` | String | Date in YYYY-MM-DD format. |
| `id` | Int64 | Id. |
| `links` | String |  |
| `name` | String | Display name. |
| `short_name` | String | Short display name. |
| `time_valid` | Boolean |  |
| `uid` | String | ESPN UID string. |
| `venues` | String |  |
| `league_$ref` | String |  |
| `season_$ref` | String |  |
| `season_type_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game-example}

```python
espn_mlb_game(event_id='401584793')
```

## espn_mlb_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_mlb_teams_core-returns}

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

### Raises {#espn_mlb_teams_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_teams_core-example}

```python
espn_mlb_teams_core()
```

## espn_mlb_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/teams/4](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_mlb_team_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String | Short abbreviation. |
| `alternate_color` | String | Alternate color (hex without leading '#'). |
| `color` | String | Primary color (hex, no leading '#'). |
| `display_name` | String | Display name. |
| `guid` | String | Stable cross-league team GUID. |
| `id` | Int64 | Id. |
| `is_active` | Boolean | Whether the team was active in this season. |
| `is_all_star` | Boolean | Is all star. |
| `links` | String |  |
| `location` | String | Team city/region (e.g. "Los Angeles"). |
| `logos` | String | Logos. |
| `name` | String | Display name. |
| `short_display_name` | String | Short display name. |
| `slug` | String | URL-safe identifier. |
| `uid` | String | ESPN UID string. |
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
| `venue_id` | Int64 | MLBAM venue ID. |
| `venue_images` | String |  |
| `venue_indoor` | Boolean |  |
| `venue_short_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_team_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_team_core-example}

```python
espn_mlb_team_core(team_id='4')
```

## espn_mlb_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/venues?limit=1000](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/venues?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_venues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_venues-example}

```python
espn_mlb_venues()
```

## espn_mlb_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/venues/3663](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_mlb_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `full_name` | String | Player's full name. |
| `grass` | Boolean |  |
| `guid` | String | Stable cross-league team GUID. |
| `id` | Int64 | Id. |
| `images` | String |  |
| `indoor` | Boolean |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_venue-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_venue-example}

```python
espn_mlb_venue(venue_id='3663')
```

## espn_mlb_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/franchises?limit=200](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/franchises?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_franchises-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_franchises-example}

```python
espn_mlb_franchises()
```

## espn_mlb_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/franchises/2](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_mlb_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String | Short abbreviation. |
| `color` | String | Primary color (hex, no leading '#'). |
| `display_name` | String | Display name. |
| `id` | Int64 | Id. |
| `is_active` | Boolean | Whether the team was active in this season. |
| `location` | String | Team city/region (e.g. "Los Angeles"). |
| `name` | String | Display name. |
| `short_display_name` | String | Short display name. |
| `slug` | String | URL-safe identifier. |
| `uid` | String | ESPN UID string. |
| `team_$ref` | String |  |
| `venue_$ref` | String |  |
| `venue_address_city` | String |  |
| `venue_address_state` | String |  |
| `venue_full_name` | String |  |
| `venue_grass` | Boolean |  |
| `venue_guid` | String |  |
| `venue_id` | Int64 | MLBAM venue ID. |
| `venue_images` | String |  |
| `venue_indoor` | Boolean |  |
| `venue_short_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_franchise-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_franchise-example}

```python
espn_mlb_franchise(franchise_id='2')
```

## espn_mlb_coach

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#espn_mlb_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `career_records` | String |  |
| `coach_seasons` | String |  |
| `experience` | Int64 | Years of professional experience. |
| `first_name` | String | Player first name. |
| `id` | Int64 | Id. |
| `last_name` | String | Player last name. |
| `uid` | String | ESPN UID string. |
| `birth_place_city` | String | Birth place city. |
| `birth_place_state` | String | Birth place state. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_coach-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_coach-example}

```python
espn_mlb_coach(coach_id='1')
```

## espn_mlb_coach_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1/record/0](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1/record/0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `record_type` | `record_type` |  |  | `Y` | record_type path parameter. |

### Returns {#espn_mlb_coach_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `display_value` | String | Human-readable value. |
| `id` | Int64 | Id. |
| `name` | String | Display name. |
| `stats` | String |  |
| `summary` | String | Record summary (e.g. 'W-L'). |
| `type` | String | Record type / category. |
| `value` | Float64 | Numeric value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_coach_record-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_coach_record-example}

```python
espn_mlb_coach_record(coach_id='1')
```

## espn_mlb_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_mlb_coach_season-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_coach_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_coach_season-example}

```python
espn_mlb_coach_season(coach_id='1', season=2024)
```

## espn_mlb_positions

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/positions?limit=200](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/positions?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_positions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_positions-example}

```python
espn_mlb_positions()
```

## espn_mlb_position

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/positions/1](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  | position_id path parameter. |

### Returns {#espn_mlb_position-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String | Short abbreviation. |
| `display_name` | String | Display name. |
| `id` | Int64 | Id. |
| `leaf` | Boolean | TRUE if a leaf position. |
| `name` | String | Display name. |
| `parent_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_position-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_position-example}

```python
espn_mlb_position(position_id='1')
```

## espn_mlb_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/tournaments?limit=200](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/tournaments?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_tournaments-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_tournaments-example}

```python
espn_mlb_tournaments()
```

## espn_mlb_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/awards?limit=200](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/awards?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_awards-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_awards-example}

```python
espn_mlb_awards()
```

## espn_mlb_award

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/awards/1](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |

### Returns {#espn_mlb_award-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `count` | Int64 | Count of count. |
| `items` | String |  |
| `page_count` | Int64 |  |
| `page_index` | Int64 |  |
| `page_size` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_award-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_award-example}

```python
espn_mlb_award(award_id='1')
```

## espn_mlb_standings_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/standings](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_standings_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_name` | String | Group name. |
| `group_abbreviation` | String | Group abbreviation. |
| `team_id` | Int64 | Team id. |
| `team_name` | String | Team name. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_display_name` | String | Team display name. |
| `team_location` | String | Team location. |
| `team_logo` | String | Team logo. |
| `ot_losses` | Float64 |  |
| `ot_wins` | Float64 |  |
| `avg_points_against` | Float64 | Avg points against. |
| `avg_points_for` | Float64 | Avg points for. |
| `clincher` | Float64 | Clincher. |
| `differential` | Float64 | Differential. |
| `division_win_percent` | Float64 | Division win percent. |
| `games_behind` | Float64 | Games behind. |
| `games_played` | Float64 | Matches played. |
| `league_win_percent` | Float64 | League win percent. |
| `losses` | Float64 | Losses. |
| `playoff_seed` | Float64 | Playoff seed. |
| `point_differential` | Float64 | Point differential. |
| `points` | Float64 | Points. |
| `points_against` | Float64 | Points against. |
| `points_for` | Float64 | Points for. |
| `streak` | Float64 | Streak. |
| `ties` | Float64 | Number of matches the team has drawn. |
| `win_percent` | Float64 | Win percent. |
| `wins` | Float64 | Wins. |
| `division_games_behind` | Float64 | Number of games the team trails the division leader in the standings, expressed as a decimal (e.g., 0.5 for half a game back). |
| `division_percent` | Float64 | The team's winning percentage in division games, calculated as division wins divided by total division games played. |
| `division_tied` | Float64 | Number of games the team has tied against opponents within their own division. |
| `home_losses` | Float64 |  |
| `home_ties` | Float64 |  |
| `home_wins` | Float64 |  |
| `magic_number_division` | Float64 | Combination of wins needed by the team (or losses needed by the division leader) for the team to clinch a division title. |
| `magic_number_wildcard` | Float64 | Combination of wins needed by the team (or losses needed by the next wildcard team) for the team to clinch a wildcard playoff berth. |
| `playoff_percent` | Float64 | Estimated or model-derived probability that the team will qualify for the playoffs, expressed as a decimal between 0 and 1. |
| `road_losses` | Float64 |  |
| `road_ties` | Float64 |  |
| `road_wins` | Float64 |  |
| `wild_card_percent` | Float64 | The team's winning percentage in games that count toward wildcard standings positioning. |
| `overall` | String | Overall. |
| `home` | String | Home. |
| `road` | String | Road. |
| `intradivision` | String | Intradivision. |
| `intraleague` | String | Intraleague. |
| `last ten games` | String | Last ten games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_standings_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_standings_core-example}

```python
espn_mlb_standings_core()
```

## espn_mlb_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/leaders](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_leaders_core-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, 2026-10-07); its rows sit under keys it does not read (top level: $ref, abbreviation, categories, id, name, type).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_leaders_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_leaders_core-example}

```python
espn_mlb_leaders_core()
```

## espn_mlb_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/notes](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_league_notes-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_league_notes-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_league_notes-example}

```python
espn_mlb_league_notes()
```

## espn_mlb_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/talentpicks](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_talentpicks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `pick_competition_$ref` | String |  |
| `pick_competitor_$ref` | String |  |
| `pick_correct` | Boolean |  |
| `pick_person_display_name` | String |  |
| `pick_person_first_name` | String |  |
| `pick_person_headshot_alt` | String |  |
| `pick_person_headshot_href` | String |  |
| `pick_person_id` | Int64 |  |
| `pick_person_last_name` | String |  |
| `week_record_display_value` | String |  |
| `week_record_season_$ref` | String |  |
| `week_record_stats` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_talentpicks-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_talentpicks-example}

```python
espn_mlb_talentpicks()
```
