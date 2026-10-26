---
title: "NBA — ESPN core API (v2) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "NBA — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — ESPN core API (v2) — Other

## espn_nba_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nba_league_root-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String | Short abbreviation. |
| `color` | String | Primary color (hex without leading '#'). |
| `display_name` | String | Display name. |
| `gender` | String |  |
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
| `season_display_name` | String | Season display label. |
| `season_end_date` | String | Date in YYYY-MM-DD format. |
| `season_futures_$ref` | String |  |
| `season_power_index_leaders_$ref` | String |  |
| `season_power_indexes_$ref` | String |  |
| `season_rankings_$ref` | String |  |
| `season_start_date` | String | Date in YYYY-MM-DD format. |
| `season_type_$ref` | String |  |
| `season_type_abbreviation` | String | Season type abbreviation. |
| `season_type_corrections_$ref` | String |  |
| `season_type_end_date` | String |  |
| `season_type_groups_$ref` | String |  |
| `season_type_has_groups` | Boolean |  |
| `season_type_has_legs` | Boolean |  |
| `season_type_has_standings` | Boolean |  |
| `season_type_id` | Int64 | Unique identifier for season type. |
| `season_type_name` | String | Season type name. |
| `season_type_slug` | String |  |
| `season_type_start_date` | String |  |
| `season_type_type` | Int64 | Season type type. |
| `season_type_weeks_$ref` | String |  |
| `season_type_year` | Int64 |  |
| `season_types_$ref` | String |  |
| `season_types_count` | Int64 |  |
| `season_types_items` | String |  |
| `season_types_page_count` | Int64 |  |
| `season_types_page_index` | Int64 |  |
| `season_types_page_size` | Int64 |  |
| `season_year` | Int64 | Season year string ('YYYY-YY' format). |
| `seasons_$ref` | String |  |
| `teams_$ref` | String |  |
| `transactions_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_league_root-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_league_root-example}

```python
espn_nba_league_root()
```

## espn_nba_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons?limit=200](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nba_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_seasons-example}

```python
espn_nba_seasons()
```

## espn_nba_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events?limit=500](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nba_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_games-example}

```python
espn_nba_games()
```

## espn_nba_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_nba_game-returns}

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
| `time_valid` | Boolean | Time valid. |
| `uid` | String | ESPN UID string. |
| `venues` | String |  |
| `league_$ref` | String |  |
| `season_$ref` | String |  |
| `season_type_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_game-example}

```python
espn_nba_game(event_id='401584793')
```

## espn_nba_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_nba_teams_core-returns}

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

### Raises {#espn_nba_teams_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_teams_core-example}

```python
espn_nba_teams_core()
```

## espn_nba_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/teams/4](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_nba_team_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String | Short abbreviation. |
| `alternate_color` | String | Alternate color (hex without leading '#'). |
| `color` | String | Primary color (hex without leading '#'). |
| `display_name` | String | Display name. |
| `guid` | String | Stable cross-league team GUID. |
| `id` | Int64 | Id. |
| `is_active` | Boolean | Is active. |
| `is_all_star` | Boolean | Is all star. |
| `links` | String |  |
| `location` | String | Location. |
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
| `venue_address_city` | String | Venue address city. |
| `venue_address_state` | String | Venue address state / region. |
| `venue_full_name` | String | Venue full name. |
| `venue_grass` | Boolean |  |
| `venue_guid` | String |  |
| `venue_id` | Int64 | Unique venue identifier. |
| `venue_images` | String |  |
| `venue_indoor` | Boolean | TRUE if the venue is indoors. |
| `venue_short_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_team_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_team_core-example}

```python
espn_nba_team_core(team_id='4')
```

## espn_nba_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/venues?limit=1000](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/venues?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nba_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_venues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_venues-example}

```python
espn_nba_venues()
```

## espn_nba_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/venues/3663](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_nba_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `full_name` | String | Player's full name. |
| `grass` | Boolean | Grass. |
| `guid` | String | Stable cross-league team GUID. |
| `id` | Int64 | Id. |
| `images` | String |  |
| `indoor` | Boolean | Indoor. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_venue-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_venue-example}

```python
espn_nba_venue(venue_id='3663')
```

## espn_nba_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/franchises?limit=200](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/franchises?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nba_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_franchises-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_franchises-example}

```python
espn_nba_franchises()
```

## espn_nba_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/franchises/2](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_nba_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String | Short abbreviation. |
| `color` | String | Primary color (hex without leading '#'). |
| `display_name` | String | Display name. |
| `id` | Int64 | Id. |
| `is_active` | Boolean | Is active. |
| `location` | String | Location. |
| `name` | String | Display name. |
| `short_display_name` | String | Short display name. |
| `slug` | String | URL-safe identifier. |
| `uid` | String | ESPN UID string. |
| `team_$ref` | String |  |
| `venue_$ref` | String |  |
| `venue_address_city` | String | Venue address city. |
| `venue_address_state` | String | Venue address state / region. |
| `venue_full_name` | String | Venue full name. |
| `venue_grass` | Boolean |  |
| `venue_guid` | String |  |
| `venue_id` | Int64 | Unique venue identifier. |
| `venue_images` | String |  |
| `venue_indoor` | Boolean | TRUE if the venue is indoors. |
| `venue_short_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_franchise-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_franchise-example}

```python
espn_nba_franchise(franchise_id='2')
```

## espn_nba_coach

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/coaches/1](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#espn_nba_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `career_records` | String |  |
| `coach_seasons` | String |  |
| `experience` | Int64 | Years of professional experience. |
| `first_name` | String | Player's first name. |
| `id` | Int64 | Id. |
| `last_name` | String | Player's last name. |
| `uid` | String | ESPN UID string. |
| `birth_place_city` | String | Birth place city. |
| `birth_place_state` | String | Birth place state. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_coach-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_coach-example}

```python
espn_nba_coach(coach_id='1')
```

## espn_nba_coach_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/coaches/1/record/0](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/coaches/1/record/0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `record_type` | `record_type` |  |  | `Y` | record_type path parameter. |

### Returns {#espn_nba_coach_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `display_value` | String | Display-formatted value. |
| `id` | Int64 | Id. |
| `name` | String | Display name. |
| `stats` | String | Stats. |
| `summary` | String | W-L summary (e.g. "50-32"). |
| `type` | String | Record type / category. |
| `value` | Float64 | Numeric or string value field. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_coach_record-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_coach_record-example}

```python
espn_nba_coach_record(coach_id='1')
```

## espn_nba_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_nba_coach_season-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_coach_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_coach_season-example}

```python
espn_nba_coach_season(coach_id='1', season=2024)
```

## espn_nba_positions

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/positions?limit=200](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/positions?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nba_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_positions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_positions-example}

```python
espn_nba_positions()
```

## espn_nba_position

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/positions/1](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  | position_id path parameter. |

### Returns {#espn_nba_position-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String | Short abbreviation. |
| `display_name` | String | Display name. |
| `id` | Int64 | Id. |
| `leaf` | Boolean |  |
| `name` | String | Display name. |
| `parent_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_position-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_position-example}

```python
espn_nba_position(position_id='1')
```

## espn_nba_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/tournaments?limit=200](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/tournaments?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nba_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_tournaments-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_tournaments-example}

```python
espn_nba_tournaments()
```

## espn_nba_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/awards?limit=200](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/awards?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nba_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_awards-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_awards-example}

```python
espn_nba_awards()
```

## espn_nba_award

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/awards/1](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |

### Returns {#espn_nba_award-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `count` | Int64 | Count of count. |
| `items` | String |  |
| `page_count` | Int64 |  |
| `page_index` | Int64 |  |
| `page_size` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_award-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_award-example}

```python
espn_nba_award(award_id='1')
```

## espn_nba_standings_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/standings](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nba_standings_core-returns}

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
| `avg_points_against` | Float64 | Avg points against. |
| `avg_points_for` | Float64 | Avg points for. |
| `clincher` | Float64 | Clincher. |
| `differential` | Float64 | Differential. |
| `division_win_percent` | Float64 | Division win percent. |
| `games_behind` | Float64 | Games behind. |
| `league_win_percent` | Float64 | League win percent. |
| `losses` | Float64 | Losses. |
| `playoff_seed` | Float64 | Playoff seed. |
| `point_differential` | Float64 | Point differential. |
| `points` | Float64 | Points. |
| `points_against` | Float64 | Points against. |
| `points_for` | Float64 | Points for. |
| `streak` | Float64 | Streak. |
| `win_percent` | Float64 | Win percent. |
| `wins` | Float64 | Wins. |
| `games_ahead` | Float64 | Games ahead. |
| `overall` | String | Overall. |
| `home` | String | Home. |
| `road` | String | Road. |
| `vs. div.` | String | Vs. div.. |
| `vs. conf.` | String | Vs. conf.. |
| `last ten games` | String | Last ten games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_standings_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_standings_core-example}

```python
espn_nba_standings_core()
```

## espn_nba_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/leaders](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nba_leaders_core-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, 2026-10-07); its rows sit under keys it does not read (top level: $ref, abbreviation, categories, id, name, type).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_leaders_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_leaders_core-example}

```python
espn_nba_leaders_core()
```

## espn_nba_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/notes](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nba_league_notes-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nba_league_notes-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_league_notes-example}

```python
espn_nba_league_notes()
```

## espn_nba_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/talentpicks](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nba_talentpicks-returns}

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

### Raises {#espn_nba_talentpicks-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nba_talentpicks-example}

```python
espn_nba_talentpicks()
```
