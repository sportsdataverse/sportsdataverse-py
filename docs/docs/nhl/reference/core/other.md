---
title: "NHL — ESPN core API (v2) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "NHL — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — ESPN core API (v2) — Other

## espn_nhl_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nhl_league_root-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String | Team abbreviation. |
| `color` | String | Primary color hex. |
| `display_name` | String | Player display name. |
| `gender` | String | League gender designation. |
| `guid` | String | Athlete global unique identifier. |
| `id` | Int64 | Unique player identifier. |
| `is_tournament` | Boolean | Whether the league is a tournament. |
| `links` | String |  |
| `logos` | String |  |
| `name` | String | Team mascot name. |
| `short_name` | String | Short game name. |
| `slug` | String | URL slug. |
| `uid` | String | Competitor uid string. |
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
| `season_type_id` | Int64 | Season type id (1=pre, 2=reg, 3=post, 4=off). |
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
| `season_year` | Int64 | Season end year. |
| `seasons_$ref` | String |  |
| `teams_$ref` | String |  |
| `transactions_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_league_root-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_league_root-example}

```python
espn_nhl_league_root()
```

## espn_nhl_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons?limit=200](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nhl_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_seasons-example}

```python
espn_nhl_seasons()
```

## espn_nhl_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events?limit=500](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nhl_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_games-example}

```python
espn_nhl_games()
```

## espn_nhl_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events/401584793](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_nhl_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `competitions` | String |  |
| `date` | String | Game date (ISO 8601 datetime string). |
| `id` | Int64 | Unique player identifier. |
| `links` | String |  |
| `name` | String | Team mascot name. |
| `short_name` | String | Short game name. |
| `time_valid` | Boolean | Whether the start time is confirmed. |
| `uid` | String | Competitor uid string. |
| `venues` | String |  |
| `league_$ref` | String |  |
| `season_$ref` | String |  |
| `season_type_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_game-example}

```python
espn_nhl_game(event_id='401584793')
```

## espn_nhl_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_nhl_teams_core-returns}

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

### Raises {#espn_nhl_teams_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_teams_core-example}

```python
espn_nhl_teams_core()
```

## espn_nhl_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/teams/4](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_nhl_team_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String | Team abbreviation. |
| `alternate_color` | String | Alternate color hex. |
| `color` | String | Primary color hex. |
| `display_name` | String | Player display name. |
| `guid` | String | Athlete global unique identifier. |
| `id` | Int64 | Unique player identifier. |
| `is_active` | Boolean | Whether the team is active. |
| `is_all_star` | Boolean | Whether the team is an all-star team. |
| `links` | String |  |
| `location` | String | Team city/location. |
| `logos` | String |  |
| `name` | String | Team mascot name. |
| `short_display_name` | String | Short display name. |
| `slug` | String | URL slug. |
| `uid` | String | Competitor uid string. |
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
| `venue_full_name` | String | Venue full name. |
| `venue_grass` | Boolean |  |
| `venue_guid` | String |  |
| `venue_id` | Int64 | Venue identifier. |
| `venue_images` | String |  |
| `venue_indoor` | Boolean | Whether the venue is indoors. |
| `venue_short_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_team_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_team_core-example}

```python
espn_nhl_team_core(team_id='4')
```

## espn_nhl_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/venues?limit=1000](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/venues?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nhl_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_venues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_venues-example}

```python
espn_nhl_venues()
```

## espn_nhl_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/venues/3663](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_nhl_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `full_name` | String | Player full name. |
| `grass` | Boolean | Whether the surface is grass. |
| `guid` | String | Athlete global unique identifier. |
| `id` | Int64 | Unique player identifier. |
| `images` | String |  |
| `indoor` | Boolean | Whether the venue is indoors. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_venue-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_venue-example}

```python
espn_nhl_venue(venue_id='3663')
```

## espn_nhl_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/franchises?limit=200](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/franchises?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nhl_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_franchises-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_franchises-example}

```python
espn_nhl_franchises()
```

## espn_nhl_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/franchises/2](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_nhl_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String | Team abbreviation. |
| `color` | String | Primary color hex. |
| `display_name` | String | Player display name. |
| `id` | Int64 | Unique player identifier. |
| `is_active` | Boolean | Whether the team is active. |
| `location` | String | Team city/location. |
| `name` | String | Team mascot name. |
| `short_display_name` | String | Short display name. |
| `slug` | String | URL slug. |
| `uid` | String | Competitor uid string. |
| `team_$ref` | String |  |
| `venue_$ref` | String |  |
| `venue_address_city` | String |  |
| `venue_address_state` | String |  |
| `venue_full_name` | String | Venue full name. |
| `venue_grass` | Boolean |  |
| `venue_guid` | String |  |
| `venue_id` | Int64 | Venue identifier. |
| `venue_images` | String |  |
| `venue_indoor` | Boolean | Whether the venue is indoors. |
| `venue_short_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_franchise-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_franchise-example}

```python
espn_nhl_franchise(franchise_id='2')
```

## espn_nhl_coach

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/coaches/1](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#espn_nhl_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `career_records` | String |  |
| `coach_seasons` | String |  |
| `experience` | Int64 | Years of coaching experience (if returned). |
| `first_name` | String | Player first name. |
| `id` | Int64 | Unique player identifier. |
| `last_name` | String | Player last name. |
| `uid` | String | Competitor uid string. |
| `birth_place_city` | String | Birth place city. |
| `birth_place_state` | String | Birth place state. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_coach-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_coach-example}

```python
espn_nhl_coach(coach_id='1')
```

## espn_nhl_coach_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/coaches/1/record/0](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/coaches/1/record/0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `record_type` | `record_type` |  |  | `Y` | record_type path parameter. |

### Returns {#espn_nhl_coach_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `display_value` | String | Leader stat display value. |
| `id` | Int64 | Unique player identifier. |
| `name` | String | Team mascot name. |
| `stats` | String |  |
| `summary` | String | Record summary string (e.g. "25-15-10"). |
| `type` | String | Competitor type (e.g. "team"). |
| `value` | Float64 | Leader stat numeric value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_coach_record-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_coach_record-example}

```python
espn_nhl_coach_record(coach_id='1')
```

## espn_nhl_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_nhl_coach_season-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_coach_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_coach_season-example}

```python
espn_nhl_coach_season(coach_id='1', season=2024)
```

## espn_nhl_positions

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/positions?limit=200](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/positions?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nhl_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_positions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_positions-example}

```python
espn_nhl_positions()
```

## espn_nhl_position

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/positions/1](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  | position_id path parameter. |

### Returns {#espn_nhl_position-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `abbreviation` | String | Team abbreviation. |
| `display_name` | String | Player display name. |
| `id` | Int64 | Unique player identifier. |
| `leaf` | Boolean | Whether this is a leaf (non-parent) position. |
| `name` | String | Team mascot name. |
| `parent_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_position-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_position-example}

```python
espn_nhl_position(position_id='1')
```

## espn_nhl_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/tournaments?limit=200](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/tournaments?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nhl_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_tournaments-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_tournaments-example}

```python
espn_nhl_tournaments()
```

## espn_nhl_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/awards?limit=200](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/awards?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nhl_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_awards-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_awards-example}

```python
espn_nhl_awards()
```

## espn_nhl_award

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/awards/1](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |

### Returns {#espn_nhl_award-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `count` | Int64 | Total athlete count matching the filter. |
| `items` | String |  |
| `page_count` | Int64 | Total pages in the collection. |
| `page_index` | Int64 |  |
| `page_size` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_award-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_award-example}

```python
espn_nhl_award(award_id='1')
```

## espn_nhl_standings_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/standings](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nhl_standings_core-returns}

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
| `ot_losses` | Float64 | Overtime losses. |
| `clincher` | Float64 | Clincher. |
| `differential` | Float64 | Differential. |
| `games_behind` | Float64 | Games behind. |
| `games_played` | Float64 | Matches played. |
| `losses` | Float64 | Losses. |
| `playoff_seed` | Float64 | Playoff seed. |
| `point_differential` | Float64 | Point differential. |
| `points` | Float64 | Points. |
| `points_against` | Float64 | Points against. |
| `points_for` | Float64 | Points for. |
| `streak` | Float64 | Streak. |
| `wins` | Float64 | Wins. |
| `overtime_losses` | Float64 | Total overtime losses. |
| `overtime_wins` | Float64 | Overtime wins. |
| `points_diff` | Float64 | Difference between total points scored for and against the team across all games played. |
| `reg_losses` | Float64 | Number of losses the team has suffered in regulation time (excluding overtime and shootout losses). |
| `reg_wins` | Float64 | Number of wins the team has earned in regulation time (excluding overtime and shootout wins). |
| `rot_losses` | Float64 | Number of losses the team has suffered in overtime or the shootout (non-regulation losses). |
| `rot_wins` | Float64 | Number of wins the team has earned in overtime or the shootout (non-regulation wins). |
| `shootout_losses` | Float64 | Shootout losses. |
| `shootout_wins` | Float64 | Shootout wins. |
| `overall` | String | Overall. |
| `home` | String | Home. |
| `road` | String | Road. |
| `last ten games` | String | Last ten games. |
| `vs. div.` | String | Vs. div.. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_standings_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_standings_core-example}

```python
espn_nhl_standings_core()
```

## espn_nhl_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/leaders](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nhl_leaders_core-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, 2026-10-07); its rows sit under keys it does not read (top level: $ref, abbreviation, categories, id, name, type).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_leaders_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_leaders_core-example}

```python
espn_nhl_leaders_core()
```

## espn_nhl_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/notes](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nhl_league_notes-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_league_notes-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_league_notes-example}

```python
espn_nhl_league_notes()
```

## espn_nhl_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/talentpicks](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nhl_talentpicks-returns}

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

### Raises {#espn_nhl_talentpicks-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_talentpicks-example}

```python
espn_nhl_talentpicks()
```
