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
| `$ref` | character |  |
| `abbreviation` | character | Short abbreviation. |
| `color` | character | Primary color (hex, no leading '#'). |
| `display_name` | character | Display name. |
| `gender` | character | Player gender. |
| `guid` | character | Stable cross-league team GUID. |
| `id` | character | Id. |
| `is_tournament` | logical |  |
| `links` | character |  |
| `logos` | character | Logos. |
| `name` | character | Display name. |
| `short_name` | character | Short display name. |
| `slug` | character | URL-safe identifier. |
| `uid` | character | ESPN UID string. |
| `athletes_$ref` | character |  |
| `awards_$ref` | character |  |
| `calendar_$ref` | character |  |
| `draft_$ref` | character |  |
| `events_$ref` | character |  |
| `franchises_$ref` | character |  |
| `group_$ref` | character |  |
| `groups_$ref` | character |  |
| `leaders_$ref` | character |  |
| `notes_$ref` | character |  |
| `rankings_$ref` | character |  |
| `season_$ref` | character |  |
| `season_athletes_$ref` | character |  |
| `season_coaches_$ref` | character |  |
| `season_display_name` | character |  |
| `season_end_date` | character | Season end date. |
| `season_futures_$ref` | character |  |
| `season_power_index_leaders_$ref` | character |  |
| `season_power_indexes_$ref` | character |  |
| `season_rankings_$ref` | character |  |
| `season_start_date` | character | Season start date. |
| `season_type_$ref` | character |  |
| `season_type_abbreviation` | character |  |
| `season_type_corrections_$ref` | character |  |
| `season_type_end_date` | character |  |
| `season_type_groups_$ref` | character |  |
| `season_type_has_groups` | logical |  |
| `season_type_has_legs` | logical |  |
| `season_type_has_standings` | logical |  |
| `season_type_id` | character |  |
| `season_type_name` | character |  |
| `season_type_slug` | character |  |
| `season_type_start_date` | character |  |
| `season_type_type` | integer |  |
| `season_type_weeks_$ref` | character |  |
| `season_type_year` | integer |  |
| `season_types_$ref` | character |  |
| `season_types_count` | integer |  |
| `season_types_items` | character |  |
| `season_types_page_count` | integer |  |
| `season_types_page_index` | integer |  |
| `season_types_page_size` | integer |  |
| `season_year` | integer |  |
| `seasons_$ref` | character |  |
| `teams_$ref` | character |  |
| `transactions_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_league_root-example}

```python
espn_mlb_league_root()
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_seasons-example}

```python
espn_mlb_seasons()
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_games-example}

```python
espn_mlb_games()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `competitions` | character |  |
| `date` | character | Date in YYYY-MM-DD format. |
| `id` | character | Id. |
| `links` | character |  |
| `name` | character | Display name. |
| `short_name` | character | Short display name. |
| `time_valid` | logical |  |
| `uid` | character | ESPN UID string. |
| `venues` | character |  |
| `league_$ref` | character |  |
| `season_$ref` | character |  |
| `season_type_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game-example}

```python
espn_mlb_game(event_id='401584793')
```

_Last validated n/a._

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
| `team_abbreviation` | character | Short team abbreviation (e.g. "BOS"). |
| `team_alternate_color` | character | Secondary team color as a hex string (no leading '#'). |
| `team_color` | character | Primary team color as a hex string (no leading '#'). |
| `team_display_name` | character | Full team display name (location + nickname). |
| `team_id` | character | ESPN team id (stable join key across ESPN endpoints). |
| `team_is_active` | logical | Whether the team is currently active. |
| `team_is_all_star` | logical | Whether the entry is an all-star squad rather than a franchise. |
| `team_location` | character | Team location / city (e.g. "Boston"). |
| `team_logos` | character | Pipe-delimited logo image URLs. |
| `team_name` | character | Team nickname/mascot (e.g. "Celtics"). |
| `team_nickname` | character | Team nickname as ESPN labels it (often equals team_name). |
| `team_short_display_name` | character | Abbreviated display name for compact UIs. |
| `team_slug` | character | URL slug used in ESPN web paths. |
| `team_uid` | character | ESPN global UID (encodes sport/league/team). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_teams_core-example}

```python
espn_mlb_teams_core()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `abbreviation` | character | Short abbreviation. |
| `alternate_color` | character | Alternate color (hex without leading '#'). |
| `color` | character | Primary color (hex, no leading '#'). |
| `display_name` | character | Display name. |
| `guid` | character | Stable cross-league team GUID. |
| `id` | character | Id. |
| `is_active` | logical | Whether the team was active in this season. |
| `is_all_star` | logical | Is all star. |
| `links` | character |  |
| `location` | character | Team city/region (e.g. "Los Angeles"). |
| `logos` | character | Logos. |
| `name` | character | Display name. |
| `short_display_name` | character | Short display name. |
| `slug` | character | URL-safe identifier. |
| `uid` | character | ESPN UID string. |
| `against_the_spread_records_$ref` | character |  |
| `athletes_$ref` | character |  |
| `awards_$ref` | character |  |
| `coaches_$ref` | character |  |
| `depth_charts_$ref` | character |  |
| `events_$ref` | character |  |
| `franchise_$ref` | character |  |
| `groups_$ref` | character |  |
| `injuries_$ref` | character |  |
| `notes_$ref` | character |  |
| `odds_records_$ref` | character |  |
| `ranks_$ref` | character |  |
| `record_$ref` | character |  |
| `transactions_$ref` | character |  |
| `venue_$ref` | character |  |
| `venue_address_city` | character |  |
| `venue_address_state` | character |  |
| `venue_full_name` | character |  |
| `venue_grass` | logical |  |
| `venue_guid` | character |  |
| `venue_id` | character | MLBAM venue ID. |
| `venue_images` | character |  |
| `venue_indoor` | logical |  |
| `venue_short_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_team_core-example}

```python
espn_mlb_team_core(team_id='4')
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_venues-example}

```python
espn_mlb_venues()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `full_name` | character | Player's full name. |
| `grass` | logical |  |
| `guid` | character | Stable cross-league team GUID. |
| `id` | character | Id. |
| `images` | character |  |
| `indoor` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_venue-example}

```python
espn_mlb_venue(venue_id='3663')
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_franchises-example}

```python
espn_mlb_franchises()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `abbreviation` | character | Short abbreviation. |
| `color` | character | Primary color (hex, no leading '#'). |
| `display_name` | character | Display name. |
| `id` | character | Id. |
| `is_active` | logical | Whether the team was active in this season. |
| `location` | character | Team city/region (e.g. "Los Angeles"). |
| `name` | character | Display name. |
| `short_display_name` | character | Short display name. |
| `slug` | character | URL-safe identifier. |
| `uid` | character | ESPN UID string. |
| `team_$ref` | character |  |
| `venue_$ref` | character |  |
| `venue_address_city` | character |  |
| `venue_address_state` | character |  |
| `venue_full_name` | character |  |
| `venue_grass` | logical |  |
| `venue_guid` | character |  |
| `venue_id` | character | MLBAM venue ID. |
| `venue_images` | character |  |
| `venue_indoor` | logical |  |
| `venue_short_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_franchise-example}

```python
espn_mlb_franchise(franchise_id='2')
```

_Last validated n/a._

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
| `$ref` | character |  |
| `career_records` | character |  |
| `coach_seasons` | character |  |
| `experience` | integer | Years of professional experience. |
| `first_name` | character | Player first name. |
| `id` | character | Id. |
| `last_name` | character | Player last name. |
| `uid` | character | ESPN UID string. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_coach-example}

```python
espn_mlb_coach(coach_id='1')
```

_Last validated n/a._

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
| `$ref` | character |  |
| `display_value` | character | Human-readable value. |
| `id` | character | Id. |
| `name` | character | Display name. |
| `stats` | character |  |
| `summary` | character | Record summary (e.g. 'W-L'). |
| `type` | character | Record type / category. |
| `value` | double | Numeric value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_coach_record-example}

```python
espn_mlb_coach_record(coach_id='1')
```

_Last validated n/a._

## espn_mlb_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_mlb_coach_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_coach_season-example}

```python
espn_mlb_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_positions-example}

```python
espn_mlb_positions()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `abbreviation` | character | Short abbreviation. |
| `display_name` | character | Display name. |
| `id` | character | Id. |
| `leaf` | logical | TRUE if a leaf position. |
| `name` | character | Display name. |
| `parent_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_position-example}

```python
espn_mlb_position(position_id='1')
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_tournaments-example}

```python
espn_mlb_tournaments()
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_awards-example}

```python
espn_mlb_awards()
```

_Last validated n/a._

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
| `count` | integer | Count of count. |
| `items` | character |  |
| `page_count` | integer |  |
| `page_index` | integer |  |
| `page_size` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_award-example}

```python
espn_mlb_award(award_id='1')
```

_Last validated n/a._

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
| `group_name` | character | Group name. |
| `group_abbreviation` | character | Group abbreviation. |
| `team_id` | character | Team id. |
| `team_name` | character | Team name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_location` | character | Team location. |
| `team_logo` | character | Team logo. |
| `ot_losses` | double |  |
| `ot_wins` | double |  |
| `avg_points_against` | double | Avg points against. |
| `avg_points_for` | double | Avg points for. |
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `division_win_percent` | double | Division win percent. |
| `games_behind` | double | Games behind. |
| `games_played` | double | Matches played. |
| `league_win_percent` | double | League win percent. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points` | double | Points. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `ties` | double | Number of matches the team has drawn. |
| `win_percent` | double | Win percent. |
| `wins` | double | Wins. |
| `division_games_behind` | double | Number of games the team trails the division leader in the standings, expressed as a decimal (e.g., 0.5 for half a game back). |
| `division_percent` | double | The team's winning percentage in division games, calculated as division wins divided by total division games played. |
| `division_tied` | double | Number of games the team has tied against opponents within their own division. |
| `home_losses` | double |  |
| `home_ties` | double |  |
| `home_wins` | double |  |
| `magic_number_division` | double | Combination of wins needed by the team (or losses needed by the division leader) for the team to clinch a division title. |
| `magic_number_wildcard` | double | Combination of wins needed by the team (or losses needed by the next wildcard team) for the team to clinch a wildcard playoff berth. |
| `playoff_percent` | double | Estimated or model-derived probability that the team will qualify for the playoffs, expressed as a decimal between 0 and 1. |
| `road_losses` | double |  |
| `road_ties` | double |  |
| `road_wins` | double |  |
| `wild_card_percent` | double | The team's winning percentage in games that count toward wildcard standings positioning. |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `intradivision` | character | Intradivision. |
| `intraleague` | character | Intraleague. |
| `last ten games` | character | Last ten games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_standings_core-example}

```python
espn_mlb_standings_core()
```

_Last validated n/a._

## espn_mlb_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/leaders](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_leaders_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_leaders_core-example}

```python
espn_mlb_leaders_core()
```

_Last validated n/a._

## espn_mlb_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/notes](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_league_notes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_league_notes-example}

```python
espn_mlb_league_notes()
```

_Last validated n/a._

## espn_mlb_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/talentpicks](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_talentpicks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_talentpicks-example}

```python
espn_mlb_talentpicks()
```

_Last validated n/a._
