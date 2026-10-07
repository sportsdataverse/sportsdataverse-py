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
| `$ref` | character |  |
| `abbreviation` | character | Short abbreviation. |
| `color` | character | Primary color (hex without leading '#'). |
| `display_name` | character | Display name. |
| `gender` | character |  |
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
| `season_display_name` | character | Season display label. |
| `season_end_date` | character | Date in YYYY-MM-DD format. |
| `season_futures_$ref` | character |  |
| `season_power_index_leaders_$ref` | character |  |
| `season_power_indexes_$ref` | character |  |
| `season_rankings_$ref` | character |  |
| `season_start_date` | character | Date in YYYY-MM-DD format. |
| `season_type_$ref` | character |  |
| `season_type_abbreviation` | character | Season type abbreviation. |
| `season_type_corrections_$ref` | character |  |
| `season_type_end_date` | character |  |
| `season_type_groups_$ref` | character |  |
| `season_type_has_groups` | logical |  |
| `season_type_has_legs` | logical |  |
| `season_type_has_standings` | logical |  |
| `season_type_id` | character | Unique identifier for season type. |
| `season_type_name` | character | Season type name. |
| `season_type_slug` | character |  |
| `season_type_start_date` | character |  |
| `season_type_type` | integer | Season type type. |
| `season_type_weeks_$ref` | character |  |
| `season_type_year` | integer |  |
| `season_types_$ref` | character |  |
| `season_types_count` | integer |  |
| `season_types_items` | character |  |
| `season_types_page_count` | integer |  |
| `season_types_page_index` | integer |  |
| `season_types_page_size` | integer |  |
| `season_year` | integer | Season year string ('YYYY-YY' format). |
| `seasons_$ref` | character |  |
| `teams_$ref` | character |  |
| `transactions_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_league_root-example}

```python
espn_nba_league_root()
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_seasons-example}

```python
espn_nba_seasons()
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_games-example}

```python
espn_nba_games()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `competitions` | character |  |
| `date` | character | Date in YYYY-MM-DD format. |
| `id` | character | Id. |
| `links` | character |  |
| `name` | character | Display name. |
| `short_name` | character | Short display name. |
| `time_valid` | logical | Time valid. |
| `uid` | character | ESPN UID string. |
| `venues` | character |  |
| `league_$ref` | character |  |
| `season_$ref` | character |  |
| `season_type_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game-example}

```python
espn_nba_game(event_id='401584793')
```

_Last validated n/a._

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

### Example {#espn_nba_teams_core-example}

```python
espn_nba_teams_core()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `abbreviation` | character | Short abbreviation. |
| `alternate_color` | character | Alternate color (hex without leading '#'). |
| `color` | character | Primary color (hex without leading '#'). |
| `display_name` | character | Display name. |
| `guid` | character | Stable cross-league team GUID. |
| `id` | character | Id. |
| `is_active` | logical | Is active. |
| `is_all_star` | logical | Is all star. |
| `links` | character |  |
| `location` | character | Location. |
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
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state / region. |
| `venue_full_name` | character | Venue full name. |
| `venue_grass` | logical |  |
| `venue_guid` | character |  |
| `venue_id` | character | Unique venue identifier. |
| `venue_images` | character |  |
| `venue_indoor` | logical | TRUE if the venue is indoors. |
| `venue_short_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_team_core-example}

```python
espn_nba_team_core(team_id='4')
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_venues-example}

```python
espn_nba_venues()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `full_name` | character | Player's full name. |
| `grass` | logical | Grass. |
| `guid` | character | Stable cross-league team GUID. |
| `id` | character | Id. |
| `images` | character |  |
| `indoor` | logical | Indoor. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_venue-example}

```python
espn_nba_venue(venue_id='3663')
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_franchises-example}

```python
espn_nba_franchises()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `abbreviation` | character | Short abbreviation. |
| `color` | character | Primary color (hex without leading '#'). |
| `display_name` | character | Display name. |
| `id` | character | Id. |
| `is_active` | logical | Is active. |
| `location` | character | Location. |
| `name` | character | Display name. |
| `short_display_name` | character | Short display name. |
| `slug` | character | URL-safe identifier. |
| `uid` | character | ESPN UID string. |
| `team_$ref` | character |  |
| `venue_$ref` | character |  |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state / region. |
| `venue_full_name` | character | Venue full name. |
| `venue_grass` | logical |  |
| `venue_guid` | character |  |
| `venue_id` | character | Unique venue identifier. |
| `venue_images` | character |  |
| `venue_indoor` | logical | TRUE if the venue is indoors. |
| `venue_short_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_franchise-example}

```python
espn_nba_franchise(franchise_id='2')
```

_Last validated n/a._

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
| `$ref` | character |  |
| `career_records` | character |  |
| `coach_seasons` | character |  |
| `experience` | integer | Years of professional experience. |
| `first_name` | character | Player's first name. |
| `id` | character | Id. |
| `last_name` | character | Player's last name. |
| `uid` | character | ESPN UID string. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_coach-example}

```python
espn_nba_coach(coach_id='1')
```

_Last validated n/a._

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
| `$ref` | character |  |
| `display_value` | character | Display-formatted value. |
| `id` | character | Id. |
| `name` | character | Display name. |
| `stats` | character | Stats. |
| `summary` | character | W-L summary (e.g. "50-32"). |
| `type` | character | Record type / category. |
| `value` | double | Numeric or string value field. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_coach_record-example}

```python
espn_nba_coach_record(coach_id='1')
```

_Last validated n/a._

## espn_nba_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_nba_coach_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_coach_season-example}

```python
espn_nba_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_positions-example}

```python
espn_nba_positions()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `abbreviation` | character | Short abbreviation. |
| `display_name` | character | Display name. |
| `id` | character | Id. |
| `leaf` | logical |  |
| `name` | character | Display name. |
| `parent_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_position-example}

```python
espn_nba_position(position_id='1')
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_tournaments-example}

```python
espn_nba_tournaments()
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_awards-example}

```python
espn_nba_awards()
```

_Last validated n/a._

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
| `count` | integer | Count of count. |
| `items` | character |  |
| `page_count` | integer |  |
| `page_index` | integer |  |
| `page_size` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_award-example}

```python
espn_nba_award(award_id='1')
```

_Last validated n/a._

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
| `group_name` | character | Group name. |
| `group_abbreviation` | character | Group abbreviation. |
| `team_id` | character | Team id. |
| `team_name` | character | Team name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_location` | character | Team location. |
| `team_logo` | character | Team logo. |
| `avg_points_against` | double | Avg points against. |
| `avg_points_for` | double | Avg points for. |
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `division_win_percent` | double | Division win percent. |
| `games_behind` | double | Games behind. |
| `league_win_percent` | double | League win percent. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points` | double | Points. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `win_percent` | double | Win percent. |
| `wins` | double | Wins. |
| `games_ahead` | double | Games ahead. |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `vs. div.` | character | Vs. div.. |
| `vs. conf.` | character | Vs. conf.. |
| `last ten games` | character | Last ten games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_standings_core-example}

```python
espn_nba_standings_core()
```

_Last validated n/a._

## espn_nba_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/leaders](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nba_leaders_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_leaders_core-example}

```python
espn_nba_leaders_core()
```

_Last validated n/a._

## espn_nba_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/notes](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nba_league_notes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_league_notes-example}

```python
espn_nba_league_notes()
```

_Last validated n/a._

## espn_nba_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/talentpicks](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nba_talentpicks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_talentpicks-example}

```python
espn_nba_talentpicks()
```

_Last validated n/a._
