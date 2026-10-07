---
title: "WBB — ESPN core API (v2) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "WBB — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WBB — ESPN core API (v2) — Other

## espn_wbb_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wbb_league_root-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character | Short abbreviation. |
| `color` | character | Primary color (hex without leading '#'). |
| `display_name` | character | Display name. |
| `gender` | character |  |
| `guid` | character | Stable cross-league team GUID. |
| `id` | character | Unique play identification number |
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

### Example {#espn_wbb_league_root-example}

```python
espn_wbb_league_root()
```

_Last validated n/a._

## espn_wbb_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/seasons?limit=200](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/seasons?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wbb_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_seasons-example}

```python
espn_wbb_seasons()
```

_Last validated n/a._

## espn_wbb_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/events?limit=500](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wbb_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_games-example}

```python
espn_wbb_games()
```

_Last validated n/a._

## espn_wbb_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/events/401584793](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_wbb_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `competitions` | character |  |
| `date` | character | Date in YYYY-MM-DD format. |
| `id` | character | Unique play identification number |
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

### Example {#espn_wbb_game-example}

```python
espn_wbb_game(event_id='401584793')
```

_Last validated n/a._

## espn_wbb_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_wbb_teams_core-returns}

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

### Example {#espn_wbb_teams_core-example}

```python
espn_wbb_teams_core()
```

_Last validated n/a._

## espn_wbb_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/teams/4](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_wbb_team_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character | Short abbreviation. |
| `alternate_color` | character | Alternate color (hex without leading '#'). |
| `color` | character | Primary color (hex without leading '#'). |
| `display_name` | character | Display name. |
| `guid` | character | Stable cross-league team GUID. |
| `id` | character | Unique play identification number |
| `is_active` | logical | Whether the team was active in this season. |
| `is_all_star` | logical | Is all star. |
| `links` | character |  |
| `location` | character | Filter results by game location. |
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

### Example {#espn_wbb_team_core-example}

```python
espn_wbb_team_core(team_id='4')
```

_Last validated n/a._

## espn_wbb_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/venues?limit=1000](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/venues?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wbb_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_venues-example}

```python
espn_wbb_venues()
```

_Last validated n/a._

## espn_wbb_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/venues/3663](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_wbb_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `full_name` | character | Player's full name. |
| `grass` | logical | Grass. |
| `guid` | character | Stable cross-league team GUID. |
| `id` | character | Unique play identification number |
| `images` | character |  |
| `indoor` | logical | Indoor. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_venue-example}

```python
espn_wbb_venue(venue_id='3663')
```

_Last validated n/a._

## espn_wbb_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/franchises?limit=200](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/franchises?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wbb_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_franchises-example}

```python
espn_wbb_franchises()
```

_Last validated n/a._

## espn_wbb_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/franchises/2](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_wbb_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character | Short abbreviation. |
| `color` | character | Primary color (hex without leading '#'). |
| `display_name` | character | Display name. |
| `id` | character | Unique play identification number |
| `is_active` | logical | Whether the team was active in this season. |
| `location` | character | Filter results by game location. |
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

### Example {#espn_wbb_franchise-example}

```python
espn_wbb_franchise(franchise_id='2')
```

_Last validated n/a._

## espn_wbb_coach

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/coaches/1](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#espn_wbb_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `career_records` | character |  |
| `coach_seasons` | character |  |
| `experience` | integer | Years of professional experience. |
| `first_name` | character | Player's first name. |
| `id` | character | Unique play identification number |
| `last_name` | character | Player's last name. |
| `uid` | character | ESPN UID string. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_coach-example}

```python
espn_wbb_coach(coach_id='1')
```

_Last validated n/a._

## espn_wbb_coach_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/coaches/1/record/0](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/coaches/1/record/0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `record_type` | `record_type` |  |  | `Y` | record_type path parameter. |

### Returns {#espn_wbb_coach_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_value` | character | Display-formatted value. |
| `id` | character | Unique play identification number |
| `name` | character | Display name. |
| `stats` | character | Stats. |
| `summary` | character | Summary. |
| `type` | character | Record type / category. |
| `value` | double | Numeric or string value field. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_coach_record-example}

```python
espn_wbb_coach_record(coach_id='1')
```

_Last validated n/a._

## espn_wbb_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_wbb_coach_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_coach_season-example}

```python
espn_wbb_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

## espn_wbb_positions

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/positions?limit=200](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/positions?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wbb_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_positions-example}

```python
espn_wbb_positions()
```

_Last validated n/a._

## espn_wbb_position

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/positions/1](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  | position_id path parameter. |

### Returns {#espn_wbb_position-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character | Short abbreviation. |
| `display_name` | character | Display name. |
| `id` | character | Unique play identification number |
| `leaf` | logical |  |
| `name` | character | Display name. |
| `parent_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_position-example}

```python
espn_wbb_position(position_id='1')
```

_Last validated n/a._

## espn_wbb_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/tournaments?limit=200](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/tournaments?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wbb_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_tournaments-example}

```python
espn_wbb_tournaments()
```

_Last validated n/a._

## espn_wbb_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/awards?limit=200](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/awards?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wbb_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_awards-example}

```python
espn_wbb_awards()
```

_Last validated n/a._

## espn_wbb_award

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/awards/1](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |

### Returns {#espn_wbb_award-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `count` | integer | Count of count. |
| `items` | character |  |
| `page_count` | integer |  |
| `page_index` | integer |  |
| `page_size` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_award-example}

```python
espn_wbb_award(award_id='1')
```

_Last validated n/a._

## espn_wbb_standings_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/standings](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wbb_standings_core-returns}

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
| `games_behind` | double | Games behind. |
| `league_win_percent` | double | League win percent. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `win_percent` | double | Win percent. |
| `wins` | double | Wins. |
| `games_ahead` | double | Games ahead. |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `vs ap top 25` | character | The team's win-loss record against opponents ranked in the AP Top 25 poll. |
| `vs usa ranked teams` | character | The team's win-loss record against opponents ranked in the USA Today Coaches Poll. |
| `vs. conf.` | character | Vs. conf.. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_standings_core-example}

```python
espn_wbb_standings_core()
```

_Last validated n/a._

## espn_wbb_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/leaders](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wbb_leaders_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_leaders_core-example}

```python
espn_wbb_leaders_core()
```

_Last validated n/a._

## espn_wbb_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/notes](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wbb_league_notes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_league_notes-example}

```python
espn_wbb_league_notes()
```

_Last validated n/a._

## espn_wbb_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/talentpicks](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wbb_talentpicks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_talentpicks-example}

```python
espn_wbb_talentpicks()
```

_Last validated n/a._

## espn_wbb_recruiting_years

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/recruiting`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/recruiting](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/recruiting)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wbb_recruiting_years-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_recruiting_years-example}

```python
espn_wbb_recruiting_years()
```

_Last validated n/a._

## espn_wbb_recruiting_players

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/recruiting/{year}/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/recruiting/2026/athletes?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/recruiting/2026/athletes?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_wbb_recruiting_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `activity` | character |  |
| `analysis` | character |  |
| `attributes` | character |  |
| `grade` | integer |  |
| `grade_display_value` | character |  |
| `recruiting_class` | integer |  |
| `schools` | character |  |
| `athlete_alternate_id` | character |  |
| `athlete_display_name` | character | Athlete display name (full). |
| `athlete_first_name` | character |  |
| `athlete_full_name` | character |  |
| `athlete_height` | double |  |
| `athlete_high_school_address_address1` | character |  |
| `athlete_high_school_address_city` | character |  |
| `athlete_high_school_address_state` | character |  |
| `athlete_high_school_address_state_abbreviation` | character |  |
| `athlete_high_school_address_zip_code` | character |  |
| `athlete_high_school_id` | character |  |
| `athlete_high_school_name` | character |  |
| `athlete_high_school_proper_name` | character |  |
| `athlete_hometown_city` | character |  |
| `athlete_hometown_state` | character |  |
| `athlete_hometown_state_abbreviation` | character |  |
| `athlete_id` | character | Unique athlete identifier (ESPN). |
| `athlete_last_name` | character |  |
| `athlete_links` | character |  |
| `athlete_position_abbreviation` | character | Athlete position abbreviation (G / F / C). |
| `athlete_position_id` | character |  |
| `athlete_short_name` | character | Athlete short display name. |
| `athlete_weight` | double |  |
| `status_description` | character |  |
| `status_id` | integer | Status identifier. |
| `athlete_hometown_zip_code` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_recruiting_players-example}

```python
espn_wbb_recruiting_players(year=2026)
```

_Last validated n/a._

## espn_wbb_recruiting_rankings

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/recruiting/{year}/rankings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/recruiting/2026/rankings](https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/recruiting/2026/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns {#espn_wbb_recruiting_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wbb_recruiting_rankings-example}

```python
espn_wbb_recruiting_rankings(year=2026)
```

_Last validated n/a._
