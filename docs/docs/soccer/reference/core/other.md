---
title: "SOCCER — ESPN core API (v2) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "SOCCER — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SOCCER — ESPN core API (v2) — Other

## espn_soccer_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_soccer_league_root-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character |  |
| `color` | character |  |
| `display_name` | character |  |
| `gender` | character |  |
| `guid` | character |  |
| `id` | character |  |
| `is_tournament` | logical |  |
| `links` | character |  |
| `logos` | character |  |
| `name` | character |  |
| `short_name` | character |  |
| `slug` | character |  |
| `uid` | character |  |
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
| `season_end_date` | character |  |
| `season_futures_$ref` | character |  |
| `season_power_index_leaders_$ref` | character |  |
| `season_power_indexes_$ref` | character |  |
| `season_rankings_$ref` | character |  |
| `season_start_date` | character |  |
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

### Example {#espn_soccer_league_root-example}

```python
espn_soccer_league_root(league='eng.1')
```

_Last validated n/a._

## espn_soccer_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/seasons?limit=200](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/seasons?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_soccer_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_seasons-example}

```python
espn_soccer_seasons(league='eng.1')
```

_Last validated n/a._

## espn_soccer_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events?limit=500](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_soccer_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_games-example}

```python
espn_soccer_games(league='eng.1')
```

_Last validated n/a._

## espn_soccer_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events/401584793](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_soccer_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `competitions` | character |  |
| `date` | character |  |
| `id` | character |  |
| `links` | character |  |
| `name` | character |  |
| `short_name` | character |  |
| `time_valid` | logical |  |
| `uid` | character |  |
| `venues` | character |  |
| `league_$ref` | character |  |
| `season_$ref` | character |  |
| `season_type_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_game-example}

```python
espn_soccer_game(league='eng.1', event_id='401584793')
```

_Last validated n/a._

## espn_soccer_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_soccer_teams_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | character | ESPN numeric identifier for the team. |
| `display_name` | character | Full display name of the team (e.g. 'Los Angeles Lakers'). |
| `abbreviation` | character | Team abbreviation. |
| `location` | character | Team location/city. |
| `name` | character | Short team name, typically the mascot (e.g. 'Lakers'). |
| `short_display_name` | character | Short team display name. |
| `nickname` | character | Alternative nickname used by ESPN for the team. |
| `slug` | character | URL slug for the team. |
| `uid` | character | ESPN universal id for the team. |
| `color` | character | Primary team color (hex). |
| `alternate_color` | character | Secondary team color (hex). |
| `is_active` | logical | Whether the team is currently active. |
| `is_all_star` | logical | Whether the team is an all-star side. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_teams_core-example}

```python
espn_soccer_teams_core(league='eng.1')
```

_Last validated n/a._

## espn_soccer_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/teams/4](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_soccer_team_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character |  |
| `alternate_color` | character |  |
| `color` | character |  |
| `display_name` | character |  |
| `guid` | character |  |
| `id` | character |  |
| `is_active` | logical |  |
| `is_all_star` | logical |  |
| `links` | character |  |
| `location` | character |  |
| `logos` | character |  |
| `name` | character |  |
| `short_display_name` | character |  |
| `slug` | character |  |
| `uid` | character |  |
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
| `venue_id` | character |  |
| `venue_images` | character |  |
| `venue_indoor` | logical |  |
| `venue_short_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_team_core-example}

```python
espn_soccer_team_core(league='eng.1', team_id='4')
```

_Last validated n/a._

## espn_soccer_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/venues?limit=1000](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/venues?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_soccer_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_venues-example}

```python
espn_soccer_venues(league='eng.1')
```

_Last validated n/a._

## espn_soccer_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/venues/3663](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_soccer_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `full_name` | character |  |
| `grass` | logical |  |
| `guid` | character |  |
| `id` | character |  |
| `images` | character |  |
| `indoor` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_venue-example}

```python
espn_soccer_venue(league='eng.1', venue_id='3663')
```

_Last validated n/a._

## espn_soccer_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/franchises?limit=200](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/franchises?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_soccer_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_franchises-example}

```python
espn_soccer_franchises(league='eng.1')
```

_Last validated n/a._

## espn_soccer_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/franchises/2](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_soccer_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character |  |
| `color` | character |  |
| `display_name` | character |  |
| `id` | character |  |
| `is_active` | logical |  |
| `location` | character |  |
| `name` | character |  |
| `short_display_name` | character |  |
| `slug` | character |  |
| `uid` | character |  |
| `team_$ref` | character |  |
| `venue_$ref` | character |  |
| `venue_address_city` | character |  |
| `venue_address_state` | character |  |
| `venue_full_name` | character |  |
| `venue_grass` | logical |  |
| `venue_guid` | character |  |
| `venue_id` | character |  |
| `venue_images` | character |  |
| `venue_indoor` | logical |  |
| `venue_short_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_franchise-example}

```python
espn_soccer_franchise(league='eng.1', franchise_id='2')
```

_Last validated n/a._

## espn_soccer_coach

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/coaches/1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#espn_soccer_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `career_records` | character |  |
| `coach_seasons` | character |  |
| `experience` | integer |  |
| `first_name` | character |  |
| `id` | character |  |
| `last_name` | character |  |
| `uid` | character |  |
| `birth_place_city` | character |  |
| `birth_place_state` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_coach-example}

```python
espn_soccer_coach(league='eng.1', coach_id='1')
```

_Last validated n/a._

## espn_soccer_coach_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/coaches/1/record/0](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/coaches/1/record/0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `record_type` | `record_type` |  |  | `Y` | record_type path parameter. |

### Returns {#espn_soccer_coach_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_value` | character |  |
| `id` | character |  |
| `name` | character |  |
| `stats` | character |  |
| `summary` | character |  |
| `type` | character |  |
| `value` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_coach_record-example}

```python
espn_soccer_coach_record(league='eng.1', coach_id='1')
```

_Last validated n/a._

## espn_soccer_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_soccer_coach_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_coach_season-example}

```python
espn_soccer_coach_season(league='eng.1', coach_id='1', season=2024)
```

_Last validated n/a._

## espn_soccer_positions

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/positions?limit=200](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/positions?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_soccer_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_positions-example}

```python
espn_soccer_positions(league='eng.1')
```

_Last validated n/a._

## espn_soccer_position

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/positions/1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  | position_id path parameter. |

### Returns {#espn_soccer_position-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character |  |
| `display_name` | character |  |
| `id` | character |  |
| `leaf` | logical |  |
| `name` | character |  |
| `parent_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_position-example}

```python
espn_soccer_position(league='eng.1', position_id='1')
```

_Last validated n/a._

## espn_soccer_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/tournaments?limit=200](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/tournaments?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_soccer_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_tournaments-example}

```python
espn_soccer_tournaments(league='eng.1')
```

_Last validated n/a._

## espn_soccer_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/awards?limit=200](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/awards?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_soccer_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_awards-example}

```python
espn_soccer_awards(league='eng.1')
```

_Last validated n/a._

## espn_soccer_award

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/awards/1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |

### Returns {#espn_soccer_award-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `count` | integer |  |
| `items` | character |  |
| `page_count` | integer |  |
| `page_index` | integer |  |
| `page_size` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_award-example}

```python
espn_soccer_award(league='eng.1', award_id='1')
```

_Last validated n/a._

## espn_soccer_standings_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/standings](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_soccer_standings_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character | Conference/group/table the row belongs to, flattened from the standings children hierarchy. |
| `team` | character | Display name of the team in this standings row. |
| `team_id` | character | ESPN numeric identifier for the team. |
| `team_abbreviation` | character | Team abbreviation. |
| `note` | character | Standings note (e.g. qualification/relegation marker). |
| `games_played` | double | Matches played. |
| `losses` | double | Number of matches the team has lost. |
| `point_differential` | double | Goal difference (for minus against). |
| `points` | double | Competition points. |
| `points_against` | double | Goals conceded. |
| `points_for` | double | Goals (or runs) scored by the team. |
| `ties` | double | Number of matches the team has drawn. |
| `wins` | double | Number of matches the team has won. |
| `advanced` | double | Whether the team has advanced/qualified. |
| `deductions` | double | Points deducted. |
| `ppg` | double | Points per game. |
| `rank` | double | Position within the group/table. |
| `rank_change` | double | Change in rank versus the previous update. |
| `overall` | character | Overall record summary as published by ESPN. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_standings_core-example}

```python
espn_soccer_standings_core(league='eng.1')
```

_Last validated n/a._

## espn_soccer_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/leaders](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_soccer_leaders_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_leaders_core-example}

```python
espn_soccer_leaders_core(league='eng.1')
```

_Last validated n/a._

## espn_soccer_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/notes](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_soccer_league_notes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_league_notes-example}

```python
espn_soccer_league_notes(league='eng.1')
```

_Last validated n/a._

## espn_soccer_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/talentpicks](https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_soccer_talentpicks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_soccer_talentpicks-example}

```python
espn_soccer_talentpicks(league='eng.1')
```

_Last validated n/a._
