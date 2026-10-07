---
title: "NFL — ESPN core API (v2) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "NFL — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — ESPN core API (v2) — Other

## espn_nfl_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nfl_league_root-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character |  |
| `color` | character |  |
| `display_name` | character | Full name of player |
| `gender` | character |  |
| `guid` | character |  |
| `id` | character | ID of the player in the 'name' column. |
| `is_tournament` | logical |  |
| `links` | character |  |
| `logos` | character |  |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `short_name` | character | Player short name (i.e. "F.Last") |
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

### Example {#espn_nfl_league_root-example}

```python
espn_nfl_league_root()
```

_Last validated n/a._

## espn_nfl_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons?limit=200](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nfl_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_seasons-example}

```python
espn_nfl_seasons()
```

_Last validated n/a._

## espn_nfl_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events?limit=500](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nfl_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_games-example}

```python
espn_nfl_games()
```

_Last validated n/a._

## espn_nfl_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_nfl_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `competitions` | character |  |
| `date` | character |  |
| `id` | character | ID of the player in the 'name' column. |
| `links` | character |  |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `short_name` | character | Player short name (i.e. "F.Last") |
| `time_valid` | logical |  |
| `uid` | character |  |
| `venues` | character |  |
| `league_$ref` | character |  |
| `season_$ref` | character |  |
| `season_type_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_game-example}

```python
espn_nfl_game(event_id='401584793')
```

_Last validated n/a._

## espn_nfl_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_nfl_teams_core-returns}

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

### Example {#espn_nfl_teams_core-example}

```python
espn_nfl_teams_core()
```

_Last validated n/a._

## espn_nfl_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams/4](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_nfl_team_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character |  |
| `alternate_color` | character |  |
| `color` | character |  |
| `display_name` | character | Full name of player |
| `guid` | character |  |
| `id` | character | ID of the player in the 'name' column. |
| `is_active` | logical | Active contract |
| `is_all_star` | logical |  |
| `links` | character |  |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `logos` | character |  |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
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

### Example {#espn_nfl_team_core-example}

```python
espn_nfl_team_core(team_id='4')
```

_Last validated n/a._

## espn_nfl_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues?limit=1000](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nfl_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_venues-example}

```python
espn_nfl_venues()
```

_Last validated n/a._

## espn_nfl_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues/3663](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_nfl_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `full_name` | character | Full name as per NFL.com |
| `grass` | logical |  |
| `guid` | character |  |
| `id` | character | ID of the player in the 'name' column. |
| `images` | character |  |
| `indoor` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_venue-example}

```python
espn_nfl_venue(venue_id='3663')
```

_Last validated n/a._

## espn_nfl_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/franchises?limit=200](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/franchises?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nfl_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_franchises-example}

```python
espn_nfl_franchises()
```

_Last validated n/a._

## espn_nfl_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/franchises/2](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_nfl_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character |  |
| `color` | character |  |
| `display_name` | character | Full name of player |
| `id` | character | ID of the player in the 'name' column. |
| `is_active` | logical | Active contract |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
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

### Example {#espn_nfl_franchise-example}

```python
espn_nfl_franchise(franchise_id='2')
```

_Last validated n/a._

## espn_nfl_coach

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/1](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#espn_nfl_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `career_records` | character |  |
| `coach_seasons` | character |  |
| `experience` | integer |  |
| `first_name` | character | First name of player |
| `id` | character | ID of the player in the 'name' column. |
| `last_name` | character | Last name of player |
| `uid` | character |  |
| `birth_place_city` | character |  |
| `birth_place_state` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_coach-example}

```python
espn_nfl_coach(coach_id='1')
```

_Last validated n/a._

## espn_nfl_coach_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/1/record/0](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/1/record/0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `record_type` | `record_type` |  |  | `Y` | record_type path parameter. |

### Returns {#espn_nfl_coach_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_value` | character |  |
| `id` | character | ID of the player in the 'name' column. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `stats` | character |  |
| `summary` | character |  |
| `type` | character |  |
| `value` | double | Total contract value |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_coach_record-example}

```python
espn_nfl_coach_record(coach_id='1')
```

_Last validated n/a._

## espn_nfl_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_nfl_coach_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_coach_season-example}

```python
espn_nfl_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

## espn_nfl_positions

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions?limit=200](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nfl_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_positions-example}

```python
espn_nfl_positions()
```

_Last validated n/a._

## espn_nfl_position

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions/1](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  | position_id path parameter. |

### Returns {#espn_nfl_position-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character |  |
| `display_name` | character | Full name of player |
| `id` | character | ID of the player in the 'name' column. |
| `leaf` | logical |  |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `parent_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_position-example}

```python
espn_nfl_position(position_id='1')
```

_Last validated n/a._

## espn_nfl_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/tournaments?limit=200](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/tournaments?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nfl_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_tournaments-example}

```python
espn_nfl_tournaments()
```

_Last validated n/a._

## espn_nfl_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/awards?limit=200](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/awards?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nfl_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_awards-example}

```python
espn_nfl_awards()
```

_Last validated n/a._

## espn_nfl_award

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/awards/1](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |

### Returns {#espn_nfl_award-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `count` | integer |  |
| `items` | character |  |
| `page_count` | integer |  |
| `page_index` | integer |  |
| `page_size` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_award-example}

```python
espn_nfl_award(award_id='1')
```

_Last validated n/a._

## espn_nfl_standings_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/standings](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nfl_standings_core-returns}

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
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `games_behind` | double | Games behind. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `ties` | double | Number of matches the team has drawn. |
| `win_percent` | double | Win percent. |
| `wins` | double | Wins. |
| `division_losses` | double | Number of games the team has lost against opponents within their own division. |
| `division_record` | double | The team's composite win-loss-tie record against division opponents, serialized as a numeric value. |
| `division_ties` | double | Number of games the team has tied against opponents within their own division. |
| `division_wins` | double | Number of games the team has won against opponents within their own division. |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `vs. div.` | character | Vs. div.. |
| `vs. conf.` | character | Vs. conf.. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_standings_core-example}

```python
espn_nfl_standings_core()
```

_Last validated n/a._

## espn_nfl_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/leaders](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nfl_leaders_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_leaders_core-example}

```python
espn_nfl_leaders_core()
```

_Last validated n/a._

## espn_nfl_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/notes](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nfl_league_notes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_league_notes-example}

```python
espn_nfl_league_notes()
```

_Last validated n/a._

## espn_nfl_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/talentpicks](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nfl_talentpicks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_talentpicks-example}

```python
espn_nfl_talentpicks()
```

_Last validated n/a._
