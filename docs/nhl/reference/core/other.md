# NHL — ESPN core API (v2) — Other

> NHL — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package.

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
| `$ref` | character |  |
| `abbreviation` | character | Team abbreviation. |
| `color` | character | Primary color hex. |
| `display_name` | character | Player display name. |
| `gender` | character | League gender designation. |
| `guid` | character | Athlete global unique identifier. |
| `id` | character | Unique player identifier. |
| `is_tournament` | logical | Whether the league is a tournament. |
| `links` | character |  |
| `logos` | character |  |
| `name` | character | Team mascot name. |
| `short_name` | character | Short game name. |
| `slug` | character | URL slug. |
| `uid` | character | Competitor uid string. |
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
| `season_type_id` | character | Season type id (1=pre, 2=reg, 3=post, 4=off). |
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
| `season_year` | integer | Season end year. |
| `seasons_$ref` | character |  |
| `teams_$ref` | character |  |
| `transactions_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_league_root-example}

```python
espn_nhl_league_root()
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_seasons-example}

```python
espn_nhl_seasons()
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_games-example}

```python
espn_nhl_games()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `competitions` | character |  |
| `date` | character | Game date (ISO 8601 datetime string). |
| `id` | character | Unique player identifier. |
| `links` | character |  |
| `name` | character | Team mascot name. |
| `short_name` | character | Short game name. |
| `time_valid` | logical | Whether the start time is confirmed. |
| `uid` | character | Competitor uid string. |
| `venues` | character |  |
| `league_$ref` | character |  |
| `season_$ref` | character |  |
| `season_type_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_game-example}

```python
espn_nhl_game(event_id='401584793')
```

_Last validated n/a._

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

### Example {#espn_nhl_teams_core-example}

```python
espn_nhl_teams_core()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `abbreviation` | character | Team abbreviation. |
| `alternate_color` | character | Alternate color hex. |
| `color` | character | Primary color hex. |
| `display_name` | character | Player display name. |
| `guid` | character | Athlete global unique identifier. |
| `id` | character | Unique player identifier. |
| `is_active` | logical | Whether the team is active. |
| `is_all_star` | logical | Whether the team is an all-star team. |
| `links` | character |  |
| `location` | character | Team city/location. |
| `logos` | character |  |
| `name` | character | Team mascot name. |
| `short_display_name` | character | Short display name. |
| `slug` | character | URL slug. |
| `uid` | character | Competitor uid string. |
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
| `venue_full_name` | character | Venue full name. |
| `venue_grass` | logical |  |
| `venue_guid` | character |  |
| `venue_id` | character | Venue identifier. |
| `venue_images` | character |  |
| `venue_indoor` | logical | Whether the venue is indoors. |
| `venue_short_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_team_core-example}

```python
espn_nhl_team_core(team_id='4')
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_venues-example}

```python
espn_nhl_venues()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `full_name` | character | Player full name. |
| `grass` | logical | Whether the surface is grass. |
| `guid` | character | Athlete global unique identifier. |
| `id` | character | Unique player identifier. |
| `images` | character |  |
| `indoor` | logical | Whether the venue is indoors. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_venue-example}

```python
espn_nhl_venue(venue_id='3663')
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_franchises-example}

```python
espn_nhl_franchises()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `abbreviation` | character | Team abbreviation. |
| `color` | character | Primary color hex. |
| `display_name` | character | Player display name. |
| `id` | character | Unique player identifier. |
| `is_active` | logical | Whether the team is active. |
| `location` | character | Team city/location. |
| `name` | character | Team mascot name. |
| `short_display_name` | character | Short display name. |
| `slug` | character | URL slug. |
| `uid` | character | Competitor uid string. |
| `team_$ref` | character |  |
| `venue_$ref` | character |  |
| `venue_address_city` | character |  |
| `venue_address_state` | character |  |
| `venue_full_name` | character | Venue full name. |
| `venue_grass` | logical |  |
| `venue_guid` | character |  |
| `venue_id` | character | Venue identifier. |
| `venue_images` | character |  |
| `venue_indoor` | logical | Whether the venue is indoors. |
| `venue_short_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_franchise-example}

```python
espn_nhl_franchise(franchise_id='2')
```

_Last validated n/a._

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
| `$ref` | character |  |
| `career_records` | character |  |
| `coach_seasons` | character |  |
| `experience` | integer | Years of coaching experience (if returned). |
| `first_name` | character | Player first name. |
| `id` | character | Unique player identifier. |
| `last_name` | character | Player last name. |
| `uid` | character | Competitor uid string. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_coach-example}

```python
espn_nhl_coach(coach_id='1')
```

_Last validated n/a._

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
| `$ref` | character |  |
| `display_value` | character | Leader stat display value. |
| `id` | character | Unique player identifier. |
| `name` | character | Team mascot name. |
| `stats` | character |  |
| `summary` | character | Record summary string (e.g. "25-15-10"). |
| `type` | character | Competitor type (e.g. "team"). |
| `value` | double | Leader stat numeric value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_coach_record-example}

```python
espn_nhl_coach_record(coach_id='1')
```

_Last validated n/a._

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

### Example {#espn_nhl_coach_season-example}

```python
espn_nhl_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_positions-example}

```python
espn_nhl_positions()
```

_Last validated n/a._

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
| `$ref` | character |  |
| `abbreviation` | character | Team abbreviation. |
| `display_name` | character | Player display name. |
| `id` | character | Unique player identifier. |
| `leaf` | logical | Whether this is a leaf (non-parent) position. |
| `name` | character | Team mascot name. |
| `parent_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_position-example}

```python
espn_nhl_position(position_id='1')
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_tournaments-example}

```python
espn_nhl_tournaments()
```

_Last validated n/a._

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
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_awards-example}

```python
espn_nhl_awards()
```

_Last validated n/a._

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
| `count` | integer | Total athlete count matching the filter. |
| `items` | character |  |
| `page_count` | integer | Total pages in the collection. |
| `page_index` | integer |  |
| `page_size` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_award-example}

```python
espn_nhl_award(award_id='1')
```

_Last validated n/a._

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
| `group_name` | character | Group name. |
| `group_abbreviation` | character | Group abbreviation. |
| `team_id` | character | Team id. |
| `team_name` | character | Team name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_location` | character | Team location. |
| `team_logo` | character | Team logo. |
| `ot_losses` | double | Overtime losses. |
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `games_behind` | double | Games behind. |
| `games_played` | double | Matches played. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points` | double | Points. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `wins` | double | Wins. |
| `overtime_losses` | double | Total overtime losses. |
| `overtime_wins` | double | Overtime wins. |
| `points_diff` | double | Difference between total points scored for and against the team across all games played. |
| `reg_losses` | double | Number of losses the team has suffered in regulation time (excluding overtime and shootout losses). |
| `reg_wins` | double | Number of wins the team has earned in regulation time (excluding overtime and shootout wins). |
| `rot_losses` | double | Number of losses the team has suffered in overtime or the shootout (non-regulation losses). |
| `rot_wins` | double | Number of wins the team has earned in overtime or the shootout (non-regulation wins). |
| `shootout_losses` | double | Shootout losses. |
| `shootout_wins` | double | Shootout wins. |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `last ten games` | character | Last ten games. |
| `vs. div.` | character | Vs. div.. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_standings_core-example}

```python
espn_nhl_standings_core()
```

_Last validated n/a._

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

### Example {#espn_nhl_leaders_core-example}

```python
espn_nhl_leaders_core()
```

_Last validated n/a._

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

### Example {#espn_nhl_league_notes-example}

```python
espn_nhl_league_notes()
```

_Last validated n/a._

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
| `pick_competition_$ref` | character |  |
| `pick_competitor_$ref` | character |  |
| `pick_correct` | logical |  |
| `pick_person_display_name` | character |  |
| `pick_person_first_name` | character |  |
| `pick_person_headshot_alt` | character |  |
| `pick_person_headshot_href` | character |  |
| `pick_person_id` | character |  |
| `pick_person_last_name` | character |  |
| `week_record_display_value` | character |  |
| `week_record_season_$ref` | character |  |
| `week_record_stats` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_talentpicks-example}

```python
espn_nhl_talentpicks()
```

_Last validated n/a._
