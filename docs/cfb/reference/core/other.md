# CFB — ESPN core API (v2) — Other

> CFB — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package.

## espn_cfb_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cfb_league_root-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character | Metric abbreviation. |
| `color` | character | Primary team color (hex, no `#`). |
| `display_name` | character | Human-readable metric name. |
| `gender` | character |  |
| `guid` | character | ESPN athlete GUID. |
| `id` | character | 247Sports referencing id for the recruit. |
| `is_tournament` | logical |  |
| `links` | character |  |
| `logos` | character | Team logos. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `short_name` | character | Ranking source short name (e.g. `AP Poll`). |
| `slug` | character | URL slug for the team. |
| `uid` | character | ESPN global unique identifier. |
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

### Example {#espn_cfb_league_root-example}

```python
espn_cfb_league_root()
```

_Last validated n/a._

## espn_cfb_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons?limit=200](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cfb_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_seasons-example}

```python
espn_cfb_seasons()
```

_Last validated n/a._

## espn_cfb_groups

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}/groups`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/groups](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_cfb_groups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_groups-example}

```python
espn_cfb_groups(season=2024, season_type=2)
```

_Last validated n/a._

## espn_cfb_futures

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/futures`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/futures](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/futures)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_cfb_futures-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_name` | character | Human-readable metric name. |
| `futures` | character |  |
| `id` | integer | 247Sports referencing id for the recruit. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_futures-example}

```python
espn_cfb_futures(season=2024)
```

_Last validated n/a._

## espn_cfb_team_powerindex

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/powerindex[/{team_id}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/powerindex](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/powerindex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `team_id` | `team_id` |  |  | `Y` | team_id path parameter. |

### Returns {#espn_cfb_team_powerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_updated` | character | Timestamp ESPN last refreshed the power index. |
| `max_game_date` | character |  |
| `run_cutoff_date` | character |  |
| `run_date_time_key` | integer |  |
| `season` | integer | Season (4-digit year). |
| `season_type` | integer | ESPN season type (2 = regular, 3 = postseason). |
| `stats` | character |  |
| `league_$ref` | character |  |
| `team_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_team_powerindex-example}

```python
espn_cfb_team_powerindex(season=2024)
```

_Last validated n/a._

## espn_cfb_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events?limit=500](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cfb_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_games-example}

```python
espn_cfb_games()
```

_Last validated n/a._

## espn_cfb_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_cfb_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `competitions` | character |  |
| `date` | character | Date of the poll release. |
| `id` | character | 247Sports referencing id for the recruit. |
| `links` | character |  |
| `name` | character | Position name (e.g. `Quarterback`). |
| `short_name` | character | Ranking source short name (e.g. `AP Poll`). |
| `time_valid` | logical |  |
| `uid` | character | ESPN global unique identifier. |
| `venues` | character |  |
| `league_$ref` | character |  |
| `season_$ref` | character |  |
| `season_type_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_game-example}

```python
espn_cfb_game(event_id='401584793')
```

_Last validated n/a._

## espn_cfb_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_cfb_teams_core-returns}

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

### Example {#espn_cfb_teams_core-example}

```python
espn_cfb_teams_core()
```

_Last validated n/a._

## espn_cfb_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/teams/4](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_cfb_team_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character | Metric abbreviation. |
| `alternate_color` | character | Alternate team color (hex, no `#`). |
| `color` | character | Primary team color (hex, no `#`). |
| `display_name` | character | Human-readable metric name. |
| `guid` | character | ESPN athlete GUID. |
| `id` | character | 247Sports referencing id for the recruit. |
| `is_active` | logical | Whether the team is currently active. |
| `is_all_star` | logical | Whether the team is an all-star team. |
| `links` | character |  |
| `location` | character | Team location / school name. |
| `logos` | character | Team logos. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `short_display_name` | character | Short human-readable metric name. |
| `slug` | character | URL slug for the team. |
| `uid` | character | ESPN global unique identifier. |
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
| `venue_grass` | logical | Whether the home venue has a grass surface. |
| `venue_guid` | character |  |
| `venue_id` | character | Referencing venue id. |
| `venue_images` | character |  |
| `venue_indoor` | logical | Whether the home venue is indoors. |
| `venue_short_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_team_core-example}

```python
espn_cfb_team_core(team_id='4')
```

_Last validated n/a._

## espn_cfb_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/venues?limit=1000](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/venues?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cfb_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_venues-example}

```python
espn_cfb_venues()
```

_Last validated n/a._

## espn_cfb_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/venues/3663](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_cfb_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `grass` | logical | TRUE/FALSE response on whether the field is grass or not. |
| `guid` | character | ESPN athlete GUID. |
| `id` | character | 247Sports referencing id for the recruit. |
| `images` | character |  |
| `indoor` | logical | `TRUE` if the venue is indoors. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_venue-example}

```python
espn_cfb_venue(venue_id='3663')
```

_Last validated n/a._

## espn_cfb_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/franchises?limit=200](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/franchises?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cfb_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_franchises-example}

```python
espn_cfb_franchises()
```

_Last validated n/a._

## espn_cfb_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/franchises/2](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_cfb_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character | Metric abbreviation. |
| `color` | character | Primary team color (hex, no `#`). |
| `display_name` | character | Human-readable metric name. |
| `id` | character | 247Sports referencing id for the recruit. |
| `is_active` | logical | Whether the team is currently active. |
| `location` | character | Team location / school name. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `short_display_name` | character | Short human-readable metric name. |
| `slug` | character | URL slug for the team. |
| `uid` | character | ESPN global unique identifier. |
| `team_$ref` | character |  |
| `venue_$ref` | character |  |
| `venue_address_city` | character |  |
| `venue_address_state` | character |  |
| `venue_full_name` | character |  |
| `venue_grass` | logical | Whether the home venue has a grass surface. |
| `venue_guid` | character |  |
| `venue_id` | character | Referencing venue id. |
| `venue_images` | character |  |
| `venue_indoor` | logical | Whether the home venue is indoors. |
| `venue_short_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_franchise-example}

```python
espn_cfb_franchise(franchise_id='2')
```

_Last validated n/a._

## espn_cfb_coach

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/1](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#espn_cfb_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `career_records` | character |  |
| `coach_seasons` | character |  |
| `experience` | integer | Years of experience ESPN credits the coach. |
| `first_name` | character | Athlete first name. |
| `id` | character | 247Sports referencing id for the recruit. |
| `last_name` | character | Athlete last name. |
| `uid` | character | ESPN global unique identifier. |
| `birth_place_city` | character |  |
| `birth_place_state` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_coach-example}

```python
espn_cfb_coach(coach_id='1')
```

_Last validated n/a._

## espn_cfb_coach_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/1/record/0](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/1/record/0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `record_type` | `record_type` |  |  | `Y` | record_type path parameter. |

### Returns {#espn_cfb_coach_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_value` | character | Display-formatted metric value as shown on ESPN. |
| `id` | character | 247Sports referencing id for the recruit. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `stats` | character |  |
| `summary` | character | Win-loss summary string (e.g. `2-0`). |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `value` | double | Metric value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_coach_record-example}

```python
espn_cfb_coach_record(coach_id='1')
```

_Last validated n/a._

## espn_cfb_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_cfb_coach_season-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_coach_season-example}

```python
espn_cfb_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

## espn_cfb_positions

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/positions?limit=200](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/positions?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cfb_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_positions-example}

```python
espn_cfb_positions()
```

_Last validated n/a._

## espn_cfb_position

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/positions/1](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  | position_id path parameter. |

### Returns {#espn_cfb_position-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character | Metric abbreviation. |
| `display_name` | character | Human-readable metric name. |
| `id` | character | 247Sports referencing id for the recruit. |
| `leaf` | logical | `TRUE` for a most-specific (leaf) position. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `parent_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_position-example}

```python
espn_cfb_position(position_id='1')
```

_Last validated n/a._

## espn_cfb_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/tournaments?limit=200](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/tournaments?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cfb_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_tournaments-example}

```python
espn_cfb_tournaments()
```

_Last validated n/a._

## espn_cfb_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/awards?limit=200](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/awards?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cfb_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_awards-example}

```python
espn_cfb_awards()
```

_Last validated n/a._

## espn_cfb_award

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/awards/1](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |

### Returns {#espn_cfb_award-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `count` | integer | Total number of players in the season index. |
| `items` | character |  |
| `page_count` | integer | Total number of pages in the season index. |
| `page_index` | integer |  |
| `page_size` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_award-example}

```python
espn_cfb_award(award_id='1')
```

_Last validated n/a._

## espn_cfb_standings_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/standings](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cfb_standings_core-returns}

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
| `games_behind` | double | Games behind. |
| `league_win_percent` | double | League win percent. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `wins` | double | Wins. |
| `division_losses` | double | Number of games the team has lost against opponents within their own division. |
| `division_ties` | double | Number of games the team has tied against opponents within their own division. |
| `division_wins` | double | Number of games the team has won against opponents within their own division. |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `away` | character | Away team name. |
| `vs. conf.` | character | Vs. conf.. |
| `vs ap top 25` | character | The team's win-loss record against opponents ranked in the AP Top 25 poll. |
| `vs usa ranked teams` | character | The team's win-loss record against opponents ranked in the USA Today Coaches Poll. |
| `vs division` | double | The team's win-loss(-tie) record against opponents within their own division, serialized as a numeric value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_standings_core-example}

```python
espn_cfb_standings_core()
```

_Last validated n/a._

## espn_cfb_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/leaders](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cfb_leaders_core-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, 2026-10-07); its rows sit under keys it does not read (top level: $ref, abbreviation, categories, id, name, type).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_leaders_core-example}

```python
espn_cfb_leaders_core()
```

_Last validated n/a._

## espn_cfb_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/notes](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cfb_league_notes-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_league_notes-example}

```python
espn_cfb_league_notes()
```

_Last validated n/a._

## espn_cfb_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/talentpicks](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cfb_talentpicks-returns}

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

### Example {#espn_cfb_talentpicks-example}

```python
espn_cfb_talentpicks()
```

_Last validated n/a._

## espn_cfb_recruits

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/recruits`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/recruits?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/recruits?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_cfb_recruits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_recruits-example}

```python
espn_cfb_recruits(season=2024)
```

_Last validated n/a._

## espn_cfb_recruiting_years

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/recruiting`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/recruiting](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/recruiting)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cfb_recruiting_years-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_recruiting_years-example}

```python
espn_cfb_recruiting_years()
```

_Last validated n/a._

## espn_cfb_recruiting_players

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/recruiting/{year}/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/recruiting/2026/athletes?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/recruiting/2026/athletes?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_cfb_recruiting_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `activity` | character |  |
| `analysis` | character |  |
| `attributes` | character |  |
| `grade` | integer | ESPN recruit grade (0-100; `0` = not rated). |
| `grade_display_value` | character | Display-formatted grade (`NR` when not rated). |
| `recruiting_class` | integer | Recruiting class / signing year. |
| `schools` | character |  |
| `athlete_alternate_id` | character |  |
| `athlete_display_name` | character | Player display name. |
| `athlete_first_name` | character | Player first name. |
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
| `athlete_id` | character | ESPN athlete id. |
| `athlete_last_name` | character | Player last name. |
| `athlete_links` | character |  |
| `athlete_position_abbreviation` | character | Player position abbreviation. |
| `athlete_position_id` | character |  |
| `athlete_short_name` | character |  |
| `athlete_weight` | double |  |
| `status_description` | character |  |
| `status_id` | integer | ESPN commitment status id. |
| `athlete_hometown_zip_code` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_recruiting_players-example}

```python
espn_cfb_recruiting_players(year=2026)
```

_Last validated n/a._

## espn_cfb_recruiting_rankings

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/recruiting/{year}/rankings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/recruiting/2026/rankings](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/recruiting/2026/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns {#espn_cfb_recruiting_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_recruiting_rankings-example}

```python
espn_cfb_recruiting_rankings(year=2026)
```

_Last validated n/a._

## espn_cfb_week_rankings

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}/weeks/{week}/rankings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/weeks/1/rankings](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/weeks/1/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `week` | `week` |  | `Y` |  | week path parameter. |

### Returns {#espn_cfb_week_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_week_rankings-example}

```python
espn_cfb_week_rankings(season=2024, season_type=2, week=1)
```

_Last validated n/a._
