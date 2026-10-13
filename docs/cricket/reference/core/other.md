# CRICKET — ESPN core API (v2) — Other

> CRICKET — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package.

## espn_cricket_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cricket_league_root-returns}

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

### Example {#espn_cricket_league_root-example}

```python
espn_cricket_league_root(league='eng.1')
```

_Last validated n/a._

## espn_cricket_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/seasons?limit=200](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/seasons?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cricket_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_seasons-example}

```python
espn_cricket_seasons(league='eng.1')
```

_Last validated n/a._

## espn_cricket_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/events?limit=500](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cricket_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_games-example}

```python
espn_cricket_games(league='eng.1')
```

_Last validated n/a._

## espn_cricket_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/events/401584793](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_cricket_game-returns}

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

### Example {#espn_cricket_game-example}

```python
espn_cricket_game(league='eng.1', event_id='401584793')
```

_Last validated n/a._

## espn_cricket_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_cricket_teams_core-returns}

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

### Example {#espn_cricket_teams_core-example}

```python
espn_cricket_teams_core(league='eng.1')
```

_Last validated n/a._

## espn_cricket_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/teams/4](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_cricket_team_core-returns}

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

### Example {#espn_cricket_team_core-example}

```python
espn_cricket_team_core(league='eng.1', team_id='4')
```

_Last validated n/a._

## espn_cricket_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/venues?limit=1000](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/venues?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cricket_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_venues-example}

```python
espn_cricket_venues(league='eng.1')
```

_Last validated n/a._

## espn_cricket_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/venues/3663](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_cricket_venue-returns}

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

### Example {#espn_cricket_venue-example}

```python
espn_cricket_venue(league='eng.1', venue_id='3663')
```

_Last validated n/a._

## espn_cricket_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/franchises?limit=200](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/franchises?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cricket_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_franchises-example}

```python
espn_cricket_franchises(league='eng.1')
```

_Last validated n/a._

## espn_cricket_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/franchises/2](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_cricket_franchise-returns}

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

### Example {#espn_cricket_franchise-example}

```python
espn_cricket_franchise(league='eng.1', franchise_id='2')
```

_Last validated n/a._

## espn_cricket_coach

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/coaches/1](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#espn_cricket_coach-returns}

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

### Example {#espn_cricket_coach-example}

```python
espn_cricket_coach(league='eng.1', coach_id='1')
```

_Last validated n/a._

## espn_cricket_coach_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/coaches/1/record/0](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/coaches/1/record/0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `record_type` | `record_type` |  |  | `Y` | record_type path parameter. |

### Returns {#espn_cricket_coach_record-returns}

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

### Example {#espn_cricket_coach_record-example}

```python
espn_cricket_coach_record(league='eng.1', coach_id='1')
```

_Last validated n/a._

## espn_cricket_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_cricket_coach_season-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_coach_season-example}

```python
espn_cricket_coach_season(league='eng.1', coach_id='1', season=2024)
```

_Last validated n/a._

## espn_cricket_positions

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/positions?limit=200](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/positions?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cricket_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_positions-example}

```python
espn_cricket_positions(league='eng.1')
```

_Last validated n/a._

## espn_cricket_position

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/positions/1](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  | position_id path parameter. |

### Returns {#espn_cricket_position-returns}

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

### Example {#espn_cricket_position-example}

```python
espn_cricket_position(league='eng.1', position_id='1')
```

_Last validated n/a._

## espn_cricket_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/tournaments?limit=200](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/tournaments?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cricket_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_tournaments-example}

```python
espn_cricket_tournaments(league='eng.1')
```

_Last validated n/a._

## espn_cricket_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/awards?limit=200](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/awards?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cricket_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_awards-example}

```python
espn_cricket_awards(league='eng.1')
```

_Last validated n/a._

## espn_cricket_award

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/awards/1](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |

### Returns {#espn_cricket_award-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `count` | integer |  |
| `items` | character |  |
| `page_count` | integer |  |
| `page_index` | integer |  |
| `page_size` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_award-example}

```python
espn_cricket_award(league='eng.1', award_id='1')
```

_Last validated n/a._

## espn_cricket_standings_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/standings](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cricket_standings_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character | Conference/group/table the row belongs to, flattened from the standings children hierarchy. |
| `team` | character | Display name of the team in this standings row. |
| `team_id` | character | ESPN numeric identifier for the team. |
| `team_abbreviation` | character | Team abbreviation. |
| `rank` | integer | Position within the group/table. |
| `matches_played` | integer | Matches played (cricket). |
| `matches_won` | integer | Matches won (cricket). |
| `matches_lost` | integer | Matches lost (cricket). |
| `noresult` | integer | Matches with no result (cricket). |
| `match_points` | integer | Competition points (cricket). |
| `qualified` | integer | Qualification flag (cricket). |
| `netrr` | double | Net run rate (cricket). |
| `for` | double | Runs/goals for. |
| `against` | double | Runs/goals against. |
| `total` | character | Aggregate/summary value as published by ESPN. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_standings_core-example}

```python
espn_cricket_standings_core(league='eng.1')
```

_Last validated n/a._

## espn_cricket_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/leaders](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cricket_leaders_core-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, 2026-10-07); its rows sit under keys it does not read (top level: $ref, abbreviation, categories, id, name, type).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_leaders_core-example}

```python
espn_cricket_leaders_core(league='eng.1')
```

_Last validated n/a._

## espn_cricket_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/notes](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cricket_league_notes-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_league_notes-example}

```python
espn_cricket_league_notes(league='eng.1')
```

_Last validated n/a._

## espn_cricket_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/talentpicks](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cricket_talentpicks-returns}

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

### Example {#espn_cricket_talentpicks-example}

```python
espn_cricket_talentpicks(league='eng.1')
```

_Last validated n/a._
