---
title: "NBA — ESPN core API (v2) — Player"
sidebar_label: "Player"
sidebar_position: 2
description: "NBA — ESPN core API (v2) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — ESPN core API (v2) — Player

## espn_nba_players_index

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes?active=true&limit=100&page=1](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes?active=true&limit=100&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `active` | `active` |  |  | `Y` | active query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_nba_players_index-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_players_index-example}

```python
espn_nba_players_index()
```

_Last validated n/a._

## espn_nba_player_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/{athlete_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nba_player_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `active` | logical | TRUE if the row represents an active record (player / team / season). |
| `age` | integer | Player age (in years). |
| `date_of_birth` | character | Date of birth (YYYY-MM-DD). |
| `debut_year` | integer | Year of professional debut. |
| `display_height` | character | Player height in display format (e.g. '6-2'). |
| `display_name` | character | Display name. |
| `display_weight` | character | Player weight in display format (e.g. '180 lbs'). |
| `first_name` | character | Player's first name. |
| `full_name` | character | Player's full name. |
| `guid` | character | Stable cross-league team GUID. |
| `height` | double | Player height (string e.g. '6-2' or inches). |
| `id` | character | Id. |
| `jersey` | character | Jersey number worn by the player. |
| `last_name` | character | Player's last name. |
| `linked` | logical | TRUE if the record is linked to a related entity. |
| `links` | character |  |
| `short_name` | character | Short display name. |
| `slug` | character | URL-safe identifier. |
| `type` | character | Record type / category. |
| `uid` | character | ESPN UID string. |
| `weight` | double | Player weight in pounds. |
| `alternate_ids_sdr` | character |  |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |
| `college_$ref` | character |  |
| `college_athlete_$ref` | character |  |
| `contracts_$ref` | character |  |
| `draft_display_text` | character | Draft display text. |
| `draft_pick_$ref` | character |  |
| `draft_round` | integer | Round of the draft selection. |
| `draft_selection` | integer | Draft selection. |
| `draft_team_$ref` | character |  |
| `draft_year` | integer | Draft year (4-digit). |
| `experience_years` | integer | Experience years. |
| `hand_abbreviation` | character | Hand abbreviation. |
| `hand_display_value` | character | Hand display value. |
| `hand_type` | character | Hand type. |
| `headshot_alt` | character | Alternative-text label for the headshot. |
| `headshot_href` | character | Headshot image URL. |
| `position_$ref` | character |  |
| `position_abbreviation` | character | Position abbreviation ('G' / 'F' / 'C'). |
| `position_display_name` | character | Position display name. |
| `position_id` | character | Unique position identifier. |
| `position_leaf` | logical | Position leaf. |
| `position_name` | character | Listed roster position ('Guard', 'Forward', 'Center'). |
| `seasons_$ref` | character |  |
| `statistics_$ref` | character |  |
| `statisticslog_$ref` | character |  |
| `status_abbreviation` | character | Status abbreviation. |
| `status_id` | character | Status identifier. |
| `status_name` | character | Status label. |
| `status_type` | character | Status type. |
| `team_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_player_core-example}

```python
espn_nba_player_core(athlete_id='4239')
```

_Last validated n/a._

## espn_nba_player_career_stats

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/{athlete_id}/statistics[/{stat_type}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/statistics](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `stat_type` | `stat_type` |  |  | `Y` | stat_type path parameter. |

### Returns {#espn_nba_player_career_stats-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, mlb, mbb, 2026-10-07); its rows sit under keys it does not read (top level: $ref, athlete, splits).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_player_career_stats-example}

```python
espn_nba_player_career_stats(athlete_id='4239')
```

_Last validated n/a._

## espn_nba_player_statisticslog

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/{athlete_id}/statisticslog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/statisticslog](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/statisticslog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nba_player_statisticslog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `statistics` | character |  |
| `season_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_player_statisticslog-example}

```python
espn_nba_player_statisticslog(athlete_id='4239')
```

_Last validated n/a._

## espn_nba_player_eventlog

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/{athlete_id}/eventlog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/eventlog](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/eventlog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nba_player_eventlog-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_player_eventlog-example}

```python
espn_nba_player_eventlog(athlete_id='4239')
```

_Last validated n/a._

## espn_nba_player_contracts

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/{athlete_id}/contracts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/contracts](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/contracts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nba_player_contracts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_player_contracts-example}

```python
espn_nba_player_contracts(athlete_id='4239')
```

_Last validated n/a._

## espn_nba_player_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/{athlete_id}/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/awards](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/awards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nba_player_awards-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_player_awards-example}

```python
espn_nba_player_awards(athlete_id='4239')
```

_Last validated n/a._

## espn_nba_player_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/{athlete_id}/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/seasons](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nba_player_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_player_seasons-example}

```python
espn_nba_player_seasons(athlete_id='4239')
```

_Last validated n/a._

## espn_nba_player_records

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/{athlete_id}/records`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/records](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nba_player_records-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_player_records-example}

```python
espn_nba_player_records(athlete_id='4239')
```

_Last validated n/a._

## espn_nba_player_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/{athlete_id}/injuries`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/injuries](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nba_player_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric identifier for the athlete. |
| `display_name` | character | Athlete's full display name as shown on ESPN. |
| `injuries` | character | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_player_injuries-example}

```python
espn_nba_player_injuries(athlete_id='4239')
```

_Last validated n/a._

## espn_nba_player_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/{athlete_id}/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/notes](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nba_player_notes-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_player_notes-example}

```python
espn_nba_player_notes(athlete_id='4239')
```

_Last validated n/a._

## espn_nba_player_vs_player

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/{athlete_id}/vsathlete/{opp_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/vsathlete/5](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/vsathlete/5)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `opp_id` | `opp_id` |  | `Y` |  | opp_id path parameter. |

### Returns {#espn_nba_player_vs_player-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_player_vs_player-example}

```python
espn_nba_player_vs_player(athlete_id='4239', opp_id='5')
```

_Last validated n/a._
