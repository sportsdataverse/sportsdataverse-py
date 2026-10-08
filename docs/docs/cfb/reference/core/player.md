---
title: "CFB — ESPN core API (v2) — Player"
sidebar_label: "Player"
sidebar_position: 2
description: "CFB — ESPN core API (v2) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — ESPN core API (v2) — Player

## espn_cfb_players_index

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes?active=true&limit=100&page=1](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes?active=true&limit=100&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `active` | `active` |  |  | `Y` | active query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_cfb_players_index-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_players_index-example}

```python
espn_cfb_players_index()
```

_Last validated n/a._

## espn_cfb_player_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cfb_player_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `active` | logical | `TRUE` if the player was active for the game. |
| `age` | integer |  |
| `date_of_birth` | character | Player date of birth (if published). |
| `debut_year` | integer |  |
| `display_height` | character | Human-readable height (e.g. `6' 1"`). |
| `display_name` | character | Human-readable metric name. |
| `display_weight` | character | Human-readable weight (e.g. `205 lbs`). |
| `first_name` | character | Athlete first name. |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `guid` | character | ESPN athlete GUID. |
| `height` | double | Listed height (inches). |
| `id` | character | 247Sports referencing id for the recruit. |
| `jersey` | character | Jersey number. |
| `last_name` | character | Athlete last name. |
| `linked` | logical |  |
| `links` | character |  |
| `short_name` | character | Ranking source short name (e.g. `AP Poll`). |
| `slug` | character | URL slug for the team. |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `uid` | character | ESPN global unique identifier. |
| `weight` | double | Listed weight (lbs). |
| `alternate_ids_sdr` | character |  |
| `birth_place_city` | character |  |
| `birth_place_state` | character |  |
| `college_$ref` | character |  |
| `college_athlete_$ref` | character |  |
| `contracts_$ref` | character |  |
| `draft_display_text` | character |  |
| `draft_pick_$ref` | character |  |
| `draft_round` | integer |  |
| `draft_selection` | integer |  |
| `draft_team_$ref` | character |  |
| `draft_year` | integer |  |
| `experience_years` | integer | Years of experience. |
| `hand_abbreviation` | character |  |
| `hand_display_value` | character |  |
| `hand_type` | character |  |
| `headshot_alt` | character |  |
| `headshot_href` | character | URL of the athlete headshot image. |
| `position_$ref` | character |  |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`). |
| `position_display_name` | character | Human-readable position name. |
| `position_id` | character | ESPN position id. |
| `position_leaf` | logical | `TRUE` for a most-specific (leaf) position. |
| `position_name` | character | Position name (e.g. `Quarterback`). |
| `seasons_$ref` | character |  |
| `statistics_$ref` | character |  |
| `statisticslog_$ref` | character |  |
| `status_abbreviation` | character |  |
| `status_id` | character | ESPN commitment status id. |
| `status_name` | character | Status-type key (e.g. `STATUS_FINAL`). |
| `status_type` | character | Status type. |
| `team_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_player_core-example}

```python
espn_cfb_player_core(athlete_id='4239')
```

_Last validated n/a._

## espn_cfb_player_career_stats

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/statistics[/{stat_type}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/statistics](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `stat_type` | `stat_type` |  |  | `Y` | stat_type path parameter. |

### Returns {#espn_cfb_player_career_stats-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, mlb, mbb, 2026-10-07); its rows sit under keys it does not read (top level: $ref, athlete, splits).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_player_career_stats-example}

```python
espn_cfb_player_career_stats(athlete_id='4239')
```

_Last validated n/a._

## espn_cfb_player_statisticslog

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/statisticslog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/statisticslog](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/statisticslog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cfb_player_statisticslog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `statistics` | character |  |
| `season_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_player_statisticslog-example}

```python
espn_cfb_player_statisticslog(athlete_id='4239')
```

_Last validated n/a._

## espn_cfb_player_eventlog

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/eventlog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/eventlog](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/eventlog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cfb_player_eventlog-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_player_eventlog-example}

```python
espn_cfb_player_eventlog(athlete_id='4239')
```

_Last validated n/a._

## espn_cfb_player_contracts

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/contracts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/contracts](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/contracts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cfb_player_contracts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_player_contracts-example}

```python
espn_cfb_player_contracts(athlete_id='4239')
```

_Last validated n/a._

## espn_cfb_player_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/awards](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/awards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cfb_player_awards-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_player_awards-example}

```python
espn_cfb_player_awards(athlete_id='4239')
```

_Last validated n/a._

## espn_cfb_player_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/seasons](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cfb_player_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_player_seasons-example}

```python
espn_cfb_player_seasons(athlete_id='4239')
```

_Last validated n/a._

## espn_cfb_player_records

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/records`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/records](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cfb_player_records-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_player_records-example}

```python
espn_cfb_player_records(athlete_id='4239')
```

_Last validated n/a._

## espn_cfb_player_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/injuries`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/injuries](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cfb_player_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric identifier for the athlete. |
| `display_name` | character | Athlete's full display name as shown on ESPN. |
| `injuries` | character | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_player_injuries-example}

```python
espn_cfb_player_injuries(athlete_id='4239')
```

_Last validated n/a._

## espn_cfb_player_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/notes](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cfb_player_notes-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_player_notes-example}

```python
espn_cfb_player_notes(athlete_id='4239')
```

_Last validated n/a._

## espn_cfb_player_vs_player

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/vsathlete/{opp_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/vsathlete/5](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/vsathlete/5)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `opp_id` | `opp_id` |  | `Y` |  | opp_id path parameter. |

### Returns {#espn_cfb_player_vs_player-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_player_vs_player-example}

```python
espn_cfb_player_vs_player(athlete_id='4239', opp_id='5')
```

_Last validated n/a._
