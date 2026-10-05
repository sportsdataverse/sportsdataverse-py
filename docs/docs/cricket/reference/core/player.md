---
title: "CRICKET — ESPN core API (v2) — Player"
sidebar_label: "Player"
sidebar_position: 2
description: "CRICKET — ESPN core API (v2) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CRICKET — ESPN core API (v2) — Player

## espn_cricket_players_index

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes?active=true&limit=100&page=1](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes?active=true&limit=100&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `active` | `active` |  |  | `Y` | active query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_cricket_players_index-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_players_index-example}

```python
espn_cricket_players_index(league='eng.1')
```

_Last validated n/a._

## espn_cricket_player_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/{athlete_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cricket_player_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_player_core-example}

```python
espn_cricket_player_core(league='eng.1', athlete_id='4239')
```

_Last validated n/a._

## espn_cricket_player_career_stats

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/{athlete_id}/statistics[/{stat_type}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/statistics](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `stat_type` | `stat_type` |  |  | `Y` | stat_type path parameter. |

### Returns {#espn_cricket_player_career_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_player_career_stats-example}

```python
espn_cricket_player_career_stats(league='eng.1', athlete_id='4239')
```

_Last validated n/a._

## espn_cricket_player_statisticslog

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/{athlete_id}/statisticslog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/statisticslog](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/statisticslog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cricket_player_statisticslog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_player_statisticslog-example}

```python
espn_cricket_player_statisticslog(league='eng.1', athlete_id='4239')
```

_Last validated n/a._

## espn_cricket_player_eventlog

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/{athlete_id}/eventlog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/eventlog](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/eventlog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cricket_player_eventlog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_player_eventlog-example}

```python
espn_cricket_player_eventlog(league='eng.1', athlete_id='4239')
```

_Last validated n/a._

## espn_cricket_player_contracts

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/{athlete_id}/contracts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/contracts](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/contracts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cricket_player_contracts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_player_contracts-example}

```python
espn_cricket_player_contracts(league='eng.1', athlete_id='4239')
```

_Last validated n/a._

## espn_cricket_player_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/{athlete_id}/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/awards](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/awards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cricket_player_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_player_awards-example}

```python
espn_cricket_player_awards(league='eng.1', athlete_id='4239')
```

_Last validated n/a._

## espn_cricket_player_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/{athlete_id}/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/seasons](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cricket_player_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_player_seasons-example}

```python
espn_cricket_player_seasons(league='eng.1', athlete_id='4239')
```

_Last validated n/a._

## espn_cricket_player_records

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/{athlete_id}/records`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/records](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cricket_player_records-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_player_records-example}

```python
espn_cricket_player_records(league='eng.1', athlete_id='4239')
```

_Last validated n/a._

## espn_cricket_player_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/{athlete_id}/injuries`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/injuries](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cricket_player_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric identifier for the athlete. |
| `display_name` | character | Athlete's full display name as shown on ESPN. |
| `injuries` | character | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_player_injuries-example}

```python
espn_cricket_player_injuries(league='eng.1', athlete_id='4239')
```

_Last validated n/a._

## espn_cricket_player_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/{athlete_id}/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/notes](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cricket_player_notes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_player_notes-example}

```python
espn_cricket_player_notes(league='eng.1', athlete_id='4239')
```

_Last validated n/a._

## espn_cricket_player_vs_player

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/{athlete_id}/vsathlete/{opp_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/vsathlete/5](https://sports.core.api.espn.com/v2/sports/cricket/leagues/eng.1/athletes/4239/vsathlete/5)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `opp_id` | `opp_id` |  | `Y` |  | opp_id path parameter. |

### Returns {#espn_cricket_player_vs_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cricket_player_vs_player-example}

```python
espn_cricket_player_vs_player(league='eng.1', athlete_id='4239', opp_id='5')
```

_Last validated n/a._
