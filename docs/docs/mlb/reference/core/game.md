---
title: "MLB — ESPN core API (v2) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "MLB — ESPN core API (v2) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — ESPN core API (v2) — Game

## espn_mlb_game_competition

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_competition-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_competition-example}

```python
espn_mlb_game_competition(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_teams-example}

```python
espn_mlb_game_teams(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_team

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_team-example}

```python
espn_mlb_game_team(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_mlb_game_team_roster

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors/{team_id}/roster`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_event_competitor_roster`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_team_roster-example}

```python
espn_mlb_game_team_roster(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_mlb_game_team_linescores

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_team_linescores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_event_competitor_linescores`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_team_linescores-example}

```python
espn_mlb_game_team_linescores(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_mlb_game_team_statistics

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_team_statistics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_event_competitor_statistics`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_team_statistics-example}

```python
espn_mlb_game_team_statistics(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_mlb_game_team_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors/{team_id}/record`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_team_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_team_record-example}

```python
espn_mlb_game_team_record(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_mlb_game_team_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors/{team_id}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_team_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_team_leaders-example}

```python
espn_mlb_game_team_leaders(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_mlb_game_odds

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/odds`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_odds-example}

```python
espn_mlb_game_odds(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_probabilities

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/probabilities`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_game_probabilities-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_probabilities-example}

```python
espn_mlb_game_probabilities(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_plays

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/plays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_game_plays-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_event_plays`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_plays-example}

```python
espn_mlb_game_plays(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_play

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/plays/{play_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `play_id` | `play_id` |  | `Y` |  | play_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_play-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_play-example}

```python
espn_mlb_game_play(event_id='401584793', play_id='1')
```

_Last validated n/a._

## espn_mlb_game_play_personnel

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/plays/{play_id}/personnel`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `play_id` | `play_id` |  | `Y` |  | play_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_play_personnel-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_play_personnel-example}

```python
espn_mlb_game_play_personnel(event_id='401584793', play_id='1')
```

_Last validated n/a._

## espn_mlb_game_situation

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/situation`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_situation-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_situation-example}

```python
espn_mlb_game_situation(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_status

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/status`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_status-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_status-example}

```python
espn_mlb_game_status(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_officials

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/officials`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_officials-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_officials-example}

```python
espn_mlb_game_officials(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_broadcasts

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/broadcasts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_broadcasts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_broadcasts-example}

```python
espn_mlb_game_broadcasts(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_predictor

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/predictor`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_predictor-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_predictor-example}

```python
espn_mlb_game_predictor(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_powerindex

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/powerindex`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_powerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_powerindex-example}

```python
espn_mlb_game_powerindex(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_propbets

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/propbets`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_propbets-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_propbets-example}

```python
espn_mlb_game_propbets(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_leaders-example}

```python
espn_mlb_game_leaders(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_scoringplays

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/scoringplays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_scoringplays-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_scoringplays-example}

```python
espn_mlb_game_scoringplays(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_game_official_detail

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/officials/{official_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `official_id` | `official_id` |  | `Y` |  | official_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_official_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game_official_detail-example}

```python
espn_mlb_game_official_detail(event_id='401584793', official_id='1')
```

_Last validated n/a._
