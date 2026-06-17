---
title: UFL — ESPN core API (v2)
sidebar_label: ESPN core API (v2)
sidebar_position: 22
---
# UFL — ESPN core API (v2)

`sportsdataverse.ufl` — 81 endpoints.

## `espn_ufl_league_root`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_league_root()
```

_Last validated n/a._

## `espn_ufl_season_pointer`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/season`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/season](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_pointer()
```

_Last validated n/a._

## `espn_ufl_seasons`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_seasons()
```

_Last validated n/a._

## `espn_ufl_season_info`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_info(season=2024)
```

_Last validated n/a._

## `espn_ufl_season_types`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/types`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_types(season=2024)
```

_Last validated n/a._

## `espn_ufl_season_type`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/types/{season_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `season_type` | `season_type` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_type(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_ufl_season_group`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/types/{season_type}/groups/{group_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/groups/80](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/groups/80)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `season_type` | `season_type` |  | `Y` |  |  |
| `group_id` | `group_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_group(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## `espn_ufl_season_groups`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/types/{season_type}/groups`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/groups](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `season_type` | `season_type` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_groups(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_ufl_season_group_teams`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/types/{season_type}/groups/{group_id}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/groups/80/teams](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/groups/80/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `season_type` | `season_type` |  | `Y` |  |  |
| `group_id` | `group_id` |  | `Y` |  |  |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_group_teams(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## `espn_ufl_season_group_children`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/types/{season_type}/groups/{group_id}/children`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/groups/80/children](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/groups/80/children)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `season_type` | `season_type` |  | `Y` |  |  |
| `group_id` | `group_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_group_children(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## `espn_ufl_season_type_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/types/{season_type}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/leaders](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `season_type` | `season_type` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_type_leaders(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_ufl_season_type_corrections`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/types/{season_type}/corrections`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/corrections](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/corrections)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `season_type` | `season_type` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_type_corrections(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_ufl_season_weeks`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/types/{season_type}/weeks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/weeks](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/weeks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `season_type` | `season_type` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_weeks(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_ufl_season_week`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/types/{season_type}/weeks/{week}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/weeks/1](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/weeks/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `season_type` | `season_type` |  | `Y` |  |  |
| `week` | `week` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_week(season=2024, season_type=2, week=1)
```

_Last validated n/a._

## `espn_ufl_season_week_games`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/types/{season_type}/weeks/{week}/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/weeks/1/events](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/types/2/weeks/1/events)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `season_type` | `season_type` |  | `Y` |  |  |
| `week` | `week` |  | `Y` |  |  |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_week_games(season=2024, season_type=2, week=1)
```

_Last validated n/a._

## `espn_ufl_season_teams`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/teams](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_teams(season=2024)
```

_Last validated n/a._

## `espn_ufl_season_team`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/teams/4](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `team_id` | `team_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_team(season=2024, team_id='4')
```

_Last validated n/a._

## `espn_ufl_season_players`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/athletes](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/athletes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_players(season=2024)
```

_Last validated n/a._

## `espn_ufl_season_coaches`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/coaches`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/coaches](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/coaches)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_coaches`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_coaches(season=2024)
```

_Last validated n/a._

## `espn_ufl_season_draft`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/draft`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/draft](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_draft`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_draft(season=2024)
```

_Last validated n/a._

## `espn_ufl_season_draft_round_picks`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/draft/rounds/{round_num}/picks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/draft/rounds/1/picks](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/draft/rounds/1/picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `round_num` | `round_num` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_draft_round_picks(season=2024, round_num='1')
```

_Last validated n/a._

## `espn_ufl_season_futures`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/futures`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/futures](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/futures)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_futures(season=2024)
```

_Last validated n/a._

## `espn_ufl_season_freeagents`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/freeagents`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/freeagents](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/freeagents)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_freeagents(season=2024)
```

_Last validated n/a._

## `espn_ufl_season_powerindex`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/powerindex[/{team_id}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/powerindex](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/powerindex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |
| `team_id` | `team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_powerindex(season=2024)
```

_Last validated n/a._

## `espn_ufl_season_powerindex_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/powerindex/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/powerindex/leaders](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/powerindex/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_powerindex_leaders(season=2024)
```

_Last validated n/a._

## `espn_ufl_season_awards`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/{season}/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/awards](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/seasons/2024/awards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_season_awards(season=2024)
```

_Last validated n/a._

## `espn_ufl_players_index`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `active` | `active` |  |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_players_index()
```

_Last validated n/a._

## `espn_ufl_player_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/{athlete_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_player_core(athlete_id='4239')
```

_Last validated n/a._

## `espn_ufl_player_career_stats`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/{athlete_id}/statistics[/{stat_type}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/statistics](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  |  |
| `stat_type` | `stat_type` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_player_career_stats(athlete_id='4239')
```

_Last validated n/a._

## `espn_ufl_player_statisticslog`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/{athlete_id}/statisticslog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/statisticslog](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/statisticslog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_player_statisticslog(athlete_id='4239')
```

_Last validated n/a._

## `espn_ufl_player_eventlog`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/{athlete_id}/eventlog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/eventlog](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/eventlog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_player_eventlog(athlete_id='4239')
```

_Last validated n/a._

## `espn_ufl_player_contracts`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/{athlete_id}/contracts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/contracts](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/contracts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_player_contracts(athlete_id='4239')
```

_Last validated n/a._

## `espn_ufl_player_awards`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/{athlete_id}/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/awards](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/awards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_player_awards(athlete_id='4239')
```

_Last validated n/a._

## `espn_ufl_player_seasons`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/{athlete_id}/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/seasons](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_player_seasons(athlete_id='4239')
```

_Last validated n/a._

## `espn_ufl_player_records`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/{athlete_id}/records`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/records](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_player_records(athlete_id='4239')
```

_Last validated n/a._

## `espn_ufl_player_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/{athlete_id}/injuries`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/injuries](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_injuries`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_player_injuries(athlete_id='4239')
```

_Last validated n/a._

## `espn_ufl_player_notes`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/{athlete_id}/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/notes](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_player_notes(athlete_id='4239')
```

_Last validated n/a._

## `espn_ufl_player_vs_player`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/{athlete_id}/vsathlete/{opp_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/vsathlete/5](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/athletes/4239/vsathlete/5)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  |  |
| `opp_id` | `opp_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_player_vs_player(athlete_id='4239', opp_id='5')
```

_Last validated n/a._

## `espn_ufl_games`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_games()
```

_Last validated n/a._

## `espn_ufl_game`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_competition`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_competition(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_teams`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/competitors`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_teams(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_team`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/competitors/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `team_id` | `team_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_team(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_ufl_game_team_roster`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/competitors/{team_id}/roster`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `team_id` | `team_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_event_competitor_roster`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_team_roster(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_ufl_game_team_linescores`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `team_id` | `team_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_event_competitor_linescores`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_team_linescores(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_ufl_game_team_statistics`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `team_id` | `team_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_event_competitor_statistics`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_team_statistics(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_ufl_game_team_record`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/competitors/{team_id}/record`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `team_id` | `team_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_team_record(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_ufl_game_team_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/competitors/{team_id}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `team_id` | `team_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_team_leaders(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_ufl_game_odds`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/odds`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_odds(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_probabilities`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/probabilities`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_probabilities(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_plays`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/plays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_event_plays`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_plays(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_play`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/plays/{play_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `play_id` | `play_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_play(event_id='401584793', play_id='1')
```

_Last validated n/a._

## `espn_ufl_game_play_personnel`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/plays/{play_id}/personnel`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `play_id` | `play_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_play_personnel(event_id='401584793', play_id='1')
```

_Last validated n/a._

## `espn_ufl_game_situation`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/situation`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_situation(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_status`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/status`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_status(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_officials`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/officials`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_officials(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_broadcasts`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/broadcasts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_broadcasts(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_predictor`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/predictor`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_predictor(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_powerindex`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/powerindex`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_powerindex(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_propbets`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/propbets`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_propbets(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_leaders(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_scoringplays`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/scoringplays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_scoringplays(event_id='401584793')
```

_Last validated n/a._

## `espn_ufl_game_official_detail`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/{event_id}/competitions/{cid}/officials/{official_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  |  |
| `official_id` | `official_id` |  | `Y` |  |  |
| `cid` | `cid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_game_official_detail(event_id='401584793', official_id='1')
```

_Last validated n/a._

## `espn_ufl_teams_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/teams](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

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

### Example

```python
espn_ufl_teams_core()
```

_Last validated n/a._

## `espn_ufl_team_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/teams/4](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_team_core(team_id='4')
```

_Last validated n/a._

## `espn_ufl_venues`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/venues](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/venues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_venues()
```

_Last validated n/a._

## `espn_ufl_venue`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/venues/3663](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_venue(venue_id='3663')
```

_Last validated n/a._

## `espn_ufl_franchises`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/franchises](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/franchises)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_franchises()
```

_Last validated n/a._

## `espn_ufl_franchise`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/franchises/2](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_franchise(franchise_id='2')
```

_Last validated n/a._

## `espn_ufl_coach`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/coaches/1](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_coach(coach_id='1')
```

_Last validated n/a._

## `espn_ufl_coach_record`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/coaches/1/record](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/coaches/1/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  |  |
| `record_type` | `record_type` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_coach_record(coach_id='1')
```

_Last validated n/a._

## `espn_ufl_coach_season`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  |  |
| `season` | `season` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

## `espn_ufl_positions`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/positions](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/positions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_positions()
```

_Last validated n/a._

## `espn_ufl_position`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/positions/1](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_position(position_id='1')
```

_Last validated n/a._

## `espn_ufl_tournaments`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/tournaments](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/tournaments)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_tournaments()
```

_Last validated n/a._

## `espn_ufl_awards`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/awards](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/awards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_awards()
```

_Last validated n/a._

## `espn_ufl_award`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/awards/1](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_award(award_id='1')
```

_Last validated n/a._

## `espn_ufl_standings_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/standings](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

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
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `division_win_percent` | double | Division win percent. |
| `games_behind` | double | Games behind. |
| `league_win_percent` | double | League win percent. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points` | double | Points. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `win_percent` | double | Win percent. |
| `wins` | double | Wins. |
| `games_ahead` | double | Games ahead. |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `vs. div.` | character | Vs. div.. |
| `vs. conf.` | character | Vs. conf.. |
| `last ten games` | character | Last ten games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_standings_core()
```

_Last validated n/a._

## `espn_ufl_leaders_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/leaders](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_leaders_core()
```

_Last validated n/a._

## `espn_ufl_league_notes`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/notes](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_league_notes()
```

_Last validated n/a._

## `espn_ufl_talentpicks`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/talentpicks](https://sports.core.api.espn.com/v2/sports/football/leagues/ufl/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ufl_talentpicks()
```

_Last validated n/a._
