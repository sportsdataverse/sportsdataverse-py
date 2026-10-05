---
title: "XFL — ESPN core API (v2) — Season"
sidebar_label: "Season"
sidebar_position: 3
description: "XFL — ESPN core API (v2) — Season — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# XFL — ESPN core API (v2) — Season

## espn_xfl_season_pointer

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/season`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/season](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_xfl_season_pointer-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_pointer-example}

```python
espn_xfl_season_pointer()
```

_Last validated n/a._

## espn_xfl_season_info

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_xfl_season_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_info-example}

```python
espn_xfl_season_info(season=2024)
```

_Last validated n/a._

## espn_xfl_season_types

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/types`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_xfl_season_types-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_types-example}

```python
espn_xfl_season_types(season=2024)
```

_Last validated n/a._

## espn_xfl_season_type

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/types/{season_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_xfl_season_type-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_type-example}

```python
espn_xfl_season_type(season=2024, season_type=2)
```

_Last validated n/a._

## espn_xfl_season_group

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/types/{season_type}/groups/{group_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/groups/80](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/groups/80)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `group_id` | `group_id` |  | `Y` |  | group_id path parameter. |

### Returns {#espn_xfl_season_group-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_group-example}

```python
espn_xfl_season_group(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## espn_xfl_season_groups

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/types/{season_type}/groups`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/groups](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_xfl_season_groups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_groups-example}

```python
espn_xfl_season_groups(season=2024, season_type=2)
```

_Last validated n/a._

## espn_xfl_season_group_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/types/{season_type}/groups/{group_id}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/groups/80/teams?limit=500](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/groups/80/teams?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `group_id` | `group_id` |  | `Y` |  | group_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_xfl_season_group_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_group_teams-example}

```python
espn_xfl_season_group_teams(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## espn_xfl_season_group_children

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/types/{season_type}/groups/{group_id}/children`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/groups/80/children?limit=500](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/groups/80/children?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `group_id` | `group_id` |  | `Y` |  | group_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_xfl_season_group_children-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_group_children-example}

```python
espn_xfl_season_group_children(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## espn_xfl_season_type_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/types/{season_type}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/leaders](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_xfl_season_type_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_type_leaders-example}

```python
espn_xfl_season_type_leaders(season=2024, season_type=2)
```

_Last validated n/a._

## espn_xfl_season_type_corrections

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/types/{season_type}/corrections`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/corrections](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/corrections)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_xfl_season_type_corrections-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_type_corrections-example}

```python
espn_xfl_season_type_corrections(season=2024, season_type=2)
```

_Last validated n/a._

## espn_xfl_season_weeks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/types/{season_type}/weeks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/weeks](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/weeks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_xfl_season_weeks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_weeks-example}

```python
espn_xfl_season_weeks(season=2024, season_type=2)
```

_Last validated n/a._

## espn_xfl_season_week

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/types/{season_type}/weeks/{week}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/weeks/1](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/weeks/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `week` | `week` |  | `Y` |  | week path parameter. |

### Returns {#espn_xfl_season_week-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_week-example}

```python
espn_xfl_season_week(season=2024, season_type=2, week=1)
```

_Last validated n/a._

## espn_xfl_season_week_powerindex

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/types/{season_type}/weeks/{week}/powerindex`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/weeks/8/powerindex](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/weeks/8/powerindex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `week` | `week` |  | `Y` |  | week path parameter. |
| `limit` | `limit` |  |  | `Y` | Page size for this weekly power-index table; pass a limit large enough to avoid paging (table size varies by sport/league -- CFB's FBS table alone is ~134 rows). |

### Returns {#espn_xfl_season_week_powerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_weekly_powerindex`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_week_powerindex-example}

```python
espn_xfl_season_week_powerindex(season=2024, season_type=2, week=8)
```

_Last validated n/a._

## espn_xfl_season_week_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/types/{season_type}/weeks/{week}/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/weeks/1/events?limit=500](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/types/2/weeks/1/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `week` | `week` |  | `Y` |  | week path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_xfl_season_week_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_week_games-example}

```python
espn_xfl_season_week_games(season=2024, season_type=2, week=1)
```

_Last validated n/a._

## espn_xfl_season_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_xfl_season_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_teams-example}

```python
espn_xfl_season_teams(season=2024)
```

_Last validated n/a._

## espn_xfl_season_team

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/teams/4](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_xfl_season_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_team-example}

```python
espn_xfl_season_team(season=2024, team_id='4')
```

_Last validated n/a._

## espn_xfl_season_players

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/athletes?limit=100&page=1](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/athletes?limit=100&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_xfl_season_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_players-example}

```python
espn_xfl_season_players(season=2024)
```

_Last validated n/a._

## espn_xfl_season_coaches

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/coaches`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/coaches?limit=500](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/coaches?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_xfl_season_coaches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_coaches`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_coaches-example}

```python
espn_xfl_season_coaches(season=2024)
```

_Last validated n/a._

## espn_xfl_season_draft

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/draft`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/draft](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_xfl_season_draft-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_draft`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_draft-example}

```python
espn_xfl_season_draft(season=2024)
```

_Last validated n/a._

## espn_xfl_season_draft_round_picks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/draft/rounds/{round_num}/picks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/draft/rounds/1/picks](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/draft/rounds/1/picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `round_num` | `round_num` |  | `Y` |  | round_num path parameter. |

### Returns {#espn_xfl_season_draft_round_picks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_draft_round_picks-example}

```python
espn_xfl_season_draft_round_picks(season=2024, round_num='1')
```

_Last validated n/a._

## espn_xfl_season_futures

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/futures`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/futures](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/futures)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_xfl_season_futures-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_futures-example}

```python
espn_xfl_season_futures(season=2024)
```

_Last validated n/a._

## espn_xfl_season_freeagents

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/freeagents`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/freeagents](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/freeagents)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_xfl_season_freeagents-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_freeagents-example}

```python
espn_xfl_season_freeagents(season=2024)
```

_Last validated n/a._

## espn_xfl_season_powerindex

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/powerindex[/{team_id}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/powerindex](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/powerindex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `team_id` | `team_id` |  |  | `Y` | team_id path parameter. |

### Returns {#espn_xfl_season_powerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_powerindex-example}

```python
espn_xfl_season_powerindex(season=2024)
```

_Last validated n/a._

## espn_xfl_season_powerindex_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/powerindex/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/powerindex/leaders](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/powerindex/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_xfl_season_powerindex_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_powerindex_leaders-example}

```python
espn_xfl_season_powerindex_leaders(season=2024)
```

_Last validated n/a._

## espn_xfl_season_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/{season}/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/awards?limit=200](https://sports.core.api.espn.com/v2/sports/football/leagues/xfl/seasons/2024/awards?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_xfl_season_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_xfl_season_awards-example}

```python
espn_xfl_season_awards(season=2024)
```

_Last validated n/a._
