---
title: "WWC — ESPN core API (v2) — Season"
sidebar_label: "Season"
sidebar_position: 3
description: "WWC — ESPN core API (v2) — Season — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WWC — ESPN core API (v2) — Season

## espn_wwc_season_pointer

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/season`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/season](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wwc_season_pointer-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_name` | character |  |
| `end_date` | character |  |
| `start_date` | character |  |
| `year` | integer |  |
| `athletes_$ref` | character |  |
| `coaches_$ref` | character |  |
| `futures_$ref` | character |  |
| `power_index_leaders_$ref` | character |  |
| `power_indexes_$ref` | character |  |
| `rankings_$ref` | character |  |
| `type_$ref` | character |  |
| `type_abbreviation` | character |  |
| `type_corrections_$ref` | character |  |
| `type_end_date` | character |  |
| `type_groups_$ref` | character |  |
| `type_has_groups` | logical |  |
| `type_has_legs` | logical |  |
| `type_has_standings` | logical |  |
| `type_id` | character |  |
| `type_name` | character |  |
| `type_slug` | character |  |
| `type_start_date` | character |  |
| `type_type` | integer |  |
| `type_weeks_$ref` | character |  |
| `type_year` | integer |  |
| `types_$ref` | character |  |
| `types_count` | integer |  |
| `types_items` | character |  |
| `types_page_count` | integer |  |
| `types_page_index` | integer |  |
| `types_page_size` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_pointer-example}

```python
espn_wwc_season_pointer()
```

_Last validated n/a._

## espn_wwc_season_info

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_wwc_season_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_name` | character |  |
| `end_date` | character |  |
| `start_date` | character |  |
| `year` | integer |  |
| `awards_$ref` | character |  |
| `futures_$ref` | character |  |
| `leaders_$ref` | character |  |
| `power_index_leaders_$ref` | character |  |
| `power_indexes_$ref` | character |  |
| `rankings_$ref` | character |  |
| `type_$ref` | character |  |
| `type_abbreviation` | character |  |
| `type_corrections_$ref` | character |  |
| `type_end_date` | character |  |
| `type_groups_$ref` | character |  |
| `type_has_groups` | logical |  |
| `type_has_legs` | logical |  |
| `type_has_standings` | logical |  |
| `type_id` | character |  |
| `type_leaders_$ref` | character |  |
| `type_name` | character |  |
| `type_slug` | character |  |
| `type_start_date` | character |  |
| `type_type` | integer |  |
| `type_weeks_$ref` | character |  |
| `type_year` | integer |  |
| `types_$ref` | character |  |
| `types_count` | integer |  |
| `types_items` | character |  |
| `types_page_count` | integer |  |
| `types_page_index` | integer |  |
| `types_page_size` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_info-example}

```python
espn_wwc_season_info(season=2024)
```

_Last validated n/a._

## espn_wwc_season_types

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/types`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_wwc_season_types-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_types-example}

```python
espn_wwc_season_types(season=2024)
```

_Last validated n/a._

## espn_wwc_season_type

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/types/{season_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_wwc_season_type-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character |  |
| `end_date` | character |  |
| `has_groups` | logical |  |
| `has_legs` | logical |  |
| `has_standings` | logical |  |
| `id` | character |  |
| `name` | character |  |
| `slug` | character |  |
| `start_date` | character |  |
| `type` | integer |  |
| `year` | integer |  |
| `corrections_$ref` | character |  |
| `groups_$ref` | character |  |
| `leaders_$ref` | character |  |
| `weeks_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_type-example}

```python
espn_wwc_season_type(season=2024, season_type=2)
```

_Last validated n/a._

## espn_wwc_season_group

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/types/{season_type}/groups/{group_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/groups/80](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/groups/80)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `group_id` | `group_id` |  | `Y` |  | group_id path parameter. |

### Returns {#espn_wwc_season_group-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `id` | character |  |
| `is_conference` | logical |  |
| `links` | character |  |
| `midsize_name` | character |  |
| `name` | character |  |
| `short_name` | character |  |
| `slug` | character |  |
| `uid` | character |  |
| `children_$ref` | character |  |
| `parent_$ref` | character |  |
| `season_$ref` | character |  |
| `standings_$ref` | character |  |
| `teams_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_group-example}

```python
espn_wwc_season_group(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## espn_wwc_season_groups

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/types/{season_type}/groups`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/groups](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_wwc_season_groups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_groups-example}

```python
espn_wwc_season_groups(season=2024, season_type=2)
```

_Last validated n/a._

## espn_wwc_season_group_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/types/{season_type}/groups/{group_id}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/groups/80/teams?limit=500](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/groups/80/teams?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `group_id` | `group_id` |  | `Y` |  | group_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wwc_season_group_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_group_teams-example}

```python
espn_wwc_season_group_teams(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## espn_wwc_season_group_children

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/types/{season_type}/groups/{group_id}/children`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/groups/80/children?limit=500](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/groups/80/children?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `group_id` | `group_id` |  | `Y` |  | group_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wwc_season_group_children-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_group_children-example}

```python
espn_wwc_season_group_children(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## espn_wwc_season_type_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/types/{season_type}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/leaders](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_wwc_season_type_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_type_leaders-example}

```python
espn_wwc_season_type_leaders(season=2024, season_type=2)
```

_Last validated n/a._

## espn_wwc_season_type_corrections

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/types/{season_type}/corrections`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/corrections](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/corrections)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_wwc_season_type_corrections-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_type_corrections-example}

```python
espn_wwc_season_type_corrections(season=2024, season_type=2)
```

_Last validated n/a._

## espn_wwc_season_weeks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/types/{season_type}/weeks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/weeks](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/weeks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_wwc_season_weeks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_weeks-example}

```python
espn_wwc_season_weeks(season=2024, season_type=2)
```

_Last validated n/a._

## espn_wwc_season_week

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/types/{season_type}/weeks/{week}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/weeks/1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/weeks/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `week` | `week` |  | `Y` |  | week path parameter. |

### Returns {#espn_wwc_season_week-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `end_date` | character |  |
| `number` | integer |  |
| `start_date` | character |  |
| `text` | character |  |
| `rankings_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_week-example}

```python
espn_wwc_season_week(season=2024, season_type=2, week=1)
```

_Last validated n/a._

## espn_wwc_season_week_powerindex

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/types/{season_type}/weeks/{week}/powerindex`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/weeks/8/powerindex](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/weeks/8/powerindex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `week` | `week` |  | `Y` |  | week path parameter. |
| `limit` | `limit` |  |  | `Y` | Page size for this weekly power-index table; pass a limit large enough to avoid paging (table size varies by sport/league -- CFB's FBS table alone is ~134 rows). |

### Returns {#espn_wwc_season_week_powerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_weekly_powerindex`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_week_powerindex-example}

```python
espn_wwc_season_week_powerindex(season=2024, season_type=2, week=8)
```

_Last validated n/a._

## espn_wwc_season_week_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/types/{season_type}/weeks/{week}/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/weeks/1/events?limit=500](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/types/2/weeks/1/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `week` | `week` |  | `Y` |  | week path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wwc_season_week_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_week_games-example}

```python
espn_wwc_season_week_games(season=2024, season_type=2, week=1)
```

_Last validated n/a._

## espn_wwc_season_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_wwc_season_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_teams-example}

```python
espn_wwc_season_teams(season=2024)
```

_Last validated n/a._

## espn_wwc_season_team

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/teams/4](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_wwc_season_team-returns}

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
| `awards_$ref` | character |  |
| `coaches_$ref` | character |  |
| `depth_charts_$ref` | character |  |
| `events_$ref` | character |  |
| `franchise_$ref` | character |  |
| `groups_$ref` | character |  |
| `injuries_$ref` | character |  |
| `leaders_$ref` | character |  |
| `notes_$ref` | character |  |
| `ranks_$ref` | character |  |
| `record_$ref` | character |  |
| `statistics_$ref` | character |  |
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

### Example {#espn_wwc_season_team-example}

```python
espn_wwc_season_team(season=2024, team_id='4')
```

_Last validated n/a._

## espn_wwc_season_players

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/athletes?limit=100&page=1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/athletes?limit=100&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_wwc_season_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_players-example}

```python
espn_wwc_season_players(season=2024)
```

_Last validated n/a._

## espn_wwc_season_coaches

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/coaches`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/coaches?limit=500](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/coaches?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wwc_season_coaches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_coaches-example}

```python
espn_wwc_season_coaches(season=2024)
```

_Last validated n/a._

## espn_wwc_season_draft

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/draft`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/draft](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_wwc_season_draft-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_draft`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_draft-example}

```python
espn_wwc_season_draft(season=2024)
```

_Last validated n/a._

## espn_wwc_season_draft_round_picks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/draft/rounds/{round_num}/picks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/draft/rounds/1/picks](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/draft/rounds/1/picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `round_num` | `round_num` |  | `Y` |  | round_num path parameter. |

### Returns {#espn_wwc_season_draft_round_picks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_draft_round_picks-example}

```python
espn_wwc_season_draft_round_picks(season=2024, round_num='1')
```

_Last validated n/a._

## espn_wwc_season_futures

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/futures`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/futures](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/futures)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_wwc_season_futures-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_name` | character |  |
| `futures` | character |  |
| `id` | integer |  |
| `name` | character |  |
| `type` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_futures-example}

```python
espn_wwc_season_futures(season=2024)
```

_Last validated n/a._

## espn_wwc_season_freeagents

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/freeagents`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/freeagents](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/freeagents)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_wwc_season_freeagents-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_freeagents-example}

```python
espn_wwc_season_freeagents(season=2024)
```

_Last validated n/a._

## espn_wwc_season_powerindex

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/powerindex[/{team_id}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/powerindex](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/powerindex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `team_id` | `team_id` |  |  | `Y` | team_id path parameter. |

### Returns {#espn_wwc_season_powerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_updated` | character |  |
| `max_game_date` | character |  |
| `run_cutoff_date` | character |  |
| `run_date_time_key` | integer |  |
| `season` | integer |  |
| `season_type` | integer |  |
| `stats` | character |  |
| `league_$ref` | character |  |
| `team_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_powerindex-example}

```python
espn_wwc_season_powerindex(season=2024)
```

_Last validated n/a._

## espn_wwc_season_powerindex_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/powerindex/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/powerindex/leaders](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/powerindex/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_wwc_season_powerindex_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_powerindex_leaders-example}

```python
espn_wwc_season_powerindex_leaders(season=2024)
```

_Last validated n/a._

## espn_wwc_season_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/{season}/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/awards?limit=200](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons/2024/awards?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wwc_season_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_season_awards-example}

```python
espn_wwc_season_awards(season=2024)
```

_Last validated n/a._
