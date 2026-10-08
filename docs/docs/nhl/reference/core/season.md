---
title: "NHL — ESPN core API (v2) — Season"
sidebar_label: "Season"
sidebar_position: 3
description: "NHL — ESPN core API (v2) — Season — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — ESPN core API (v2) — Season

## espn_nhl_season_pointer

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/season`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/season](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nhl_season_pointer-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_name` | character | Player display name. |
| `end_date` | character | Season end date. |
| `start_date` | character | Season start date. |
| `year` | integer | Season year from the API. |
| `athletes_$ref` | character |  |
| `coaches_$ref` | character |  |
| `futures_$ref` | character |  |
| `power_index_leaders_$ref` | character |  |
| `power_indexes_$ref` | character |  |
| `rankings_$ref` | character |  |
| `type_$ref` | character |  |
| `type_abbreviation` | character | Play type abbreviation. |
| `type_corrections_$ref` | character |  |
| `type_end_date` | character |  |
| `type_groups_$ref` | character |  |
| `type_has_groups` | logical |  |
| `type_has_legs` | logical |  |
| `type_has_standings` | logical |  |
| `type_id` | character | Play type id. |
| `type_name` | character | Status type name (e.g. "STATUS_FINAL"). |
| `type_slug` | character | Broadcast type slug. |
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

### Example {#espn_nhl_season_pointer-example}

```python
espn_nhl_season_pointer()
```

_Last validated n/a._

## espn_nhl_season_info

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_nhl_season_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_name` | character | Player display name. |
| `end_date` | character | Season end date. |
| `start_date` | character | Season start date. |
| `year` | integer | Season year from the API. |
| `awards_$ref` | character |  |
| `futures_$ref` | character |  |
| `leaders_$ref` | character |  |
| `power_index_leaders_$ref` | character |  |
| `power_indexes_$ref` | character |  |
| `rankings_$ref` | character |  |
| `type_$ref` | character |  |
| `type_abbreviation` | character | Play type abbreviation. |
| `type_corrections_$ref` | character |  |
| `type_end_date` | character |  |
| `type_groups_$ref` | character |  |
| `type_has_groups` | logical |  |
| `type_has_legs` | logical |  |
| `type_has_standings` | logical |  |
| `type_id` | character | Play type id. |
| `type_leaders_$ref` | character |  |
| `type_name` | character | Status type name (e.g. "STATUS_FINAL"). |
| `type_slug` | character | Broadcast type slug. |
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

### Example {#espn_nhl_season_info-example}

```python
espn_nhl_season_info(season=2024)
```

_Last validated n/a._

## espn_nhl_season_types

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/types`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_nhl_season_types-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_types-example}

```python
espn_nhl_season_types(season=2024)
```

_Last validated n/a._

## espn_nhl_season_type

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/types/{season_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_nhl_season_type-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `abbreviation` | character | Team abbreviation. |
| `end_date` | character | Season end date. |
| `has_groups` | logical | Whether this season type has groups. |
| `has_legs` | logical | Whether this season type has legs. |
| `has_standings` | logical | Whether this season type has standings. |
| `id` | character | Unique player identifier. |
| `name` | character | Team mascot name. |
| `slug` | character | URL slug. |
| `start_date` | character | Season start date. |
| `type` | integer | Competitor type (e.g. "team"). |
| `year` | integer | Season year from the API. |
| `corrections_$ref` | character |  |
| `groups_$ref` | character |  |
| `leaders_$ref` | character |  |
| `weeks_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_type-example}

```python
espn_nhl_season_type(season=2024, season_type=2)
```

_Last validated n/a._

## espn_nhl_season_group

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/types/{season_type}/groups/{group_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/groups/80](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/groups/80)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `group_id` | `group_id` |  | `Y` |  | group_id path parameter. |

### Returns {#espn_nhl_season_group-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `id` | character | Unique player identifier. |
| `is_conference` | logical | Whether this group is a conference (vs. division). |
| `links` | character |  |
| `midsize_name` | character |  |
| `name` | character | Team mascot name. |
| `short_name` | character | Short game name. |
| `slug` | character | URL slug. |
| `uid` | character | Competitor uid string. |
| `children_$ref` | character |  |
| `parent_$ref` | character |  |
| `season_$ref` | character |  |
| `standings_$ref` | character |  |
| `teams_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_group-example}

```python
espn_nhl_season_group(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## espn_nhl_season_groups

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/types/{season_type}/groups`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/groups](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_nhl_season_groups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_groups-example}

```python
espn_nhl_season_groups(season=2024, season_type=2)
```

_Last validated n/a._

## espn_nhl_season_group_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/types/{season_type}/groups/{group_id}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/groups/80/teams?limit=500](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/groups/80/teams?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `group_id` | `group_id` |  | `Y` |  | group_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nhl_season_group_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_group_teams-example}

```python
espn_nhl_season_group_teams(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## espn_nhl_season_group_children

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/types/{season_type}/groups/{group_id}/children`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/groups/80/children?limit=500](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/groups/80/children?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `group_id` | `group_id` |  | `Y` |  | group_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nhl_season_group_children-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_group_children-example}

```python
espn_nhl_season_group_children(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## espn_nhl_season_type_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/types/{season_type}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/leaders](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_nhl_season_type_leaders-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb, 2026-10-07); its rows sit under keys it does not read (top level: $ref, abbreviation, categories, id, name, type).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_type_leaders-example}

```python
espn_nhl_season_type_leaders(season=2024, season_type=2)
```

_Last validated n/a._

## espn_nhl_season_type_corrections

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/types/{season_type}/corrections`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/corrections](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/corrections)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_nhl_season_type_corrections-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athlete_$ref` | character |  |
| `competition_$ref` | character |  |
| `split_stats_abbreviation` | character |  |
| `split_stats_categories` | character |  |
| `split_stats_id` | character |  |
| `split_stats_name` | character |  |
| `team_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_type_corrections-example}

```python
espn_nhl_season_type_corrections(season=2024, season_type=2)
```

_Last validated n/a._

## espn_nhl_season_weeks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/types/{season_type}/weeks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/weeks](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/weeks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |

### Returns {#espn_nhl_season_weeks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_weeks-example}

```python
espn_nhl_season_weeks(season=2024, season_type=2)
```

_Last validated n/a._

## espn_nhl_season_week

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/types/{season_type}/weeks/{week}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/weeks/1](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/weeks/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `week` | `week` |  | `Y` |  | week path parameter. |

### Returns {#espn_nhl_season_week-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `end_date` | character | Season end date. |
| `number` | integer | Week number as returned by the API. |
| `start_date` | character | Season start date. |
| `text` | character | Full play description text. |
| `rankings_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_week-example}

```python
espn_nhl_season_week(season=2024, season_type=2, week=1)
```

_Last validated n/a._

## espn_nhl_season_week_powerindex

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/types/{season_type}/weeks/{week}/powerindex`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/weeks/8/powerindex](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/weeks/8/powerindex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `week` | `week` |  | `Y` |  | week path parameter. |
| `limit` | `limit` |  |  | `Y` | Page size for this weekly power-index table; pass a limit large enough to avoid paging (table size varies by sport/league -- CFB's FBS table alone is ~134 rows). |

### Returns {#espn_nhl_season_week_powerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `season` | integer | Season year (echoed from arg). |
| `last_updated` | character | Timestamp the entry was last updated. |
| `run_date_time_key` | integer |  |
| `fpi` | double |  |
| `fpirank` | double |  |
| `projectedw` | double |  |
| `projectedl` | double |  |
| `projectedt` | double |  |
| `record` | double |  |
| `probwinout` | double |  |
| `probwinconf` | character |  |
| `sosremainingrank` | double |  |
| `accomplishment` | character |  |
| `accomplishmentrank` | double |  |
| `adjwins` | character |  |
| `adjlosses` | character |  |
| `adjwinpctrank` | double |  |
| `gamecontrol` | character |  |
| `gamecontrolrank` | double |  |
| `adjavgingamewp` | character |  |
| `adjavgingamewprank` | double |  |
| `avgingamewp` | double |  |
| `avgingamewprank` | double |  |
| `avgsosrank` | double |  |
| `epaoffense` | double |  |
| `epadefense` | double |  |
| `epaspecialteams` | double |  |
| `probwindiv` | double |  |
| `offefficiency` | double |  |
| `offefficiencyrank` | double |  |
| `defefficiency` | double |  |
| `defefficiencyrank` | double |  |
| `stefficiency` | double |  |
| `stefficiencyrank` | double |  |
| `totefficiency` | double |  |
| `totefficiencyrank` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_week_powerindex-example}

```python
espn_nhl_season_week_powerindex(season=2024, season_type=2, week=8)
```

_Last validated n/a._

## espn_nhl_season_week_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/types/{season_type}/weeks/{week}/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/weeks/1/events?limit=500](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/types/2/weeks/1/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `season_type` | `season_type` |  | `Y` |  | season_type path parameter. |
| `week` | `week` |  | `Y` |  | week path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nhl_season_week_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_week_games-example}

```python
espn_nhl_season_week_games(season=2024, season_type=2, week=1)
```

_Last validated n/a._

## espn_nhl_season_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_nhl_season_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_teams-example}

```python
espn_nhl_season_teams(season=2024)
```

_Last validated n/a._

## espn_nhl_season_team

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/teams/4](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_nhl_season_team-returns}

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
| `venue_full_name` | character | Venue full name. |
| `venue_grass` | logical |  |
| `venue_guid` | character |  |
| `venue_id` | character | Venue identifier. |
| `venue_images` | character |  |
| `venue_indoor` | logical | Whether the venue is indoors. |
| `venue_short_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_team-example}

```python
espn_nhl_season_team(season=2024, team_id='4')
```

_Last validated n/a._

## espn_nhl_season_players

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/athletes?limit=100&page=1](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/athletes?limit=100&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_nhl_season_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_players-example}

```python
espn_nhl_season_players(season=2024)
```

_Last validated n/a._

## espn_nhl_season_coaches

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/coaches`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/coaches?limit=500](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/coaches?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nhl_season_coaches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_coaches-example}

```python
espn_nhl_season_coaches(season=2024)
```

_Last validated n/a._

## espn_nhl_season_draft

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/draft`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/draft](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_nhl_season_draft-returns}

**`return_parsed=True`** (default) — the output of `parse_draft`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_draft raises AttributeError: 'str' object has no attribute 'get' on the live payload (nba, nfl, wnba, 2026-10-07), so no columns can be derived.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_draft-example}

```python
espn_nhl_season_draft(season=2024)
```

_Last validated n/a._

## espn_nhl_season_draft_round_picks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/draft/rounds/{round_num}/picks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/draft/rounds/1/picks](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/draft/rounds/1/picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `round_num` | `round_num` |  | `Y` |  | round_num path parameter. |

### Returns {#espn_nhl_season_draft_round_picks-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_draft_round_picks-example}

```python
espn_nhl_season_draft_round_picks(season=2024, round_num='1')
```

_Last validated n/a._

## espn_nhl_season_futures

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/futures`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/futures](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/futures)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_nhl_season_futures-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_name` | character | Player display name. |
| `futures` | character |  |
| `id` | integer | Unique player identifier. |
| `name` | character | Team mascot name. |
| `type` | character | Competitor type (e.g. "team"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_futures-example}

```python
espn_nhl_season_futures(season=2024)
```

_Last validated n/a._

## espn_nhl_season_freeagents

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/freeagents`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/freeagents](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/freeagents)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_nhl_season_freeagents-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nfl, mlb, nhl, cfb; 400 in nba, wnba, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_freeagents-example}

```python
espn_nhl_season_freeagents(season=2024)
```

_Last validated n/a._

## espn_nhl_season_powerindex

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/powerindex[/{team_id}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/powerindex](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/powerindex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `team_id` | `team_id` |  |  | `Y` | team_id path parameter. |

### Returns {#espn_nhl_season_powerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_updated` | character | Timestamp the entry was last updated. |
| `max_game_date` | character |  |
| `run_cutoff_date` | character |  |
| `run_date_time_key` | integer |  |
| `season` | integer | Season year (echoed from arg). |
| `season_type` | integer | Season type code (echoed from arg). |
| `stats` | character |  |
| `league_$ref` | character |  |
| `team_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_powerindex-example}

```python
espn_nhl_season_powerindex(season=2024)
```

_Last validated n/a._

## espn_nhl_season_powerindex_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/powerindex/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/powerindex/leaders](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/powerindex/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_nhl_season_powerindex_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `display_name` | character | Player display name. |
| `leaders` | character |  |
| `name` | character | Team mascot name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_powerindex_leaders-example}

```python
espn_nhl_season_powerindex_leaders(season=2024)
```

_Last validated n/a._

## espn_nhl_season_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/awards?limit=200](https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2024/awards?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nhl_season_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_season_awards-example}

```python
espn_nhl_season_awards(season=2024)
```

_Last validated n/a._
