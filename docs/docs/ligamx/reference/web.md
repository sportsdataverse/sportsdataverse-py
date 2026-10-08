---
title: LIGAMX — ESPN web API (v3)
sidebar_label: ESPN web API (v3)
description: "LIGAMX — ESPN web API (v3) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 21
toc_max_heading_level: 2
---
# LIGAMX — ESPN web API (v3)

`sportsdataverse.ligamx` — 5 endpoints.

## espn_ligamx_player_overview

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/athletes/{athlete_id}/overview`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/athletes/4239/overview](https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/athletes/4239/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_ligamx_player_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athlete_id` | character |  |
| `athlete_display_name` | character |  |
| `athlete_short_name` | character |  |
| `athlete_position` | character |  |
| `athlete_jersey` | character |  |
| `athlete_team_id` | character |  |
| `athlete_team_abbreviation` | character |  |
| `split_name` | character |  |
| `split_category` | character |  |
| `games_played` | character |  |
| `avg_minutes` | character |  |
| `field_goal_pct` | character |  |
| `three_point_pct` | character |  |
| `free_throw_pct` | character |  |
| `avg_rebounds` | character |  |
| `avg_assists` | character |  |
| `avg_blocks` | character |  |
| `avg_steals` | character |  |
| `avg_fouls` | character |  |
| `avg_turnovers` | character |  |
| `avg_points` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_player_overview-example}

```python
espn_ligamx_player_overview(athlete_id='4239')
```

_Last validated n/a._

## espn_ligamx_player_stats

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/athletes/{athlete_id}/stats`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/athletes/4239/stats](https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/athletes/4239/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_ligamx_player_stats-returns}

**`return_parsed=True`** (default) — the output of `parse_athlete_stats`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_athlete_stats returns no columns on the live payload (nba, mlb, mbb, 2026-10-07); its rows sit under keys it does not read (top level: categories, filters, glossary, teams).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_player_stats-example}

```python
espn_ligamx_player_stats(athlete_id='4239')
```

_Last validated n/a._

## espn_ligamx_player_gamelog

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/athletes/{athlete_id}/gamelog`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/athletes/4239/gamelog](https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/athletes/4239/gamelog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_ligamx_player_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_type_id` | character |  |
| `season_type_name` | character |  |
| `category` | character |  |
| `event_id` | character |  |
| `event_date` | character |  |
| `home_away` | character |  |
| `score` | character |  |
| `opponent_id` | character |  |
| `opponent_abbreviation` | character |  |
| `opponent_display_name` | character |  |
| `game_result` | character |  |
| `game_processed` | character |  |
| `stat_0` | character |  |
| `stat_1` | character |  |
| `stat_2` | character |  |
| `stat_3` | character |  |
| `stat_4` | character |  |
| `stat_5` | character |  |
| `stat_6` | character |  |
| `stat_7` | character |  |
| `stat_8` | character |  |
| `stat_9` | character |  |
| `stat_10` | character |  |
| `stat_11` | character |  |
| `stat_12` | character |  |
| `stat_13` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_player_gamelog-example}

```python
espn_ligamx_player_gamelog(athlete_id='4239')
```

_Last validated n/a._

## espn_ligamx_player_splits

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/athletes/{athlete_id}/splits`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/athletes/4239/splits](https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/athletes/4239/splits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_ligamx_player_splits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `descriptions` | character |  |
| `display_name` | character |  |
| `display_names` | character |  |
| `filters` | character |  |
| `labels` | character |  |
| `names` | character |  |
| `split_categories` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_player_splits-example}

```python
espn_ligamx_player_splits(athlete_id='4239')
```

_Last validated n/a._

## espn_ligamx_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/statistics/byathlete`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/statistics/byathlete?limit=50&page=1](https://site.web.api.espn.com/apis/common/v3/sports/soccer/mex.1/statistics/byathlete?limit=50&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  |  | `Y` | category query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sort` | `sort` |  |  | `Y` | sort query parameter. |

### Returns {#espn_ligamx_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athletes` | character | Athletes. |
| `glossary` | character | Glossary. |
| `categories` | character | Categories. |
| `pagination_count` | integer | Pagination count. |
| `pagination_limit` | integer | Pagination limit. |
| `pagination_page` | integer | Pagination page. |
| `pagination_pages` | integer | Pagination pages. |
| `pagination_first` | character | Pagination first. |
| `pagination_next` | character | Pagination next. |
| `pagination_last` | character | Pagination last. |
| `league_id` | character | League id. |
| `league_uid` | character | League uid. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | League abbreviation. |
| `league_slug` | character | League slug. |
| `league_short_name` | character | League short name. |
| `current_season_year` | integer | Current season year. |
| `current_season_display_name` | character | Current season display name. |
| `current_season_start_date` | character | Current season start date. |
| `current_season_end_date` | character | Current season end date. |
| `current_season_type_id` | character | Current season type id. |
| `current_season_type_type` | integer | Current season type type. |
| `current_season_type_name` | character | Current season type name. |
| `current_season_type_start_date` | character | Current season type start date. |
| `current_season_type_end_date` | character | Current season type end date. |
| `requested_season_year` | integer | Requested season year. |
| `requested_season_display_name` | character | Requested season display name. |
| `requested_season_start_date` | character | Requested season start date. |
| `requested_season_end_date` | character | Requested season end date. |
| `requested_season_type_id` | character | Requested season type id. |
| `requested_season_type_type` | integer | Requested season type type. |
| `requested_season_type_name` | character | Requested season type name. |
| `requested_season_type_start_date` | character | Requested season type start date. |
| `requested_season_type_end_date` | character | Requested season type end date. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_leaders-example}

```python
espn_ligamx_leaders()
```

_Last validated n/a._
