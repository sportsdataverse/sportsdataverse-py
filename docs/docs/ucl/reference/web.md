---
title: UCL — ESPN web API (v3)
sidebar_label: ESPN web API (v3)
description: "UCL — ESPN web API (v3) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 21
toc_max_heading_level: 2
---
# UCL — ESPN web API (v3)

`sportsdataverse.ucl` — 5 endpoints.

## espn_ucl_player_overview

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/athletes/{athlete_id}/overview`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/athletes/4239/overview](https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/athletes/4239/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_ucl_player_overview-returns}

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

### Example {#espn_ucl_player_overview-example}

```python
espn_ucl_player_overview(athlete_id='4239')
```

_Last validated n/a._

## espn_ucl_player_stats

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/athletes/{athlete_id}/stats`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/athletes/4239/stats](https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/athletes/4239/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_ucl_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | character | ESPN stat category the row belongs to (NBA "averages", "totals" or "miscellaneous"); each category carries its own stat columns, and the other categories' columns are null on its rows. |
| `split_name` | character | Season label of the row (e.g., "2010-11") on the Web v3 payload; the split's name on payloads that ship named splits. |
| `split_category` | character | Category of a named split; null on the Web v3 per-season rows. |
| `split_value` | character | Value of a named split; null on the Web v3 per-season rows. |
| `season` | integer | Season end year of the row (e.g., 2011 for 2010-11). |
| `team_id` | integer | ESPN team id (Int64) the athlete played that season for; a traded player has one row per team. |
| `position` | character | Position abbreviation the athlete was listed at for that season and team (e.g., "G"). |
| `games_played` | character | Games played (GP), as ESPN ships it (a string). |
| `games_started` | character | Games started (GS). |
| `avg_minutes` | character | Minutes per game (MIN). |
| `avg_field_goals_made_avg_field_goals_attempted` | character | Field goals made-attempted per game (FG), one "made-attempted" string (e.g., "3.9-9.0"). |
| `field_goal_pct` | character | Field goal percentage (FG%), FGM / FGA x 100. |
| `avg_three_point_field_goals_made_avg_three_point_field_goals_attempted` | character | Three-point field goals made-attempted per game (3PT), one "made-attempted" string. |
| `three_point_field_goal_pct` | character | Three-point field goal percentage (3P%), 3PM / 3PA x 100. |
| `avg_free_throws_made_avg_free_throws_attempted` | character | Free throws made-attempted per game (FT), one "made-attempted" string. |
| `free_throw_pct` | character | Free throw percentage (FT%), FTM / FTA x 100. |
| `avg_offensive_rebounds` | character | Offensive rebounds per game (OR). |
| `avg_defensive_rebounds` | character | Defensive rebounds per game (DR). |
| `avg_rebounds` | character | Rebounds per game (REB). |
| `avg_assists` | character | Assists per game (AST). |
| `avg_blocks` | character | Blocks per game (BLK). |
| `avg_steals` | character | Steals per game (STL). |
| `avg_fouls` | character | Personal fouls per game (PF). |
| `avg_turnovers` | character | Turnovers per game (TO). |
| `avg_points` | character | Points per game (PTS). |
| `field_goals_made_field_goals_attempted` | character | Season field goals made-attempted (FG), one "made-attempted" string. |
| `three_point_field_goals_made_three_point_field_goals_attempted` | character | Season three-point field goals made-attempted (3PT), one "made-attempted" string. |
| `free_throws_made_free_throws_attempted` | character | Season free throws made-attempted (FT), one "made-attempted" string. |
| `offensive_rebounds` | character | Season offensive rebounds (OR). |
| `defensive_rebounds` | character | Season defensive rebounds (DR). |
| `total_rebounds` | character | Season rebounds (REB). |
| `assists` | character | Season assists (AST). |
| `blocks` | character | Season blocked shots (BLK). |
| `steals` | character | Season steals (STL). |
| `fouls` | character | Season personal fouls (PF). |
| `turnovers` | character | Season turnovers (TO). |
| `points` | character | Season points (PTS). |
| `double_double` | character | Games with double figures in two of points, rebounds, assists, steals and blocks (DD2). |
| `triple_double` | character | Games with double figures in three of points, rebounds, assists, steals and blocks (TD3). |
| `disqualifications` | character | Times the athlete fouled out (DQ). |
| `ejections` | character | Times the athlete was ejected (EJECT). |
| `technical_fouls` | character | Technical fouls called on the athlete (TECH). |
| `flagrant_fouls` | character | Flagrant fouls called on the athlete (FLAG). |
| `assist_turnover_ratio` | character | Assists per turnover (AST/TO). |
| `steal_turnover_ratio` | character | Steals per turnover (STL/TO). |
| `scoring_efficiency` | character | ESPN's scoring efficiency (SC-EFF). |
| `shooting_efficiency` | character | ESPN's shooting efficiency (SH-EFF). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ucl_player_stats-example}

```python
espn_ucl_player_stats(athlete_id='4239')
```

_Last validated n/a._

## espn_ucl_player_gamelog

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/athletes/{athlete_id}/gamelog`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/athletes/4239/gamelog](https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/athletes/4239/gamelog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_ucl_player_gamelog-returns}

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

### Example {#espn_ucl_player_gamelog-example}

```python
espn_ucl_player_gamelog(athlete_id='4239')
```

_Last validated n/a._

## espn_ucl_player_splits

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/athletes/{athlete_id}/splits`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/athletes/4239/splits](https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/athletes/4239/splits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_ucl_player_splits-returns}

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

### Example {#espn_ucl_player_splits-example}

```python
espn_ucl_player_splits(athlete_id='4239')
```

_Last validated n/a._

## espn_ucl_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/statistics/byathlete`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/statistics/byathlete?limit=50&page=1](https://site.web.api.espn.com/apis/common/v3/sports/soccer/uefa.champions/statistics/byathlete?limit=50&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  |  | `Y` | category query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sort` | `sort` |  |  | `Y` | sort query parameter. |

### Returns {#espn_ucl_leaders-returns}

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

### Example {#espn_ucl_leaders-example}

```python
espn_ucl_leaders()
```

_Last validated n/a._
