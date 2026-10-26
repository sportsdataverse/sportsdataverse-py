---
title: WNBA — ESPN web API (v3)
sidebar_label: ESPN web API (v3)
description: "WNBA — ESPN web API (v3) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 21
toc_max_heading_level: 2
---
# WNBA — ESPN web API (v3)

`sportsdataverse.wnba` — 5 endpoints.

## espn_wnba_player_overview

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/athletes/{athlete_id}/overview`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/athletes/4239/overview](https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/athletes/4239/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wnba_player_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athlete_id` | integer | Unique athlete identifier (ESPN). |
| `athlete_display_name` | character | Athlete display name (full). |
| `athlete_short_name` | character | Athlete short display name. |
| `athlete_position` | character | Athlete position. |
| `athlete_jersey` | character | Athlete jersey number. |
| `athlete_team_id` | integer |  |
| `athlete_team_abbreviation` | character |  |
| `split_name` | character | Split name (typically "All Splits"). |
| `split_category` | character |  |
| `games_played` | character | Games played. |
| `avg_minutes` | character |  |
| `field_goal_pct` | character | Field goal percentage (0-1). |
| `three_point_pct` | character |  |
| `free_throw_pct` | character | Free throw percentage (0-1). |
| `avg_rebounds` | character |  |
| `avg_assists` | character |  |
| `avg_blocks` | character |  |
| `avg_steals` | character |  |
| `avg_fouls` | character |  |
| `avg_turnovers` | character |  |
| `avg_points` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wnba_player_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wnba_player_overview-example}

```python
espn_wnba_player_overview(athlete_id='4239')
```

## espn_wnba_player_stats_v3

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/athletes/{athlete_id}/stats`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/athletes/3149391/stats](https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/athletes/3149391/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_wnba_player_stats_v3-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | character | ESPN stat category the row belongs to (e.g., basketball "averages" / "totals" / "miscellaneous", NFL "passing", MLB "career-batting"). The stat columns after `position` are named from each category's own `names` list, so they differ by sport, and the other categories' columns are null on a row. |
| `split_name` | character | Season label of the row (e.g., "2010-11") on the Web v3 payload, or "Career" on the row built from the category's career totals; the split's name on payloads that ship named splits. |
| `split_category` | character | Category of a named split; null on the Web v3 per-season rows. |
| `split_value` | character | Value of a named split; null on the Web v3 per-season rows. |
| `season` | integer | Season end year of the row (e.g., 2011 for 2010-11); null on the "Career" row. |
| `team_id` | integer | ESPN team id (Int64) the athlete played that season for; a traded player has one row per team. Null on the "Career" row. |
| `position` | character | Position abbreviation the athlete was listed at for that season and team (e.g., "G"); null on the "Career" row. |
| `games_played` | character | Games played (GP), as ESPN ships it (a string). |
| `games_started` | character | Games started (GS). |
| `avg_minutes` | character | Minutes per game (MIN). |
| `avg_points` | character | Points per game (PTS). |
| `avg_offensive_rebounds` | character | Offensive rebounds per game (OR). |
| `avg_defensive_rebounds` | character | Defensive rebounds per game (DR). |
| `avg_rebounds` | character | Rebounds per game (REB). |
| `avg_assists` | character | Assists per game (AST). |
| `avg_steals` | character | Steals per game (STL). |
| `avg_blocks` | character | Blocks per game (BLK). |
| `avg_turnovers` | character | Turnovers per game (TO). |
| `avg_field_goals_made_avg_field_goals_attempted` | character | Field goals made-attempted per game (FG), one "made-attempted" string (e.g., "3.9-9.0"). |
| `field_goal_pct` | character | Field goal percentage (FG%), FGM / FGA x 100. |
| `avg_three_point_field_goals_made_avg_three_point_field_goals_attempted` | character | Three-point field goals made-attempted per game (3PT), one "made-attempted" string. |
| `three_point_field_goal_pct` | character | Three-point field goal percentage (3P%), 3PM / 3PA x 100. |
| `avg_free_throws_made_avg_free_throws_attempted` | character | Free throws made-attempted per game (FT), one "made-attempted" string. |
| `free_throw_pct` | character | Free throw percentage (FT%), FTM / FTA x 100. |
| `avg_fouls` | character | Personal fouls per game (PF). |
| `points` | character | Season points (PTS). |
| `offensive_rebounds` | character | Season offensive rebounds (OR). |
| `defensive_rebounds` | character | Season defensive rebounds (DR). |
| `total_rebounds` | character | Season rebounds (REB). |
| `assists` | character | Season assists (AST). |
| `steals` | character | Season steals (STL). |
| `blocks` | character | Season blocked shots (BLK). |
| `turnovers` | character | Season turnovers (TO). |
| `field_goals_made_field_goals_attempted` | character | Season field goals made-attempted (FG), one "made-attempted" string. |
| `three_point_field_goals_made_three_point_field_goals_attempted` | character | Season three-point field goals made-attempted (3PT), one "made-attempted" string. |
| `free_throws_made_free_throws_attempted` | character | Season free throws made-attempted (FT), one "made-attempted" string. |
| `fouls` | character | Season personal fouls (PF). |
| `double_double` | character | Games with double figures in two of points, rebounds, assists, steals and blocks (DD2). |
| `triple_double` | character | Games with double figures in three of points, rebounds, assists, steals and blocks (TD3). |
| `assist_turnover_ratio` | character | Assists per turnover (AST/TO). |
| `steal_turnover_ratio` | character | Steals per turnover (STL/TO). |
| `scoring_efficiency` | character | ESPN's scoring efficiency (SC-EFF). |
| `shooting_efficiency` | character | ESPN's shooting efficiency (SH-EFF). |
| `technical_fouls` | character | Technical fouls called on the athlete (TECH). |
| `flagrant_fouls` | character | Flagrant fouls called on the athlete (FLAG). |
| `disqualifications` | character | Times the athlete fouled out (DQ). |
| `ejections` | character | Times the athlete was ejected (EJECT). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wnba_player_stats_v3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wnba_player_stats_v3-example}

```python
espn_wnba_player_stats_v3(athlete_id='3149391')
```

## espn_wnba_player_gamelog

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/athletes/{athlete_id}/gamelog`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/athletes/4239/gamelog](https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/athletes/4239/gamelog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_wnba_player_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_type_id` | integer | Unique identifier for season type. |
| `season_type_name` | character | Season type name. |
| `category` | character | Category label. |
| `event_id` | integer | Unique event / game identifier (ESPN). |
| `event_date` | character |  |
| `home_away` | character | Game venue label ('home' or 'away'). |
| `score` | character | Final score. |
| `opponent_id` | integer | Unique identifier for opponent. |
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

### Raises {#espn_wnba_player_gamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wnba_player_gamelog-example}

```python
espn_wnba_player_gamelog(athlete_id='4239')
```

## espn_wnba_player_splits

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/athletes/{athlete_id}/splits`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/athletes/4239/splits](https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/athletes/4239/splits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_wnba_player_splits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `descriptions` | character |  |
| `display_name` | character | Display name. |
| `display_names` | character |  |
| `filters` | character |  |
| `labels` | character |  |
| `names` | character | Names. |
| `split_categories` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wnba_player_splits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wnba_player_splits-example}

```python
espn_wnba_player_splits(athlete_id='4239')
```

## espn_wnba_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/statistics/byathlete`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/statistics/byathlete?limit=50&page=1](https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/statistics/byathlete?limit=50&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  |  | `Y` | Leaderboard population, which also fills that category's per-stat ranks. Football takes '<group>:<category>' ('offense:passing', 'offense:rushing', 'offense:receiving', 'defense:defensive', 'scoring:scoring'); a bare 'passing' is rejected with HTTP 400 (ValueError). |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sort` | `sort` |  |  | `Y` | Sort key '<category>.<statName>:<asc\|desc>', the names in the payload's categories ('passing.passingYards:desc', 'offensive.avgPoints:desc'). A stat without its category is rejected with HTTP 400 (ValueError). |

### Returns {#espn_wnba_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | integer | Place of the athlete in ESPN's leaderboard order (the requested category or sort), counted across pages: (page - 1) * limit + position on the page. |
| `athlete_id` | integer | ESPN id of the athlete, the key the athlete endpoints (athlete_overview, athlete_stats) take. |
| `athlete_uid` | character | ESPN composite athlete uid (s:<sport>~l:<league>~a:<athlete id>), kept as text. |
| `athlete_display_name` | character | Athlete's full display name. |
| `athlete_short_name` | character | Athlete's abbreviated name (e.g. 'B. Nix'). |
| `athlete_first_name` | character | Athlete's first name. |
| `athlete_last_name` | character | Athlete's last name. |
| `athlete_position` | character | Athlete's position abbreviation (e.g. QB, G, C). |
| `athlete_status` | character | Athlete's ESPN status name (e.g. Active, Inactive) as ESPN reports it at request time, not for the requested season. |
| `team_id` | integer | ESPN id of the team the athlete played for in the requested season. |
| `team_abbreviation` | character | Abbreviation of the athlete's team in the requested season (ESPN teamShortName, e.g. ORE). |
| `team_name` | character | Nickname of the athlete's team in the requested season (ESPN teamName, e.g. Ducks). |
| `general_games_played` | double | Games Played (ESPN 'general' category). |
| `general_avg_minutes` | double | Minutes Per Game (ESPN 'general' category): The average number of minutes per game. |
| `general_double_double` | double | Double Double (ESPN 'general' category): The number of times double digit values were accumulated in 2 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `general_triple_double` | double | Triple Double (ESPN 'general' category): The number of times double digit values were accumulated in 3 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `general_per` | double | Player Efficiency Rating (ESPN 'general' category): A numerical value for each of a player's accomplishments per-minute and is pace-adjusted for the team they play on. The league average in PER to 15.00 every season. |
| `general_avg_rebounds` | double | Rebounds Per Game (ESPN 'general' category): The average rebounds per game. |
| `offensive_points` | double | Points (ESPN 'Points', offensive category): points scored in basketball; goals plus assists in hockey. |
| `offensive_avg_points` | double | Points Per Game (ESPN 'offensive' category): The average number of points scored per game. |
| `offensive_avg_field_goals_made` | double | Average Field Goals Made (ESPN 'offensive' category): The average field goals made per game. |
| `offensive_avg_field_goals_attempted` | double | Average Field Goals Attempted (ESPN 'offensive' category): The average field goals attempted per game. |
| `offensive_field_goal_pct` | double | Field Goal Percentage (ESPN 'offensive' category): The ratio of field goals made to field goals attempted: FGM / FGA. |
| `offensive_avg_three_point_field_goals_made` | double | Average 3-Point Field Goals Made (ESPN 'offensive' category): The average three point field goals made per game. |
| `offensive_avg_three_point_field_goals_attempted` | double | Average 3-Point Field Goals Attempted (ESPN 'offensive' category): The average three point field goals attempted per game. |
| `offensive_three_point_field_goal_pct` | double | 3-Point Field Goal Percentage (ESPN 'offensive' category): The ratio of 3pt field goals made to 3pt field goals attempted: 3PM / 3PA. |
| `offensive_avg_free_throws_made` | double | Average Free Throws Made (ESPN 'offensive' category): The average free throw shots made per game. |
| `offensive_avg_free_throws_attempted` | double | Average Free Throws Attempted (ESPN 'offensive' category): The average free throw shots attempted per game. |
| `offensive_free_throw_pct` | double | Free Throw Percentage (ESPN 'offensive' category): The ratio of free throws made to free throws attempted: FTM / FTA. |
| `offensive_avg_assists` | double | Assists Per Game (ESPN 'offensive' category): The average assists per game. |
| `offensive_avg_turnovers` | double | Turnovers Per Game (ESPN 'offensive' category): The average turnovers committed per game. |
| `defensive_avg_steals` | double | Steals Per Game (ESPN 'defensive' category): The average steals per game. |
| `defensive_avg_blocks` | double | Blocks Per Game (ESPN 'defensive' category): The average blocks per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wnba_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wnba_leaders-example}

```python
espn_wnba_leaders()
```
