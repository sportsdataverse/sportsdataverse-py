---
title: Women's college basketball — ESPN web API (v3)
sidebar_label: ESPN web API (v3)
description: "Women's college basketball — ESPN web API (v3) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 21
toc_max_heading_level: 2
---
# Women's college basketball — ESPN web API (v3)

`sportsdataverse.wbb` — 5 endpoints.

## espn_wbb_player_overview

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/athletes/{athlete_id}/overview`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/athletes/4239/overview](https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/athletes/4239/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wbb_player_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athlete_id` | Int64 | Unique athlete identifier (ESPN). |
| `athlete_display_name` | String | Athlete display name (full). |
| `athlete_short_name` | String | Athlete short display name. |
| `athlete_position` | String | Athlete position. |
| `athlete_jersey` | String | Athlete jersey number. |
| `athlete_team_id` | Int64 |  |
| `athlete_team_abbreviation` | String |  |
| `split_name` | String | Split name (typically "All Splits"). |
| `split_category` | String |  |
| `games_played` | String | Games played. |
| `avg_minutes` | String |  |
| `field_goal_pct` | String | Field goal percentage (0-1). |
| `three_point_pct` | String |  |
| `free_throw_pct` | String | Free throw percentage (0-1). |
| `avg_rebounds` | String |  |
| `avg_assists` | String |  |
| `avg_blocks` | String |  |
| `avg_steals` | String |  |
| `avg_fouls` | String |  |
| `avg_turnovers` | String |  |
| `avg_points` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wbb_player_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wbb_player_overview-example}

```python
espn_wbb_player_overview(athlete_id='4239')
```

## espn_wbb_player_stats_v3

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/athletes/{athlete_id}/stats`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/athletes/4433403/stats](https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/athletes/4433403/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_wbb_player_stats_v3-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | String | ESPN stat category the row belongs to (e.g., basketball "averages" / "totals" / "miscellaneous", NFL "passing", MLB "career-batting"). The stat columns after `position` are named from each category's own `names` list, so they differ by sport, and the other categories' columns are null on a row. |
| `split_name` | String | Season label of the row (e.g., "2010-11") on the Web v3 payload, or "Career" on the row built from the category's career totals; the split's name on payloads that ship named splits. |
| `split_category` | String | Category of a named split; null on the Web v3 per-season rows. |
| `split_value` | String | Value of a named split; null on the Web v3 per-season rows. |
| `season` | Int64 | Season end year of the row (e.g., 2011 for 2010-11); null on the "Career" row. |
| `team_id` | Int64 | ESPN team id (Int64) the athlete played that season for; a traded player has one row per team. Null on the "Career" row. |
| `position` | String | Position abbreviation the athlete was listed at for that season and team (e.g., "G"); null on the "Career" row. |
| `games_played` | String | Games played (GP), as ESPN ships it (a string). |
| `games_started` | String | Games started (GS). |
| `avg_minutes` | String | Minutes per game (MIN). |
| `avg_points` | String | Points per game (PTS). |
| `avg_rebounds` | String | Rebounds per game (REB). |
| `avg_assists` | String | Assists per game (AST). |
| `avg_steals` | String | Steals per game (STL). |
| `avg_blocks` | String | Blocks per game (BLK). |
| `avg_turnovers` | String | Turnovers per game (TO). |
| `avg_field_goals_made_avg_field_goals_attempted` | String | Field goals made-attempted per game (FG), one "made-attempted" string (e.g., "3.9-9.0"). |
| `field_goal_pct` | String | Field goal percentage (FG%), FGM / FGA x 100. |
| `avg_three_point_field_goals_made_avg_three_point_field_goals_attempted` | String | Three-point field goals made-attempted per game (3PT), one "made-attempted" string. |
| `three_point_field_goal_pct` | String | Three-point field goal percentage (3P%), 3PM / 3PA x 100. |
| `avg_free_throws_made_avg_free_throws_attempted` | String | Free throws made-attempted per game (FT), one "made-attempted" string. |
| `free_throw_pct` | String | Free throw percentage (FT%), FTM / FTA x 100. |
| `avg_fouls` | String | Personal fouls per game (PF). |
| `avg_offensive_rebounds` | String | Offensive rebounds per game (OR). |
| `avg_defensive_rebounds` | String | Defensive rebounds per game (DR). |
| `points` | String | Season points (PTS). |
| `offensive_rebounds` | String | Season offensive rebounds (OR). |
| `defensive_rebounds` | String | Season defensive rebounds (DR). |
| `total_rebounds` | String | Season rebounds (REB). |
| `assists` | String | Season assists (AST). |
| `steals` | String | Season steals (STL). |
| `blocks` | String | Season blocked shots (BLK). |
| `turnovers` | String | Season turnovers (TO). |
| `field_goals_made_field_goals_attempted` | String | Season field goals made-attempted (FG), one "made-attempted" string. |
| `three_point_field_goals_made_three_point_field_goals_attempted` | String | Season three-point field goals made-attempted (3PT), one "made-attempted" string. |
| `free_throws_made_free_throws_attempted` | String | Season free throws made-attempted (FT), one "made-attempted" string. |
| `fouls` | String | Season personal fouls (PF). |
| `double_double` | String | Games with double figures in two of points, rebounds, assists, steals and blocks (DD2). |
| `triple_double` | String | Games with double figures in three of points, rebounds, assists, steals and blocks (TD3). |
| `assist_turnover_ratio` | String | Assists per turnover (AST/TO). |
| `steal_turnover_ratio` | String | Steals per turnover (STL/TO). |
| `scoring_efficiency` | String | ESPN's scoring efficiency (SC-EFF). |
| `shooting_efficiency` | String | ESPN's shooting efficiency (SH-EFF). |
| `technical_fouls` | String | Technical fouls called on the athlete (TECH). |
| `disqualifications` | String | Times the athlete fouled out (DQ). |
| `ejections` | String | Times the athlete was ejected (EJECT). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wbb_player_stats_v3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wbb_player_stats_v3-example}

```python
espn_wbb_player_stats_v3(athlete_id='4433403')
```

## espn_wbb_player_gamelog

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/athletes/{athlete_id}/gamelog`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/athletes/4239/gamelog](https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/athletes/4239/gamelog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_wbb_player_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_type_id` | Int64 | Unique identifier for season type. |
| `season_type_name` | String | Season type name. |
| `category` | String | Category label. |
| `event_id` | Int64 | Unique event / game identifier (ESPN). |
| `event_date` | String |  |
| `home_away` | String | Game venue label ('home' or 'away'). |
| `score` | String | Final score. |
| `opponent_id` | Int64 | Unique identifier for opponent. |
| `opponent_abbreviation` | String |  |
| `opponent_display_name` | String |  |
| `game_result` | String |  |
| `game_processed` | String |  |
| `stat_0` | String |  |
| `stat_1` | String |  |
| `stat_2` | String |  |
| `stat_3` | String |  |
| `stat_4` | String |  |
| `stat_5` | String |  |
| `stat_6` | String |  |
| `stat_7` | String |  |
| `stat_8` | String |  |
| `stat_9` | String |  |
| `stat_10` | String |  |
| `stat_11` | String |  |
| `stat_12` | String |  |
| `stat_13` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wbb_player_gamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wbb_player_gamelog-example}

```python
espn_wbb_player_gamelog(athlete_id='4239')
```

## espn_wbb_player_splits

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/athletes/{athlete_id}/splits`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/athletes/4239/splits](https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/athletes/4239/splits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_wbb_player_splits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `descriptions` | String |  |
| `display_name` | String | Display name. |
| `display_names` | String |  |
| `filters` | String |  |
| `labels` | String |  |
| `names` | String | Names. |
| `split_categories` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wbb_player_splits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wbb_player_splits-example}

```python
espn_wbb_player_splits(athlete_id='4239')
```

## espn_wbb_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/statistics/byathlete`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/statistics/byathlete?limit=50&page=1](https://site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/statistics/byathlete?limit=50&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  |  | `Y` | Leaderboard population: the athletes ranked in that category, whose values it fills (other categories come back null). Football takes `<group>:<category>` (`offense:passing`, `offense:rushing`, `offense:receiving`, `defense:defensive`, `scoring:scoring`); a bare `passing` is rejected with HTTP 400 (ValueError). Basketball takes the bare name (`offensive`). |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sort` | `sort` |  |  | `Y` | Sort key `<category>.<statName>:<asc\|desc>`, the names in the payload's categories (`passing.passingYards:desc`, `offensive.avgPoints:desc`). A stat without its category is rejected with HTTP 400 (ValueError). |

### Returns {#espn_wbb_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `sort_order` | Int64 | Position of the athlete in ESPN's leaderboard order (the requested category or sort), counted across pages: (page - 1) * limit + position on the page. A position, not a rank: tied athletes still count up; ESPN's per-stat ranks stay in the raw payload (return_parsed=False). |
| `athlete_id` | Int64 | ESPN id of the athlete, the key the athlete endpoints (athlete_overview, athlete_stats) take. |
| `athlete_uid` | String | ESPN composite athlete uid (`s:<sport>~l:<league>~a:<athlete id>`), kept as text. |
| `athlete_display_name` | String | Athlete's full display name. |
| `athlete_short_name` | String | Athlete's abbreviated name (e.g. 'B. Nix'). |
| `athlete_first_name` | String | Athlete's first name. |
| `athlete_last_name` | String | Athlete's last name. |
| `athlete_position` | String | Athlete's position abbreviation (e.g. QB, G, C). |
| `athlete_status` | String | Athlete's ESPN status name (e.g. Active, Inactive) as ESPN reports it at request time, not for the requested season. |
| `team_id` | Int64 | ESPN id of the team the athlete played for in the requested season. |
| `team_abbreviation` | String | Abbreviation of the athlete's team in the requested season (ESPN teamShortName, e.g. ORE). |
| `team_name` | String | Nickname of the athlete's team in the requested season (ESPN teamName, e.g. Ducks). |
| `general_games_played` | Float64 | Games Played (ESPN 'general' category). |
| `general_avg_minutes` | Float64 | Minutes Per Game (ESPN 'general' category): The average number of minutes per game. |
| `general_avg_fouls` | Float64 | Fouls Per Game (ESPN 'general' category): The average fouls committed per game. |
| `general_technical_fouls` | Float64 | Technical Fouls (ESPN 'general' category): The number of times an player or coach was called for a technical foul (unsportsmanlike conduct or violations). |
| `general_ejections` | Float64 | Ejections (ESPN 'general' category): The number of times a player or coach is removed from the game as a result of a serious offense. |
| `general_double_double` | Float64 | Double Double (ESPN 'general' category): The number of times double digit values were accumulated in 2 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `general_triple_double` | Float64 | Triple Double (ESPN 'general' category): The number of times double digit values were accumulated in 3 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `general_per` | Float64 | Player Efficiency Rating (ESPN 'general' category): A numerical value for each of a player's accomplishments per-minute and is pace-adjusted for the team they play on. The league average in PER to 15.00 every season. |
| `general_minutes` | Float64 | Minutes (ESPN 'general' category): The total number of minutes played. |
| `general_rebounds` | Float64 | Rebounds (ESPN 'general' category): The total number of rebounds (offensive and defensive). |
| `general_fouls` | Float64 | Fouls (ESPN 'general' category): The number of times a player had illegal contact with the opponent. |
| `general_avg_rebounds` | Float64 | Rebounds Per Game (ESPN 'general' category): The average rebounds per game. |
| `offensive_avg_points` | Float64 | Points Per Game (ESPN 'offensive' category): The average number of points scored per game. |
| `offensive_avg_field_goals_made` | Float64 | Average Field Goals Made (ESPN 'offensive' category): The average field goals made per game. |
| `offensive_avg_field_goals_attempted` | Float64 | Average Field Goals Attempted (ESPN 'offensive' category): The average field goals attempted per game. |
| `offensive_field_goal_pct` | Float64 | Field Goal Percentage (ESPN 'offensive' category): The ratio of field goals made to field goals attempted: FGM / FGA. |
| `offensive_avg_three_point_field_goals_made` | Float64 | Average 3-Point Field Goals Made (ESPN 'offensive' category): The average three point field goals made per game. |
| `offensive_avg_three_point_field_goals_attempted` | Float64 | Average 3-Point Field Goals Attempted (ESPN 'offensive' category): The average three point field goals attempted per game. |
| `offensive_three_point_field_goal_pct` | Float64 | 3-Point Field Goal Percentage (ESPN 'offensive' category): The ratio of 3pt field goals made to 3pt field goals attempted: 3PM / 3PA. |
| `offensive_avg_free_throws_made` | Float64 | Average Free Throws Made (ESPN 'offensive' category): The average free throw shots made per game. |
| `offensive_avg_free_throws_attempted` | Float64 | Average Free Throws Attempted (ESPN 'offensive' category): The average free throw shots attempted per game. |
| `offensive_free_throw_pct` | Float64 | Free Throw Percentage (ESPN 'offensive' category): The ratio of free throws made to free throws attempted: FTM / FTA. |
| `offensive_avg_assists` | Float64 | Assists Per Game (ESPN 'offensive' category): The average assists per game. |
| `offensive_avg_turnovers` | Float64 | Turnovers Per Game (ESPN 'offensive' category): The average turnovers committed per game. |
| `offensive_points` | Float64 | Points (ESPN 'Points', offensive category): points scored in basketball; goals plus assists in hockey. |
| `offensive_field_goals_made` | Float64 | Field Goals Made (ESPN 'offensive' category): The number of times a 2pt field goal was made. |
| `offensive_field_goals_attempted` | Float64 | Field Goals Attempted (ESPN 'offensive' category): The number of times a 2pt field goal was attempted. |
| `offensive_three_point_field_goals_made` | Float64 | 3-Point Field Goals Made (ESPN 'offensive' category): The number of times a 3pt field goal was made. |
| `offensive_three_point_field_goals_attempted` | Float64 | 3-Point Field Goals Attempted (ESPN 'offensive' category): The number of times a 3pt field goal was attempted. |
| `offensive_free_throws_made` | Float64 | Free Throws Made (ESPN 'offensive' category): The number of times a free throw was made. |
| `offensive_free_throws_attempted` | Float64 | Free Throws Attempted (ESPN 'offensive' category): The number of times a free throw was attempted. |
| `offensive_assists` | Float64 | Assists (ESPN 'Assists', offensive category): passes that lead directly to a score in basketball; goal assists in hockey. |
| `offensive_turnovers` | Float64 | Turnovers (ESPN 'offensive' category): The number of times a player loses possession to the other team. |
| `defensive_avg_steals` | Float64 | Steals Per Game (ESPN 'defensive' category): The average steals per game. |
| `defensive_avg_blocks` | Float64 | Blocks Per Game (ESPN 'defensive' category): The average blocks per game. |
| `defensive_steals` | Float64 | Steals (ESPN 'defensive' category): The number of times a defensive player forced a turnover by intercepting or deflecting a pass or a dribble of an offensive player. |
| `defensive_blocks` | Float64 | Blocks (ESPN 'defensive' category): Short for blocked shot, number of times when a defensive player legally deflects a field goal attempt from an offensive player. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wbb_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wbb_leaders-example}

```python
espn_wbb_leaders()
```
