---
title: NFL — ESPN web API (v3)
sidebar_label: ESPN web API (v3)
description: "NFL — ESPN web API (v3) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 21
toc_max_heading_level: 2
---
# NFL — ESPN web API (v3)

`sportsdataverse.nfl` — 5 endpoints.

## espn_nfl_player_overview

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{athlete_id}/overview`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/overview](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nfl_player_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athlete_id` | Int64 |  |
| `athlete_display_name` | String |  |
| `athlete_short_name` | String |  |
| `athlete_position` | String |  |
| `athlete_jersey` | String |  |
| `athlete_team_id` | Int64 |  |
| `athlete_team_abbreviation` | String |  |
| `split_name` | String |  |
| `split_category` | String |  |
| `games_played` | String |  |
| `avg_minutes` | String |  |
| `field_goal_pct` | String |  |
| `three_point_pct` | String |  |
| `free_throw_pct` | String |  |
| `avg_rebounds` | String |  |
| `avg_assists` | String |  |
| `avg_blocks` | String |  |
| `avg_steals` | String |  |
| `avg_fouls` | String |  |
| `avg_turnovers` | String |  |
| `avg_points` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nfl_player_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_player_overview-example}

```python
espn_nfl_player_overview(athlete_id='4239')
```

## espn_nfl_player_stats_v3

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{athlete_id}/stats`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/3139477/stats](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/3139477/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_nfl_player_stats_v3-returns}

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
| `completions` | String | Pass completions (CMP). |
| `passing_attempts` | String | Pass attempts (ATT). |
| `completion_pct` | String | Completion percentage (CMP%). |
| `passing_yards` | String | Passing yards (YDS). |
| `yards_per_pass_attempt` | String | Passing yards per attempt (AVG). |
| `passing_touchdowns` | String | Passing touchdowns (TD). |
| `interceptions` | String | Passes thrown that were intercepted (INT). |
| `long_passing` | String | Longest completed pass in yards (LNG). |
| `sacks` | String | Times sacked (SACK) on the passing line; a defender's sacks on the defensive line. |
| `qb_rating` | String | NFL passer rating (RTG). |
| `adj_qbr` | String | ESPN's adjusted Total QBR (QBR). |
| `rushing_attempts` | String | Rushing attempts (CAR). |
| `rushing_yards` | String | Rushing yards (YDS). |
| `yards_per_rush_attempt` | String | Rushing yards per attempt (AVG). |
| `rushing_touchdowns` | String | Rushing touchdowns (TD). |
| `long_rushing` | String | Longest run in yards (LNG). |
| `rushing_first_downs` | String | First downs gained by rushing (FD). |
| `rushing_fumbles` | String | Fumbles on rushing plays (FUM). |
| `rushing_fumbles_lost` | String | Fumbles on rushing plays that the opponent recovered (LST). |
| `receptions` | String | Receptions (REC). |
| `receiving_targets` | String | Times targeted by a pass (TGTS). |
| `receiving_yards` | String | Receiving yards (YDS). |
| `yards_per_reception` | String | Receiving yards per reception (AVG). |
| `receiving_touchdowns` | String | Receiving touchdowns (TD). |
| `long_reception` | String | Longest reception in yards (LNG). |
| `receiving_first_downs` | String | First downs gained by reception (FD). |
| `receiving_fumbles` | String | Fumbles after a reception (FUM). |
| `receiving_fumbles_lost` | String | Fumbles after a reception that the opponent recovered (LST). |
| `total_tackles` | String | Total tackles (TOT), solo plus assisted. |
| `solo_tackles` | String | Unassisted tackles (SOLO). |
| `assist_tackles` | String | Assisted tackles (AST). |
| `fumbles_forced` | String | Forced fumbles (FF). |
| `fumbles_recovered` | String | Fumbles recovered (FR). |
| `fumbles_recovered_yards` | String | Return yards after fumble recoveries (YDS). |
| `interception_yards` | String | Return yards after interceptions (YDS). |
| `avg_interception_yards` | String | Average return yards per interception (AVG). |
| `interception_touchdowns` | String | Interceptions returned for a touchdown (TD). |
| `long_interception` | String | Longest interception return in yards (LNG). |
| `passes_defended` | String | Passes defended (PD). |
| `stuffs` | String | Runners stopped at or behind the line of scrimmage (STF). |
| `stuff_yards` | String | Yards lost on stuffs (STFYDS). |
| `kicks_blocked` | String | Kicks blocked (KB). |
| `return_touchdowns` | String | Kick and punt return touchdowns (RET). |
| `total_touchdowns` | String | Touchdowns of every kind (TD). |
| `total_two_point_convs` | String | Successful two-point conversions (2PT). |
| `kick_extra_points` | String | Kicked extra points made (PAT). |
| `field_goals` | String | Field goals made (FG). |
| `total_points` | String | Points scored (PTS). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nfl_player_stats_v3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_player_stats_v3-example}

```python
espn_nfl_player_stats_v3(athlete_id='3139477')
```

## espn_nfl_player_gamelog

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{athlete_id}/gamelog`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/gamelog](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/gamelog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_nfl_player_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_type_id` | Int64 |  |
| `season_type_name` | String |  |
| `category` | String | Broader category of player positions |
| `event_id` | Int64 |  |
| `event_date` | String |  |
| `home_away` | String |  |
| `score` | String |  |
| `opponent_id` | Int64 |  |
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

### Raises {#espn_nfl_player_gamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_player_gamelog-example}

```python
espn_nfl_player_gamelog(athlete_id='4239')
```

## espn_nfl_player_splits

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{athlete_id}/splits`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/splits](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/splits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_nfl_player_splits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `descriptions` | String |  |
| `display_name` | String | Full name of player |
| `display_names` | String |  |
| `filters` | String |  |
| `labels` | String |  |
| `names` | String |  |
| `split_categories` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nfl_player_splits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_player_splits-example}

```python
espn_nfl_player_splits(athlete_id='4239')
```

## espn_nfl_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/statistics/byathlete`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/statistics/byathlete?limit=50&page=1](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/statistics/byathlete?limit=50&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  |  | `Y` | Leaderboard population: the athletes ranked in that category, whose values it fills (other categories come back null). Football takes `<group>:<category>` (`offense:passing`, `offense:rushing`, `offense:receiving`, `defense:defensive`, `scoring:scoring`); a bare `passing` is rejected with HTTP 400 (ValueError). Basketball takes the bare name (`offensive`). |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sort` | `sort` |  |  | `Y` | Sort key `<category>.<statName>:<asc\|desc>`, the names in the payload's categories (`passing.passingYards:desc`, `offensive.avgPoints:desc`). A stat without its category is rejected with HTTP 400 (ValueError). |

### Returns {#espn_nfl_leaders-returns}

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
| `general_fumbles_forced` | Float64 | Forced Fumbles (ESPN 'general' category): The total number of forced fumbles. |
| `general_fumbles_recovered` | Float64 | Fumbles Recovered (ESPN 'general' category): The number of fumbles recovered. |
| `general_fumbles_touchdowns` | Float64 | Fumbles Touchdowns (ESPN 'general' category): The number of times a fumbles is recovered and returned for a touchdown. |
| `passing_completions` | Float64 | Completions (ESPN 'passing' category): The times a player completes a pass to another player who is eligible to catch a pass. |
| `passing_passing_attempts` | Float64 | Passing Attempts (ESPN 'passing' category): The number of times a pass is attempted. |
| `passing_completion_pct` | Float64 | Completion Percentage (ESPN 'passing' category): The percentage of completed passes. |
| `passing_passing_yards` | Float64 | Passing Yards (ESPN 'passing' category): The total passing yards. |
| `passing_yards_per_pass_attempt` | Float64 | Yards Per Pass Attempt (ESPN 'passing' category): The average number yards per pass attempt. |
| `passing_passing_yards_per_game` | Float64 | Passing Yards Per Game (ESPN 'passing' category): The number of passing yards per game. |
| `passing_long_passing` | Float64 | Longest Pass (ESPN 'passing' category): The longest pass play completed in terms of yards. |
| `passing_passing_touchdowns` | Float64 | Passing Touchdowns (ESPN 'passing' category): The total number of passing touchdowns. |
| `passing_interceptions` | Float64 | Interceptions (ESPN 'passing' category): The number of passes thrown that were intercepted by the opposing team. |
| `passing_sacks` | Float64 | Total Sacks (ESPN 'passing' category): The total number of sacks. |
| `passing_sack_yards_lost` | Float64 | Sack Yards Lost (ESPN 'passing' category): The amount of yards lost due to sacks. |
| `passing_qbr` | Float64 | Total QBR (ESPN 'passing' category): Total Quarterback Rating, which values quarterback on all play types on a 0-to-100 scale. |
| `passing_qb_rating` | Float64 | Passer Rating (ESPN 'passing' category). |
| `passing_adj_qbr` | Float64 | Adjusted QBR (ESPN 'passing' category): Adjusted Quarterback Rating. |
| `rushing_rushing_attempts` | Float64 | Rushing Attempts (ESPN 'rushing' category): The total number of attempted rushes. |
| `rushing_rushing_yards` | Float64 | Rushing Yards (ESPN 'rushing' category): The total rushing yards. |
| `rushing_yards_per_rush_attempt` | Float64 | Yards Per Rush Attempt (ESPN 'rushing' category): The average number of yards per rush attempt. |
| `rushing_long_rushing` | Float64 | Long Rushing (ESPN 'rushing' category): The amount of yards for the longest run. |
| `rushing_rushing_big_plays` | Float64 | 20+ Yard Rushing Plays (ESPN 'rushing' category): The number of rushing big plays gained. |
| `rushing_rushing_touchdowns` | Float64 | Rushing Touchdowns (ESPN 'rushing' category): The total number of rushing touchdowns. |
| `rushing_rushing_yards_per_game` | Float64 | Rushing Yards Per Game (ESPN 'rushing' category): The average number of rushing yards per game. |
| `rushing_rushing_fumbles` | Float64 | Rushing Fumbles (ESPN 'rushing' category): The number of times there is a run and then the ball is fumbled. |
| `rushing_rushing_fumbles_lost` | Float64 | Rushing Fumbles Lost (ESPN 'rushing' category): The number of times there is a run and then the ball is fumbled. |
| `rushing_rushing_first_downs` | Float64 | Rushing 1st downs (ESPN 'rushing' category): The number of times a first down is picked up by a run. |
| `receiving_receptions` | Float64 | Receptions (ESPN 'receiving' category): The total number of receptions. |
| `receiving_receiving_targets` | Float64 | Receiving Targets (ESPN 'receiving' category): The number of times a receiver is thrown to. |
| `receiving_receiving_yards` | Float64 | Receiving Yards (ESPN 'receiving' category): The total receiving yards. |
| `receiving_yards_per_reception` | Float64 | Yards Per Reception (ESPN 'receiving' category): The average number of yards per reception. |
| `receiving_receiving_touchdowns` | Float64 | Receiving Touchdowns (ESPN 'receiving' category): The total number of receiving touchdowns. |
| `receiving_long_reception` | Float64 | Long Reception (ESPN 'receiving' category): The amount of yards for the longest reception. |
| `receiving_receiving_big_plays` | Float64 | 20+ Yard Receiving Plays (ESPN 'receiving' category): The number of receiving big plays gained. |
| `receiving_receiving_yards_per_game` | Float64 | Receiving Yards Per Game (ESPN 'receiving' category): The average number of receiving yards per game. |
| `receiving_receiving_fumbles` | Float64 | Receiving Fumbles (ESPN 'receiving' category): The number of times a reception is made and then the ball is fumbled. |
| `receiving_receiving_fumbles_lost` | Float64 | Receiving Fumbles Lost (ESPN 'receiving' category): The number of times a reception is made and the balled is fumbled and recovered by the opposing team. |
| `receiving_receiving_yards_after_catch` | Float64 | Receiving Yards After Catch (ESPN 'receiving' category): The number of yards gained after the catch has been made. |
| `receiving_receiving_first_downs` | Float64 | Receiving First Downs (ESPN 'receiving' category): The number of times a first down is picked up by a reception. |
| `defensive_solo_tackles` | Float64 | Solo Tackles (ESPN 'defensive' category): The number of times a tackle was made unassisted. |
| `defensive_assist_tackles` | Float64 | Assist Tackles (ESPN 'defensive' category): The number of assists on tackles. |
| `defensive_total_tackles` | Float64 | Total Tackles (ESPN 'defensive' category): The total number of tackles. |
| `defensive_sacks` | Float64 | Sacks (ESPN 'defensive' category): The total number of sacks. |
| `defensive_sack_yards` | Float64 | Sack Yards (ESPN 'defensive' category): The number of yards lost from sacks. |
| `defensive_tackles_for_loss` | Float64 | Tackles For Loss (ESPN 'defensive' category): The number of tackles that result in a loss of yardage. |
| `defensive_passes_defended` | Float64 | Passes Defended (ESPN 'defensive' category): The total number of passes defended. |
| `defensive_long_interception` | Float64 | Long Interception (ESPN 'defensive' category): The amount of yards of the longest interception return. |
| `defensiveinterceptions_interceptions` | Float64 | Interceptions (ESPN 'defensiveinterceptions' category): The total number of interceptions. |
| `defensiveinterceptions_interception_yards` | Float64 | Interception Yards (ESPN 'defensiveinterceptions' category): The number of yards gained after an interception. |
| `defensiveinterceptions_interception_touchdowns` | Float64 | Interception Touchdowns (ESPN 'defensiveinterceptions' category): The number of times an interception is returned for a touchdown. |
| `scoring_rushing_touchdowns` | Float64 | Rushing Touchdowns (ESPN 'scoring' category): The total number of rushing touchdowns. |
| `scoring_receiving_touchdowns` | Float64 | Receiving Touchdowns (ESPN 'scoring' category): The total number of receiving touchdowns. |
| `scoring_return_touchdowns` | Float64 | Return Touchdowns (ESPN 'scoring' category): The total number of return touchdowns. |
| `scoring_total_touchdowns` | Float64 | Total Touchdowns (ESPN 'scoring' category): The number of touchdowns scored in total. |
| `scoring_field_goals` | Float64 | Field Goals (ESPN 'scoring' category): The number of field goals made. |
| `scoring_kick_extra_points` | Float64 | Kick Extra Points (ESPN 'scoring' category): The number of extra points made. |
| `scoring_total_two_point_convs` | Float64 | Total Two Point Conversions (ESPN 'scoring' category): The number of times a 2-point conversion is successful. |
| `scoring_total_points` | Float64 | Total Points (ESPN 'scoring' category): The number of total points scored. |
| `scoring_total_points_per_game` | Float64 | Total Points Per Game (ESPN 'scoring' category): The number of points scored per game. |
| `returning_kick_returns` | Float64 | Kick Returns (ESPN 'returning' category): The number of kick returns. |
| `returning_kick_return_yards` | Float64 | Kick Return Yards (ESPN 'returning' category): The total yardage of kick returns. |
| `returning_yards_per_kick_return` | Float64 | Yards Per Kick Return (ESPN 'returning' category): The average number of yards per kick return. |
| `returning_long_kick_return` | Float64 | Long Kick Return (ESPN 'returning' category): The longest kick return in yards. |
| `returning_kick_return_touchdowns` | Float64 | Kick Return Touchdowns (ESPN 'returning' category): The number of kick return touchdowns. |
| `returning_punt_returns` | Float64 | Punt Returns (ESPN 'returning' category): The number of punt returns. |
| `returning_punt_return_yards` | Float64 | Punt Return Yards (ESPN 'returning' category): The total yardage of punt returns. |
| `returning_yards_per_punt_return` | Float64 | Yards Per Punt Return (ESPN 'returning' category): The average number of yards per punt return. |
| `returning_long_punt_return` | Float64 | Long Punt Return (ESPN 'returning' category): The longest punt return in yards. |
| `returning_punt_return_touchdowns` | Float64 | Punt Return Touchdowns (ESPN 'returning' category): The number of punt return touchdowns. |
| `returning_punt_return_fair_catches` | Float64 | Punt Return Fair Catches (ESPN 'returning' category): The number of punt return fair catches. |
| `kicking_field_goals_made` | Float64 | Field Goal Made (ESPN 'kicking' category): The number of times a field goal is made. |
| `kicking_field_goal_attempts` | Float64 | Field Goal Attempts (ESPN 'kicking' category): The number of times a field goal is attempted. |
| `kicking_field_goal_pct` | Float64 | Field Goal Percentage (ESPN 'kicking' category): The percentage of field goals attempted that are made. |
| `kicking_long_field_goal_made` | Float64 | Long Field Goal Made (ESPN 'kicking' category): The distance of the longest field goal made. |
| `kicking_field_goals_made1_19` | Float64 | Field Goals Made 1-19 (ESPN 'kicking' category): The number of field goals made from 1-19 yards. |
| `kicking_field_goals_made20_29` | Float64 | Field Goals Made 20-29 (ESPN 'kicking' category): The number of field goals made from 20-29 yards. |
| `kicking_field_goals_made30_39` | Float64 | Field Goals Made 30-39 (ESPN 'kicking' category): The number of field goals made from 30-39 yards. |
| `kicking_field_goals_made40_49` | Float64 | Field Goals Made 40-49 (ESPN 'kicking' category): The number of field goals made from 40-49 yards. |
| `kicking_field_goals_made50` | Float64 | Field Goals Made 50+ (ESPN 'kicking' category): The number of field goals made from 50+ yards. |
| `kicking_field_goal_attempts1_19` | Float64 | Field Goal Attempts 1-19 (ESPN 'kicking' category): The number of field goals attempted from 1-19 yards. |
| `kicking_field_goal_attempts20_29` | Float64 | Field Goal Attempts 20-29 (ESPN 'kicking' category): The number of field goals attempted from 20-29 yards. |
| `kicking_field_goal_attempts30_39` | Float64 | Field Goal Attempts 30-39 (ESPN 'kicking' category): The number of field goals attempted from 30-39 yards. |
| `kicking_field_goal_attempts40_49` | Float64 | Field Goal Attempts 40-49 (ESPN 'kicking' category): The number of field goals attempted from 40-49 yards. |
| `kicking_field_goal_attempts50` | Float64 | Field Goal Attempts 50+ (ESPN 'kicking' category): The number of field goals attempted from 50+ yards. |
| `kicking_extra_points_made` | Float64 | Extra Points Made (ESPN 'kicking' category): The number of extra point attempts made. |
| `kicking_extra_point_attempts` | Float64 | Extra Point Attempts (ESPN 'kicking' category): The number of extra point attempts. |
| `kicking_extra_point_pct` | Float64 | Extra Point Percentage (ESPN 'kicking' category): The percentage of extra point attempts that are converted. |
| `punting_punts` | Float64 | Punts (ESPN 'punting' category): The number of punts taken. |
| `punting_punt_yards` | Float64 | Punt Yards (ESPN 'punting' category): The total amount of yardage of punts. |
| `punting_long_punt` | Float64 | Long Punt (ESPN 'punting' category): The distance of the longest punt. |
| `punting_gross_avg_punt_yards` | Float64 | Gross Average Punt Yards (ESPN 'punting' category): The average gross number of yards per punt. |
| `punting_net_avg_punt_yards` | Float64 | Net Average Punt Yards (ESPN 'punting' category): The average net number of yards per punt. |
| `punting_punts_blocked` | Float64 | Punts Blocked (ESPN 'punting' category): The number of times a punt was blocked. |
| `punting_punts_inside20` | Float64 | Punts Inside 20 (ESPN 'punting' category): The number of times a punt is downed inside the 20. |
| `punting_touchbacks` | Float64 | Touchbacks (ESPN 'punting' category): The number of kickoffs that result in touchbacks. |
| `punting_fair_catches` | Float64 | Fair Catches (ESPN 'punting' category): The number of fair catches. |
| `punting_punt_returns` | Float64 | Punt Returns (ESPN 'punting' category): The number of times a punt was returned. |
| `punting_punt_return_yards` | Float64 | Punt Return Yards (ESPN 'punting' category): The total amount of yardage of punt returns. |
| `punting_avg_punt_return_yards` | Float64 | Average Punt Return Yards (ESPN 'punting' category): The average number of yards per punt return. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nfl_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_leaders-example}

```python
espn_nfl_leaders()
```
