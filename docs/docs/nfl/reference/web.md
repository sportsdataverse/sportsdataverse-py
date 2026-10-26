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
| `athlete_id` | integer |  |
| `athlete_display_name` | character |  |
| `athlete_short_name` | character |  |
| `athlete_position` | character |  |
| `athlete_jersey` | character |  |
| `athlete_team_id` | integer |  |
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
| `category` | character | ESPN stat category the row belongs to (e.g., basketball "averages" / "totals" / "miscellaneous", NFL "passing", MLB "career-batting"). The stat columns after `position` are named from each category's own `names` list, so they differ by sport, and the other categories' columns are null on a row. |
| `split_name` | character | Season label of the row (e.g., "2010-11") on the Web v3 payload, or "Career" on the row built from the category's career totals; the split's name on payloads that ship named splits. |
| `split_category` | character | Category of a named split; null on the Web v3 per-season rows. |
| `split_value` | character | Value of a named split; null on the Web v3 per-season rows. |
| `season` | integer | Season end year of the row (e.g., 2011 for 2010-11); null on the "Career" row. |
| `team_id` | integer | ESPN team id (Int64) the athlete played that season for; a traded player has one row per team. Null on the "Career" row. |
| `position` | character | Position abbreviation the athlete was listed at for that season and team (e.g., "G"); null on the "Career" row. |
| `games_played` | character | Games played (GP), as ESPN ships it (a string). |
| `completions` | character | Pass completions (CMP). |
| `passing_attempts` | character | Pass attempts (ATT). |
| `completion_pct` | character | Completion percentage (CMP%). |
| `passing_yards` | character | Passing yards (YDS). |
| `yards_per_pass_attempt` | character | Passing yards per attempt (AVG). |
| `passing_touchdowns` | character | Passing touchdowns (TD). |
| `interceptions` | character | Passes thrown that were intercepted (INT). |
| `long_passing` | character | Longest completed pass in yards (LNG). |
| `sacks` | character | Times sacked (SACK) on the passing line; a defender's sacks on the defensive line. |
| `qb_rating` | character | NFL passer rating (RTG). |
| `adj_qbr` | character | ESPN's adjusted Total QBR (QBR). |
| `rushing_attempts` | character | Rushing attempts (CAR). |
| `rushing_yards` | character | Rushing yards (YDS). |
| `yards_per_rush_attempt` | character | Rushing yards per attempt (AVG). |
| `rushing_touchdowns` | character | Rushing touchdowns (TD). |
| `long_rushing` | character | Longest run in yards (LNG). |
| `rushing_first_downs` | character | First downs gained by rushing (FD). |
| `rushing_fumbles` | character | Fumbles on rushing plays (FUM). |
| `rushing_fumbles_lost` | character | Fumbles on rushing plays that the opponent recovered (LST). |
| `receptions` | character | Receptions (REC). |
| `receiving_targets` | character | Times targeted by a pass (TGTS). |
| `receiving_yards` | character | Receiving yards (YDS). |
| `yards_per_reception` | character | Receiving yards per reception (AVG). |
| `receiving_touchdowns` | character | Receiving touchdowns (TD). |
| `long_reception` | character | Longest reception in yards (LNG). |
| `receiving_first_downs` | character | First downs gained by reception (FD). |
| `receiving_fumbles` | character | Fumbles after a reception (FUM). |
| `receiving_fumbles_lost` | character | Fumbles after a reception that the opponent recovered (LST). |
| `total_tackles` | character | Total tackles (TOT), solo plus assisted. |
| `solo_tackles` | character | Unassisted tackles (SOLO). |
| `assist_tackles` | character | Assisted tackles (AST). |
| `fumbles_forced` | character | Forced fumbles (FF). |
| `fumbles_recovered` | character | Fumbles recovered (FR). |
| `fumbles_recovered_yards` | character | Return yards after fumble recoveries (YDS). |
| `interception_yards` | character | Return yards after interceptions (YDS). |
| `avg_interception_yards` | character | Average return yards per interception (AVG). |
| `interception_touchdowns` | character | Interceptions returned for a touchdown (TD). |
| `long_interception` | character | Longest interception return in yards (LNG). |
| `passes_defended` | character | Passes defended (PD). |
| `stuffs` | character | Runners stopped at or behind the line of scrimmage (STF). |
| `stuff_yards` | character | Yards lost on stuffs (STFYDS). |
| `kicks_blocked` | character | Kicks blocked (KB). |
| `return_touchdowns` | character | Kick and punt return touchdowns (RET). |
| `total_touchdowns` | character | Touchdowns of every kind (TD). |
| `total_two_point_convs` | character | Successful two-point conversions (2PT). |
| `kick_extra_points` | character | Kicked extra points made (PAT). |
| `field_goals` | character | Field goals made (FG). |
| `total_points` | character | Points scored (PTS). |

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
| `season_type_id` | integer |  |
| `season_type_name` | character |  |
| `category` | character | Broader category of player positions |
| `event_id` | integer |  |
| `event_date` | character |  |
| `home_away` | character |  |
| `score` | character |  |
| `opponent_id` | integer |  |
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
| `descriptions` | character |  |
| `display_name` | character | Full name of player |
| `display_names` | character |  |
| `filters` | character |  |
| `labels` | character |  |
| `names` | character |  |
| `split_categories` | character |  |

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
| `category` | `category` |  |  | `Y` | Leaderboard population, which also fills that category's per-stat ranks. Football takes '<group>:<category>' ('offense:passing', 'offense:rushing', 'offense:receiving', 'defense:defensive', 'scoring:scoring'); a bare 'passing' is rejected with HTTP 400 (ValueError). |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sort` | `sort` |  |  | `Y` | Sort key '<category>.<statName>:<asc\|desc>', the names in the payload's categories ('passing.passingYards:desc', 'offensive.avgPoints:desc'). A stat without its category is rejected with HTTP 400 (ValueError). |

### Returns {#espn_nfl_leaders-returns}

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
| `general_fumbles_forced` | double | Forced Fumbles (ESPN 'general' category): The total number of forced fumbles. |
| `general_fumbles_recovered` | double | Fumbles Recovered (ESPN 'general' category): The number of fumbles recovered. |
| `general_fumbles_touchdowns` | double | Fumbles Touchdowns (ESPN 'general' category): The number of times a fumbles is recovered and returned for a touchdown. |
| `passing_completions` | double | Completions (ESPN 'passing' category): The times a player completes a pass to another player who is eligible to catch a pass. |
| `passing_passing_attempts` | double | Passing Attempts (ESPN 'passing' category): The number of times a pass is attempted. |
| `passing_completion_pct` | double | Completion Percentage (ESPN 'passing' category): The percentage of completed passes. |
| `passing_passing_yards` | double | Passing Yards (ESPN 'passing' category): The total passing yards. |
| `passing_yards_per_pass_attempt` | double | Yards Per Pass Attempt (ESPN 'passing' category): The average number yards per pass attempt. |
| `passing_passing_yards_per_game` | double | Passing Yards Per Game (ESPN 'passing' category): The number of passing yards per game. |
| `passing_long_passing` | double | Longest Pass (ESPN 'passing' category): The longest pass play completed in terms of yards. |
| `passing_passing_touchdowns` | double | Passing Touchdowns (ESPN 'passing' category): The total number of passing touchdowns. |
| `passing_interceptions` | double | Interceptions (ESPN 'passing' category): The number of passes thrown that were intercepted by the opposing team. |
| `passing_sacks` | double | Total Sacks (ESPN 'passing' category): The total number of sacks. |
| `passing_sack_yards_lost` | double | Sack Yards Lost (ESPN 'passing' category): The amount of yards lost due to sacks. |
| `passing_qbr` | double | Total QBR (ESPN 'passing' category): Total Quarterback Rating, which values quarterback on all play types on a 0-to-100 scale. |
| `passing_qb_rating` | double | Passer Rating (ESPN 'passing' category). |
| `passing_adj_qbr` | double | Adjusted QBR (ESPN 'passing' category): Adjusted Quarterback Rating. |
| `rushing_rushing_attempts` | double | Rushing Attempts (ESPN 'rushing' category): The total number of attempted rushes. |
| `rushing_rushing_yards` | double | Rushing Yards (ESPN 'rushing' category): The total rushing yards. |
| `rushing_yards_per_rush_attempt` | double | Yards Per Rush Attempt (ESPN 'rushing' category): The average number of yards per rush attempt. |
| `rushing_long_rushing` | double | Long Rushing (ESPN 'rushing' category): The amount of yards for the longest run. |
| `rushing_rushing_big_plays` | double | 20+ Yard Rushing Plays (ESPN 'rushing' category): The number of rushing big plays gained. |
| `rushing_rushing_touchdowns` | double | Rushing Touchdowns (ESPN 'rushing' category): The total number of rushing touchdowns. |
| `rushing_rushing_yards_per_game` | double | Rushing Yards Per Game (ESPN 'rushing' category): The average number of rushing yards per game. |
| `rushing_rushing_fumbles` | double | Rushing Fumbles (ESPN 'rushing' category): The number of times there is a run and then the ball is fumbled. |
| `rushing_rushing_fumbles_lost` | double | Rushing Fumbles Lost (ESPN 'rushing' category): The number of times there is a run and then the ball is fumbled. |
| `rushing_rushing_first_downs` | double | Rushing 1st downs (ESPN 'rushing' category): The number of times a first down is picked up by a run. |
| `receiving_receptions` | double | Receptions (ESPN 'receiving' category): The total number of receptions. |
| `receiving_receiving_targets` | double | Receiving Targets (ESPN 'receiving' category): The number of times a receiver is thrown to. |
| `receiving_receiving_yards` | double | Receiving Yards (ESPN 'receiving' category): The total receiving yards. |
| `receiving_yards_per_reception` | double | Yards Per Reception (ESPN 'receiving' category): The average number of yards per reception. |
| `receiving_receiving_touchdowns` | double | Receiving Touchdowns (ESPN 'receiving' category): The total number of receiving touchdowns. |
| `receiving_long_reception` | double | Long Reception (ESPN 'receiving' category): The amount of yards for the longest reception. |
| `receiving_receiving_big_plays` | double | 20+ Yard Receiving Plays (ESPN 'receiving' category): The number of receiving big plays gained. |
| `receiving_receiving_yards_per_game` | double | Receiving Yards Per Game (ESPN 'receiving' category): The average number of receiving yards per game. |
| `receiving_receiving_fumbles` | double | Receiving Fumbles (ESPN 'receiving' category): The number of times a reception is made and then the ball is fumbled. |
| `receiving_receiving_fumbles_lost` | double | Receiving Fumbles Lost (ESPN 'receiving' category): The number of times a reception is made and the balled is fumbled and recovered by the opposing team. |
| `receiving_receiving_yards_after_catch` | double | Receiving Yards After Catch (ESPN 'receiving' category): The number of yards gained after the catch has been made. |
| `receiving_receiving_first_downs` | double | Receiving First Downs (ESPN 'receiving' category): The number of times a first down is picked up by a reception. |
| `defensive_solo_tackles` | double | Solo Tackles (ESPN 'defensive' category): The number of times a tackle was made unassisted. |
| `defensive_assist_tackles` | double | Assist Tackles (ESPN 'defensive' category): The number of assists on tackles. |
| `defensive_total_tackles` | double | Total Tackles (ESPN 'defensive' category): The total number of tackles. |
| `defensive_sacks` | double | Sacks (ESPN 'defensive' category): The total number of sacks. |
| `defensive_sack_yards` | double | Sack Yards (ESPN 'defensive' category): The number of yards lost from sacks. |
| `defensive_tackles_for_loss` | double | Tackles For Loss (ESPN 'defensive' category): The number of tackles that result in a loss of yardage. |
| `defensive_passes_defended` | double | Passes Defended (ESPN 'defensive' category): The total number of passes defended. |
| `defensive_long_interception` | double | Long Interception (ESPN 'defensive' category): The amount of yards of the longest interception return. |
| `defensiveinterceptions_interceptions` | double | Interceptions (ESPN 'defensiveinterceptions' category): The total number of interceptions. |
| `defensiveinterceptions_interception_yards` | double | Interception Yards (ESPN 'defensiveinterceptions' category): The number of yards gained after an interception. |
| `defensiveinterceptions_interception_touchdowns` | double | Interception Touchdowns (ESPN 'defensiveinterceptions' category): The number of times an interception is returned for a touchdown. |
| `scoring_rushing_touchdowns` | double | Rushing Touchdowns (ESPN 'scoring' category): The total number of rushing touchdowns. |
| `scoring_receiving_touchdowns` | double | Receiving Touchdowns (ESPN 'scoring' category): The total number of receiving touchdowns. |
| `scoring_return_touchdowns` | double | Return Touchdowns (ESPN 'scoring' category): The total number of return touchdowns. |
| `scoring_total_touchdowns` | double | Total Touchdowns (ESPN 'scoring' category): The number of touchdowns scored in total. |
| `scoring_field_goals` | double | Field Goals (ESPN 'scoring' category): The number of field goals made. |
| `scoring_kick_extra_points` | double | Kick Extra Points (ESPN 'scoring' category): The number of extra points made. |
| `scoring_total_two_point_convs` | double | Total Two Point Conversions (ESPN 'scoring' category): The number of times a 2-point conversion is successful. |
| `scoring_total_points` | double | Total Points (ESPN 'scoring' category): The number of total points scored. |
| `scoring_total_points_per_game` | double | Total Points Per Game (ESPN 'scoring' category): The number of points scored per game. |
| `returning_kick_returns` | double | Kick Returns (ESPN 'returning' category): The number of kick returns. |
| `returning_kick_return_yards` | double | Kick Return Yards (ESPN 'returning' category): The total yardage of kick returns. |
| `returning_yards_per_kick_return` | double | Yards Per Kick Return (ESPN 'returning' category): The average number of yards per kick return. |
| `returning_long_kick_return` | double | Long Kick Return (ESPN 'returning' category): The longest kick return in yards. |
| `returning_kick_return_touchdowns` | double | Kick Return Touchdowns (ESPN 'returning' category): The number of kick return touchdowns. |
| `returning_punt_returns` | double | Punt Returns (ESPN 'returning' category): The number of punt returns. |
| `returning_punt_return_yards` | double | Punt Return Yards (ESPN 'returning' category): The total yardage of punt returns. |
| `returning_yards_per_punt_return` | double | Yards Per Punt Return (ESPN 'returning' category): The average number of yards per punt return. |
| `returning_long_punt_return` | double | Long Punt Return (ESPN 'returning' category): The longest punt return in yards. |
| `returning_punt_return_touchdowns` | double | Punt Return Touchdowns (ESPN 'returning' category): The number of punt return touchdowns. |
| `returning_punt_return_fair_catches` | double | Punt Return Fair Catches (ESPN 'returning' category): The number of punt return fair catches. |
| `kicking_field_goals_made` | double | Field Goal Made (ESPN 'kicking' category): The number of times a field goal is made. |
| `kicking_field_goal_attempts` | double | Field Goal Attempts (ESPN 'kicking' category): The number of times a field goal is attempted. |
| `kicking_field_goal_pct` | double | Field Goal Percentage (ESPN 'kicking' category): The percentage of field goals attempted that are made. |
| `kicking_long_field_goal_made` | double | Long Field Goal Made (ESPN 'kicking' category): The distance of the longest field goal made. |
| `kicking_field_goals_made1_19` | double | Field Goals Made 1-19 (ESPN 'kicking' category): The number of field goals made from 1-19 yards. |
| `kicking_field_goals_made20_29` | double | Field Goals Made 20-29 (ESPN 'kicking' category): The number of field goals made from 20-29 yards. |
| `kicking_field_goals_made30_39` | double | Field Goals Made 30-39 (ESPN 'kicking' category): The number of field goals made from 30-39 yards. |
| `kicking_field_goals_made40_49` | double | Field Goals Made 40-49 (ESPN 'kicking' category): The number of field goals made from 40-49 yards. |
| `kicking_field_goals_made50` | double | Field Goals Made 50+ (ESPN 'kicking' category): The number of field goals made from 50+ yards. |
| `kicking_field_goal_attempts1_19` | double | Field Goal Attempts 1-19 (ESPN 'kicking' category): The number of field goals attempted from 1-19 yards. |
| `kicking_field_goal_attempts20_29` | double | Field Goal Attempts 20-29 (ESPN 'kicking' category): The number of field goals attempted from 20-29 yards. |
| `kicking_field_goal_attempts30_39` | double | Field Goal Attempts 30-39 (ESPN 'kicking' category): The number of field goals attempted from 30-39 yards. |
| `kicking_field_goal_attempts40_49` | double | Field Goal Attempts 40-49 (ESPN 'kicking' category): The number of field goals attempted from 40-49 yards. |
| `kicking_field_goal_attempts50` | double | Field Goal Attempts 50+ (ESPN 'kicking' category): The number of field goals attempted from 50+ yards. |
| `kicking_extra_points_made` | double | Extra Points Made (ESPN 'kicking' category): The number of extra point attempts made. |
| `kicking_extra_point_attempts` | double | Extra Point Attempts (ESPN 'kicking' category): The number of extra point attempts. |
| `kicking_extra_point_pct` | double | Extra Point Percentage (ESPN 'kicking' category): The percentage of extra point attempts that are converted. |
| `punting_punts` | double | Punts (ESPN 'punting' category): The number of punts taken. |
| `punting_punt_yards` | double | Punt Yards (ESPN 'punting' category): The total amount of yardage of punts. |
| `punting_long_punt` | double | Long Punt (ESPN 'punting' category): The distance of the longest punt. |
| `punting_gross_avg_punt_yards` | double | Gross Average Punt Yards (ESPN 'punting' category): The average gross number of yards per punt. |
| `punting_net_avg_punt_yards` | double | Net Average Punt Yards (ESPN 'punting' category): The average net number of yards per punt. |
| `punting_punts_blocked` | double | Punts Blocked (ESPN 'punting' category): The number of times a punt was blocked. |
| `punting_punts_inside20` | double | Punts Inside 20 (ESPN 'punting' category): The number of times a punt is downed inside the 20. |
| `punting_touchbacks` | double | Touchbacks (ESPN 'punting' category): The number of kickoffs that result in touchbacks. |
| `punting_fair_catches` | double | Fair Catches (ESPN 'punting' category): The number of fair catches. |
| `punting_punt_returns` | double | Punt Returns (ESPN 'punting' category): The number of times a punt was returned. |
| `punting_punt_return_yards` | double | Punt Return Yards (ESPN 'punting' category): The total amount of yardage of punt returns. |
| `punting_avg_punt_return_yards` | double | Average Punt Return Yards (ESPN 'punting' category): The average number of yards per punt return. |

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
