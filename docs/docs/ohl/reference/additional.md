---
title: OHL — additional Python functions
sidebar_label: Additional functions
description: "OHL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# OHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.hockey.ohl` that are not
generated API-endpoint wrappers (those have their own reference pages).

## HockeyTech / LeagueStat

### ohl_game_corsi {#ohl_game_corsi}

`ohl_game_corsi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

Player-level on-ice Corsi and Fenwick for a single OHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player on ice for a shot attempt: `player_id` (Int64), `corsi_for` / `corsi_against` / `corsi_for_pct`, `fenwick_for` / `fenwick_against` / `fenwick_for_pct`, `corsi_includes_missed` (Boolean), `toi_seconds` (Int64) and `corsi_for_per60` (Float64, null without time on ice). A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `corsi_for` | Int64 | Total shot attempts (goals, saves, missed shots, and blocked shots) directed toward the opposing team while the player was on the ice in the OHL game. |
| `corsi_against` | Int64 | Total shot attempts (goals, saves, missed shots, and blocked shots) directed against the player's team while that player was on the ice in the OHL game. |
| `corsi_for_pct` | Float64 | Share of all shot attempts while the player was on the ice that were directed toward the opponent, expressed as a percentage (Corsi For / (Corsi For + Corsi Against)). |
| `fenwick_for` | Int64 | Unblocked shot attempts (goals, saves, and missed shots only) directed toward the opposing team while the player was on the ice. |
| `fenwick_against` | Int64 | Unblocked shot attempts (goals, saves, and missed shots only) directed against the player's team while that player was on the ice. |
| `fenwick_for_pct` | Float64 | Share of all unblocked shot attempts while the player was on the ice that were directed toward the opponent, expressed as a percentage (Fenwick For / (Fenwick For + Fenwick Against)). |
| `corsi_includes_missed` | Boolean | Boolean flag indicating whether missed shots are included in the Corsi totals for this record. |
| `toi_seconds` | Int64 | Total time on ice for the player during the game, recorded in seconds. |
| `corsi_for_per60` | Float64 | The player's Corsi For rate normalized to a 60-minute pace, enabling comparison across players with different ice times. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

### ohl_game_shifts {#ohl_game_shifts}

`ohl_game_shifts(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

Parsed shift stints for a single OHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per shift: `game_id` and `player_id` (Int64), `first_name`, `last_name`, `jersey_number`, `home` (Int64, 1 = home), `period`, `start_time` / `end_time` / `length` (clock strings), `start_s` / `end_s` (Int64 seconds) and the `goal_on_shift` / `penalty_on_shift` flags. A pandas DataFrame when `return_as_pandas` is True.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

### ohl_game_summary {#ohl_game_summary}

`ohl_game_summary(game_id: 'int') -> 'dict'`

OHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |

**Returns**

`game` (one row: `game_id`, `date`, `status`, `venue`, `attendance`, both teams and scores), `goals` (one row per goal with the scorer, both assists and the plus / minus skaters), `penalties` (one row per penalty), `shots_by_period` (`side`, `period`, `shots`) and `three_stars`. When the league denies the summary view, the event frames are empty and `game` is a `game_id` stub row.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

### ohl_leaders {#ohl_leaders}

`ohl_leaders(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

OHL statistical leaders for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per ranked skater (points and goals leaders): `rank` (Int64), `player_id`, `name`, `jersey_number`, `position`, `team_id` / `team_name` / `team_code`, `stat_formatted` and `type_formatted` (`player_id` / `team_id` Int64, the rest String), plus photo and logo URLs. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the streak. |
| `player_id` | Int64 | Unique player identifier. |
| `jersey_number` | String | Jersey number. |
| `name` | String | Team mascot name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Team name. |
| `team_code` | String | Team abbreviation. |
| `team_logo` | String | URL to the team logo image. |
| `team_logo_small` | String | URL of the small-format team logo image for the player's OHL club. |
| `stat_formatted` | String | The leader's value in the type_formatted category as a display string; for the Points and Goals leaderboards requested here it is a whole-number count (e.g. '29'), and '0' on every row when the season has no games yet. |
| `type_formatted` | String | Human-readable label describing the statistical category for which the player appears on the leaders list (e.g., 'Points', 'Goals', 'Save Percentage'). |
| `photo` | String | URL to the player photo. |
| `photo_small` | String | URL of a small-format headshot image of the player from the OHL HockeyTech feed. |
| `position` | String | Player position. |
| `division` | String | Division identifier. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

### ohl_pbp {#ohl_pbp}

`ohl_pbp(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

OHL play-by-play — one row per event, fully enriched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per event (shot, goal, penalty, faceoff, hit, goalie change ...): `game_id`, `event`, `team_id`, `period_of_game`, `time_of_period`, rink `x_coord` / `y_coord` (Float64, feet, centre ice at (0, 0); the raw 600×300 feed canvas values are `x_coord_original` / `y_coord_original`), the feet frames `x_coord_fixed` / `y_coord_fixed` (home team shoots right) and `x_coord_right` / `y_coord_right` (every team shoots right), the primary / second / third player and goalie ids (Int64) and names, the plus / minus skaters on a goal, game metadata from the game summary, and derived `shot_distance` / `shot_angle` / `scoring_chance` and the `on_ice_home` / `on_ice_away` skaters from the shift feed. Some leagues (USHL, MJHL) publish only goals, penalties and goalie changes, with no coordinates. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Unique game identifier. |
| `event` | String | Event description label. |
| `team_id` | Int64 | Unique team identifier. |
| `period_of_game` | String | Period in which the event occurred. |
| `time_of_period` | String | Elapsed time within the period (MM:SS). |
| `x_coord` | Float64 | Transformed x-coordinate of the event (feet scale). |
| `y_coord` | Float64 | Transformed y-coordinate of the event (feet scale). |
| `player_id` | Int64 | Unique player identifier. |
| `player_name_first` | String | Primary player first name. |
| `player_name_last` | String | Primary player last name. |
| `player_position` | String | Primary player position. |
| `goal` | Boolean | Flag for whether the event was a goal. |
| `is_goal_twin` | Boolean | Boolean flag marking a shot row that duplicates a goal event on the same play — the HockeyTech feed emits both a shot row and a goal row for every goal. Shot rows (twins included) match the official boxscore shots-on-goal totals; drop rows with this flag set to get a deduplicated event stream. |
| `goalie_id` | Int64 | Goalie identifier on the play. |
| `goalie_first` | String | Goalie first name. |
| `goalie_last` | String | Goalie last name. |
| `home_win` | String | Whether the home player won the faceoff. |
| `player_team_id` | Int64 | Unique team identifier of the primary player. |
| `event_type` | String | Standardized event type code. |
| `shot_quality` | String | Shot quality descriptor. |
| `player_two_id` | Int64 | Second player's unique identifier. |
| `player_two_name_first` | String | Second player first name. |
| `player_two_name_last` | String | Second player last name. |
| `player_two_position` | String | Second player position. |
| `penalty_length` | String | Penalty length in minutes. |
| `power_play` | String | Whether the event occurred on a power play. |
| `empty_net` | String | Whether the net was empty. |
| `game_winner` | String | Whether the goal was the game-winning goal. |
| `penalty_shot` | String | Whether the goal came on a penalty shot. |
| `insurance` | String | Whether the goal was an insurance goal. |
| `short_handed` | String | Whether the event occurred while short-handed. |
| `player_three_id` | Int64 | Third player's unique identifier. |
| `player_three_name_first` | String | Third player first name. |
| `player_three_name_last` | String | Third player last name. |
| `player_three_position` | String | Third player position. |
| `plus_player_one_id` | Int64 | On-ice plus player one unique identifier. |
| `plus_player_one_first` | String | On-ice plus player one first name. |
| `plus_player_one_last` | String | On-ice plus player one last name. |
| `plus_player_one_position` | String | On-ice plus player one position. |
| `plus_player_two_id` | Int64 | On-ice plus player two unique identifier. |
| `plus_player_two_first` | String | On-ice plus player two first name. |
| `plus_player_two_last` | String | On-ice plus player two last name. |
| `plus_player_two_position` | String | On-ice plus player two position. |
| `plus_player_three_id` | Int64 | On-ice plus player three unique identifier. |
| `plus_player_three_first` | String | On-ice plus player three first name. |
| `plus_player_three_last` | String | On-ice plus player three last name. |
| `plus_player_three_position` | String | On-ice plus player three position. |
| `plus_player_four_id` | Int64 | On-ice plus player four unique identifier. |
| `plus_player_four_first` | String | On-ice plus player four first name. |
| `plus_player_four_last` | String | On-ice plus player four last name. |
| `plus_player_four_position` | String | On-ice plus player four position. |
| `plus_player_five_id` | Int64 | On-ice plus player five unique identifier. |
| `plus_player_five_first` | String | On-ice plus player five first name. |
| `plus_player_five_last` | String | On-ice plus player five last name. |
| `plus_player_five_position` | String | On-ice plus player five position. |
| `minus_player_one_id` | Int64 | On-ice minus player one unique identifier. |
| `minus_player_one_first` | String | On-ice minus player one first name. |
| `minus_player_one_last` | String | On-ice minus player one last name. |
| `minus_player_one_position` | String | On-ice minus player one position. |
| `minus_player_two_id` | Int64 | On-ice minus player two unique identifier. |
| `minus_player_two_first` | String | On-ice minus player two first name. |
| `minus_player_two_last` | String | On-ice minus player two last name. |
| `minus_player_two_position` | String | On-ice minus player two position. |
| `minus_player_three_id` | Int64 | On-ice minus player three unique identifier. |
| `minus_player_three_first` | String | On-ice minus player three first name. |
| `minus_player_three_last` | String | On-ice minus player three last name. |
| `minus_player_three_position` | String | On-ice minus player three position. |
| `minus_player_four_id` | Int64 | On-ice minus player four unique identifier. |
| `minus_player_four_first` | String | On-ice minus player four first name. |
| `minus_player_four_last` | String | On-ice minus player four last name. |
| `minus_player_four_position` | String | On-ice minus player four position. |
| `minus_player_five_id` | Int64 | On-ice minus player five unique identifier. |
| `minus_player_five_first` | String | On-ice minus player five first name. |
| `minus_player_five_last` | String | On-ice minus player five last name. |
| `minus_player_five_position` | String | On-ice minus player five position. |
| `game_date` | String | Game date. |
| `game_season` | Int64 | Season (concluding year, YYYY). |
| `game_season_id` | Int64 | HockeyTech season identifier. |
| `home_team` | String | Home team name. |
| `home_team_id` | Int64 | Home team identifier. |
| `away_team` | String | Away team name. |
| `away_team_id` | Int64 | Away team identifier. |
| `x_coord_original` | Float64 | Original raw x-coordinate from the feed. |
| `y_coord_original` | Float64 | Original raw y-coordinate from the feed. |
| `x_coord_neutral` | Float64 | Neutral-zone-centered x-coordinate. |
| `y_coord_neutral` | Float64 | Neutral-zone-centered y-coordinate. |
| `x_coord_fixed` | Float64 | Fixed-orientation x-coordinate. |
| `y_coord_fixed` | Float64 | Fixed-orientation y-coordinate. |
| `x_coord_right` | Float64 | Right-orientation x-coordinate. |
| `y_coord_right` | Float64 | Right-orientation y-coordinate. |
| `x_coord_vertical` | Float64 | Vertical-orientation x-coordinate. |
| `y_coord_vertical` | Float64 | Vertical-orientation y-coordinate. |
| `minute_start` | Int64 | Minute mark of the period when the event started. |
| `second_start` | Int64 | Second mark of the period when the event started. |
| `clock` | String | Game clock time remaining (MM:SS). |
| `sec_from_start` | Int64 | Seconds elapsed since the start of the game. |
| `shot_distance` | Float64 | Distance of the shot from the net. |
| `shot_angle` | Float64 | Angle of the shot relative to the net. |
| `scoring_chance` | Boolean | Boolean flag indicating whether this play was classified as a scoring chance by the HockeyTech data feed. |
| `on_ice_home` | String | Jersey numbers or player IDs of home-team skaters on the ice at the time of this play. |
| `on_ice_away` | String | Jersey numbers or player IDs of away-team skaters on the ice at the time of this play. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

### ohl_player_stats {#ohl_player_stats}

`ohl_player_stats(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

OHL player season stats across all seasons.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  | The HockeyTech player id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per season (and team) the player played: `season_id`, `season_name`, `playoff`, `team_id` / `team_name` / `team_code`, `games_played`, `goals`, `assists`, `points`, `plus_minus`, `penalty_minutes`, power-play / short-handed / shootout splits, `shots`, `faceoff_wins` / `faceoff_attempts`, `ice_time` and `stat_type`. `season_id` and `team_id` are Int64; the rest are String, as the feed ships them. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `season_id` | Int64 | Season identifier. |
| `season_name` | String | Full season name (e.g., "2024-25 Regular Season"). |
| `shortname` | String | Player short name. |
| `playoff` | String | Whether the row is playoff statistics. |
| `career` | String | Whether this is a career-stats season. |
| `sopt_track_faceoffs` | String | Flag indicating whether faceoff tracking is enabled for this player's statistical record in the HockeyTech system. |
| `max_start_date` | String | Latest game start date for the season. |
| `veteran_status` | String | Player veteran status. |
| `veteran` | String | Whether the player is a veteran. |
| `jersey_number` | String | Jersey number. |
| `goals` | String | Goals scored. |
| `games_played` | String | Games played. |
| `assists` | String | Assists. |
| `points` | String | Total points (goals + assists). |
| `plus_minus` | String | Plus/minus rating. |
| `penalty_minutes` | String | Penalty minutes. |
| `power_play_goals` | String | Power-play goals. |
| `power_play_assists` | String | Power-play assists. |
| `shots` | String | Shots on goal. |
| `shootout_attempts` | String | Shootout attempts. |
| `shootout_goals` | String | Shootout goals. |
| `shootout_percentage` | String | Shootout scoring percentage. |
| `shooting_percentage` | String | Shooting percentage. |
| `shootout_winning_goals` | String | Shootout game-winning goals. |
| `points_per_game` | String | Points per game. |
| `short_handed_goals` | String | Short-handed goals. |
| `short_handed_assists` | String | Short-handed assists. |
| `game_winning_goals` | String | Game-winning goals. |
| `game_tieing_goals` | String | Game-tying goals. |
| `faceoff_wins` | String | Faceoff wins. |
| `faceoff_attempts` | String | Faceoff attempts. |
| `faceoff_pct` | String | Faceoff win percentage. |
| `hits` | String | Hits. |
| `shots_on` | String | Shots on goal count. |
| `shots_wide` | String | Count of shot attempts by the player that missed the net wide, as tracked by OHL shot-location data. |
| `team_name` | String | Team name. |
| `team_code` | String | Team abbreviation. |
| `team_city` | String | Team city. |
| `team_nickname` | String | Team nickname. |
| `team_id` | Int64 | Unique team identifier. |
| `active` | String | Whether athlete is currently active. |
| `first_goals` | String | First goals of a game. |
| `insurance_goals` | String | Insurance goals. |
| `overtime_goals` | String | Overtime goals. |
| `unassisted_goals` | String | Unassisted goals. |
| `empty_net_goals` | String | Empty-net goals. |
| `penalty_minutes_per_game` | String | Penalty minutes per game. |
| `division` | String | Division identifier. |
| `ice_time` | String | Total ice time. |
| `ice_time_minutes_seconds` | String | Ice time in minutes and seconds. |
| `shots_blocked_by_player` | String | Shots blocked by the player. |
| `stat_type` | String | Statistic type ("regular"/"playoff"). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

### ohl_player_toi {#ohl_player_toi}

`ohl_player_toi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

Per-player time-on-ice totals for a single OHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player: `player_id` (Int64), `first_name`, `last_name`, `toi_seconds` (Int64), `num_shifts` and `avg_shift_s` (Float64). A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `toi_seconds` | Int64 | Total time on ice for the player during the game or tracked period, expressed in seconds. |
| `num_shifts` | Int64 | Total number of shifts the player took during the game or tracked period. |
| `avg_shift_s` | Float64 | Average duration of the player's individual shifts during the game or season, measured in seconds. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

### ohl_schedule {#ohl_schedule}

`ohl_schedule(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

OHL schedule — one row per game of one season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per game: `game_id`, `game_date`, `game_status`, `home_team` / `home_team_id` / `home_score`, `away_team` / `away_team_id` / `away_score`, `venue`, `season_id` and `game_type`; the `*_id` columns are Int64, the rest String. Only the requested season's games: a regular season, its playoffs and its preseason are separate season ids, so pass `season_id` (from `ohl_season_id()`) for the playoffs. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Unique game identifier. |
| `game_date` | String | Game date. |
| `game_status` | String | Game status text. |
| `home_team` | String | Home team name. |
| `home_team_id` | Int64 | Home team identifier. |
| `home_score` | String | Home team final score. |
| `away_team` | String | Away team name. |
| `away_team_id` | Int64 | Away team identifier. |
| `away_score` | String | Away team final score. |
| `venue` | String | Venue where the game was played. |
| `season_id` | Int64 | Season identifier. |
| `game_type` | String | Game type the row belongs to. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. With no season given, the seasons feed lists no regular season. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |
| `ValueError` | No season matches `season`. |

### ohl_standings {#ohl_standings}

`ohl_standings(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

OHL standings — one row per team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `team`, `team_code`, `team_rank` and `wins` (Int64), and `games_played`, `losses`, `regulation_wins`, `non_reg_wins`, `non_reg_losses`, `points`, `goals_for`, `goals_against`, `games_remaining`, `percentage` and `overall_rank` (String, as the feed ships them). A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `team_code` | String | Team abbreviation. |
| `wins` | String | Wins. |
| `losses` | String | Losses. |
| `ot_losses` | String | Overtime losses. |
| `ot_wins` | String | Overtime wins. |
| `shootout_wins` | String | Shootout wins. |
| `shootout_losses` | String | Shootout losses. |
| `regulation_wins` | String | Wins in regulation. |
| `row` | String |  |
| `points` | Int64 | Total points (goals + assists). |
| `penalty_minutes` | String | Penalty minutes. |
| `streak` | String | Current streak value. |
| `goals_for` | String | Goals for. |
| `goals_against` | String | Goals against. |
| `goals_diff` | String | Net goal differential for the team (goals for minus goals against) displayed as a signed string. |
| `percentage` | String | Team points percentage expressed as a string, calculated as points earned divided by maximum possible points. |
| `overall_rank` | String |  |
| `games_played` | String | Games played. |
| `team_rank` | Int64 | Team rank in the standings. |
| `past_10` | String | Record over the team's most recent 10 games (feed header 'Past 10 Games') as a hyphenated string: W-L-OTL-SOL when four parts are shipped (e.g. '2-0-0-1'), W-L-OTL when three are (e.g. '0-2-1'). |
| `team` | String | Team name. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

### ohl_team_roster {#ohl_team_roster}

`ohl_team_roster(team_id: 'int', season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

OHL team roster for a given team + season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  | The HockeyTech team id. |
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per rostered player: `player_id`, `person_id`, names (`first_name`, `last_name`, `display_name`), `position`, `tp_jersey_number`, `shoots` / `catches`, `height` / `weight`, `birthdate`, home and birth places, `rookie`, `veteran_status`, `draft_status` and `player_image`. The `id` / `*_id` columns are Int64; the rest are String, as the feed ships them. A pandas DataFrame when `return_as_pandas` is True.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

### ohl_teams {#ohl_teams}

`ohl_teams(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

OHL teams for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `team_id`, `team_name`, `team_code`, `team_nickname`, `team_label`, `division` and `team_logo`; `team_id` is Int64, the rest String. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `team_name` | String | Team name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_code` | String | Team abbreviation. |
| `team_nickname` | String | Team nickname. |
| `team_label` | String | Short city label. |
| `division` | String | Division identifier. |
| `team_logo` | String | URL to the team logo image. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

## Dates and seasons

### most_recent_ohl_season {#most_recent_ohl_season}

`most_recent_ohl_season() -> 'int'`

Newest OHL regular season as an end-year integer: the highest `season_yr` of a regular season that is not a one-off event, so a preseason listed first is not a default.

**Returns**

The newest regular season's END year (2026 = the 2025-26 season).

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The seasons feed lists no regular season. |
| `AssetFetchError` | The seasons feed failed. |

### ohl_season_id {#ohl_season_id}

`ohl_season_id(return_as_pandas: 'bool' = False) -> 'Any'`

All OHL seasons with end-year + game-type labels.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per season: `season_id` (Int64), `season_name`, `season_short`, `career`, `playoff`, `start_date`, `end_date`, `season_yr` (Int64, the END year) and `game_type_label`. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `season_id` | Int64 | Season identifier. |
| `season_name` | String | Full season name (e.g., "2024-25 Regular Season"). |
| `season_short` | String | Short season name. |
| `career` | String | Whether this is a career-stats season. |
| `playoff` | String | Whether the row is playoff statistics. |
| `start_date` | String | Season start date. |
| `end_date` | String | Season end date. |
| `season_yr` | Int64 | End year of the season the row belongs to, read from its name: "2025-26", "2025/26" and "2025-2026" are 2026, "26-27" is 2027, a compact "2425" is 2025. A preseason or exhibition named with the one year it starts in belongs to the next season ("2026 Pre-season" is 2027). Null when the name holds no year. |
| `game_type_label` | String | Game type read from the season name, first match wins: "preseason" (pre-season, preseason), "playoffs" (playoff, post), "exhibition", else "regular". One-off events such as all-star games are labelled "regular" too; season resolution skips them. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |
