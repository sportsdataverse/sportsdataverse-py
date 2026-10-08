---
title: OHL — additional Python functions
sidebar_label: Additional functions
description: "OHL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# OHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.ohl`
not covered by the generated API-endpoint reference above.

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

One row per player on ice for a shot attempt: `player_id` (String), `corsi_for` / `corsi_against` / `corsi_for_pct`, `fenwick_for` / `fenwick_against` / `fenwick_for_pct`, `corsi_includes_missed` (Boolean), `toi_seconds` (Int64) and `corsi_for_per60` (Float64, null without time on ice). A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `corsi_for` | integer | Total shot attempts (goals, saves, missed shots, and blocked shots) directed toward the opposing team while the player was on the ice in the OHL game. |
| `corsi_against` | integer | Total shot attempts (goals, saves, missed shots, and blocked shots) directed against the player's team while that player was on the ice in the OHL game. |
| `corsi_for_pct` | double | Share of all shot attempts while the player was on the ice that were directed toward the opponent, expressed as a percentage (Corsi For / (Corsi For + Corsi Against)). |
| `fenwick_for` | integer | Unblocked shot attempts (goals, saves, and missed shots only) directed toward the opposing team while the player was on the ice. |
| `fenwick_against` | integer | Unblocked shot attempts (goals, saves, and missed shots only) directed against the player's team while that player was on the ice. |
| `fenwick_for_pct` | double | Share of all unblocked shot attempts while the player was on the ice that were directed toward the opponent, expressed as a percentage (Fenwick For / (Fenwick For + Fenwick Against)). |
| `corsi_includes_missed` | logical | Boolean flag indicating whether missed shots are included in the Corsi totals for this record. |
| `toi_seconds` | integer | Total time on ice for the player during the game, recorded in seconds. |
| `corsi_for_per60` | double | The player's Corsi For rate normalized to a 60-minute pace, enabling comparison across players with different ice times. |

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

### ohl_game_summary {#ohl_game_summary}

`ohl_game_summary(game_id: 'int') -> 'dict'`

OHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |

**Returns**

`game` (one row: `game_id`, `date`, `status`, `venue`, `attendance`, both teams and scores), `goals` (one row per goal with the scorer, both assists and the plus / minus skaters), `penalties` (one row per penalty), `shots_by_period` (`side`, `period`, `shots`) and `three_stars`. When the league denies the summary view, the event frames are empty and `game` is a `game_id` stub row.

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

One row per ranked skater (points and goals leaders): `rank` (Int64), `player_id`, `name`, `jersey_number`, `position`, `team_id` / `team_name` / `team_code`, `stat_formatted` and `type_formatted` (String), plus photo and logo URLs. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank of the streak. |
| `player_id` | character | Unique player identifier. |
| `jersey_number` | character | Jersey number. |
| `name` | character | Team mascot name. |
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Team name. |
| `team_code` | character | Team abbreviation. |
| `team_logo` | character | URL to the team logo image. |
| `team_logo_small` | character | URL of the small-format team logo image for the player's OHL club. |
| `stat_formatted` | character | The leader's value in the type_formatted category as a display string; for the Points and Goals leaderboards requested here it is a whole-number count (e.g. '29'), and '0' on every row when the season has no games yet. |
| `type_formatted` | character | Human-readable label describing the statistical category for which the player appears on the leaders list (e.g., 'Points', 'Goals', 'Save Percentage'). |
| `photo` | character | URL to the player photo. |
| `photo_small` | character | URL of a small-format headshot image of the player from the OHL HockeyTech feed. |
| `position` | character | Player position. |
| `division` | character | Division identifier. |

### ohl_pbp {#ohl_pbp}

`ohl_pbp(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

OHL play-by-play — one row per event, fully enriched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per event (shot, goal, penalty, faceoff, hit, goalie change ...): `game_id`, `event`, `team_id`, `period_of_game`, `time_of_period`, rink `x_coord` / `y_coord` (Float64, raw 600×300 coordinates), the primary / second / third player and goalie ids and names, the plus / minus skaters on a goal, game metadata from the game summary, and derived `shot_distance` / `shot_angle` / `scoring_chance` and the `on_ice_home` / `on_ice_away` skaters from the shift feed. Player ids are Float64 here. Some leagues (USHL, MJHL) publish only goals, penalties and goalie changes, with no coordinates. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `event` | character | Event description label. |
| `team_id` | character | Unique team identifier. |
| `period_of_game` | character | Period in which the event occurred. |
| `time_of_period` | character | Elapsed time within the period (MM:SS). |
| `x_coord` | double | Transformed x-coordinate of the event (feet scale). |
| `y_coord` | double | Transformed y-coordinate of the event (feet scale). |
| `player_id` | double | Unique player identifier. |
| `player_name_first` | character | Primary player first name. |
| `player_name_last` | character | Primary player last name. |
| `player_position` | character | Primary player position. |
| `goal` | logical | Flag for whether the event was a goal. |
| `is_goal_twin` | logical | Boolean flag marking a shot row that duplicates a goal event on the same play — the HockeyTech feed emits both a shot row and a goal row for every goal. Shot rows (twins included) match the official boxscore shots-on-goal totals; drop rows with this flag set to get a deduplicated event stream. |
| `goalie_id` | double | Goalie identifier on the play. |
| `goalie_first` | character | Goalie first name. |
| `goalie_last` | character | Goalie last name. |
| `home_win` | character | Whether the home player won the faceoff. |
| `player_team_id` | character | Unique team identifier of the primary player. |
| `event_type` | character | Standardized event type code. |
| `shot_quality` | character | Shot quality descriptor. |
| `player_two_id` | double | Second player's unique identifier. |
| `player_two_name_first` | character | Second player first name. |
| `player_two_name_last` | character | Second player last name. |
| `player_two_position` | character | Second player position. |
| `penalty_length` | character | Penalty length in minutes. |
| `power_play` | character | Whether the event occurred on a power play. |
| `empty_net` | character | Whether the net was empty. |
| `game_winner` | character | Whether the goal was the game-winning goal. |
| `penalty_shot` | character | Whether the goal came on a penalty shot. |
| `insurance` | character | Whether the goal was an insurance goal. |
| `short_handed` | character | Whether the event occurred while short-handed. |
| `player_three_id` | double | Third player's unique identifier. |
| `player_three_name_first` | character | Third player first name. |
| `player_three_name_last` | character | Third player last name. |
| `player_three_position` | character | Third player position. |
| `plus_player_one_id` | double | On-ice plus player one unique identifier. |
| `plus_player_one_first` | character | On-ice plus player one first name. |
| `plus_player_one_last` | character | On-ice plus player one last name. |
| `plus_player_one_position` | character | On-ice plus player one position. |
| `plus_player_two_id` | double | On-ice plus player two unique identifier. |
| `plus_player_two_first` | character | On-ice plus player two first name. |
| `plus_player_two_last` | character | On-ice plus player two last name. |
| `plus_player_two_position` | character | On-ice plus player two position. |
| `plus_player_three_id` | double | On-ice plus player three unique identifier. |
| `plus_player_three_first` | character | On-ice plus player three first name. |
| `plus_player_three_last` | character | On-ice plus player three last name. |
| `plus_player_three_position` | character | On-ice plus player three position. |
| `plus_player_four_id` | double | On-ice plus player four unique identifier. |
| `plus_player_four_first` | character | On-ice plus player four first name. |
| `plus_player_four_last` | character | On-ice plus player four last name. |
| `plus_player_four_position` | character | On-ice plus player four position. |
| `plus_player_five_id` | double | On-ice plus player five unique identifier. |
| `plus_player_five_first` | character | On-ice plus player five first name. |
| `plus_player_five_last` | character | On-ice plus player five last name. |
| `plus_player_five_position` | character | On-ice plus player five position. |
| `minus_player_one_id` | double | On-ice minus player one unique identifier. |
| `minus_player_one_first` | character | On-ice minus player one first name. |
| `minus_player_one_last` | character | On-ice minus player one last name. |
| `minus_player_one_position` | character | On-ice minus player one position. |
| `minus_player_two_id` | double | On-ice minus player two unique identifier. |
| `minus_player_two_first` | character | On-ice minus player two first name. |
| `minus_player_two_last` | character | On-ice minus player two last name. |
| `minus_player_two_position` | character | On-ice minus player two position. |
| `minus_player_three_id` | double | On-ice minus player three unique identifier. |
| `minus_player_three_first` | character | On-ice minus player three first name. |
| `minus_player_three_last` | character | On-ice minus player three last name. |
| `minus_player_three_position` | character | On-ice minus player three position. |
| `minus_player_four_id` | double | On-ice minus player four unique identifier. |
| `minus_player_four_first` | character | On-ice minus player four first name. |
| `minus_player_four_last` | character | On-ice minus player four last name. |
| `minus_player_four_position` | character | On-ice minus player four position. |
| `minus_player_five_id` | double | On-ice minus player five unique identifier. |
| `minus_player_five_first` | character | On-ice minus player five first name. |
| `minus_player_five_last` | character | On-ice minus player five last name. |
| `minus_player_five_position` | character | On-ice minus player five position. |
| `game_date` | character | Game date. |
| `game_season` | integer | Season (concluding year, YYYY). |
| `game_season_id` | character | HockeyTech season identifier. |
| `home_team` | character | Home team name. |
| `home_team_id` | character | Home team identifier. |
| `away_team` | character | Away team name. |
| `away_team_id` | character | Away team identifier. |
| `x_coord_original` | double | Original raw x-coordinate from the feed. |
| `y_coord_original` | double | Original raw y-coordinate from the feed. |
| `x_coord_neutral` | double | Neutral-zone-centered x-coordinate. |
| `y_coord_neutral` | double | Neutral-zone-centered y-coordinate. |
| `x_coord_fixed` | double | Fixed-orientation x-coordinate. |
| `y_coord_fixed` | double | Fixed-orientation y-coordinate. |
| `x_coord_right` | double | Right-orientation x-coordinate. |
| `y_coord_right` | double | Right-orientation y-coordinate. |
| `x_coord_vertical` | double | Vertical-orientation x-coordinate. |
| `y_coord_vertical` | double | Vertical-orientation y-coordinate. |
| `minute_start` | integer | Minute mark of the period when the event started. |
| `second_start` | integer | Second mark of the period when the event started. |
| `clock` | character | Game clock time remaining (MM:SS). |
| `sec_from_start` | integer | Seconds elapsed since the start of the game. |
| `shot_distance` | double | Distance of the shot from the net. |
| `shot_angle` | double | Angle of the shot relative to the net. |
| `scoring_chance` | logical | Boolean flag indicating whether this play was classified as a scoring chance by the HockeyTech data feed. |
| `on_ice_home` | character | Jersey numbers or player IDs of home-team skaters on the ice at the time of this play. |
| `on_ice_away` | character | Jersey numbers or player IDs of away-team skaters on the ice at the time of this play. |

### ohl_player_stats {#ohl_player_stats}

`ohl_player_stats(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

OHL player season stats across all seasons.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  | The HockeyTech player id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per season (and team) the player played: `season_id`, `season_name`, `playoff`, `team_id` / `team_name` / `team_code`, `games_played`, `goals`, `assists`, `points`, `plus_minus`, `penalty_minutes`, power-play / short-handed / shootout splits, `shots`, `faceoff_wins` / `faceoff_attempts`, `ice_time` and `stat_type` (String, as the feed ships them). A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `season_id` | character | Season identifier. |
| `season_name` | character | Full season name (e.g., "2024-25 Regular Season"). |
| `shortname` | character | Player short name. |
| `playoff` | character | Whether the row is playoff statistics. |
| `career` | character | Whether this is a career-stats season. |
| `sopt_track_faceoffs` | character | Flag indicating whether faceoff tracking is enabled for this player's statistical record in the HockeyTech system. |
| `max_start_date` | character | Latest game start date for the season. |
| `veteran_status` | character | Player veteran status. |
| `veteran` | character | Whether the player is a veteran. |
| `jersey_number` | character | Jersey number. |
| `goals` | character | Goals scored. |
| `games_played` | character | Games played. |
| `assists` | character | Assists. |
| `points` | character | Total points (goals + assists). |
| `plus_minus` | character | Plus/minus rating. |
| `penalty_minutes` | character | Penalty minutes. |
| `power_play_goals` | character | Power-play goals. |
| `power_play_assists` | character | Power-play assists. |
| `shots` | character | Shots on goal. |
| `shootout_attempts` | character | Shootout attempts. |
| `shootout_goals` | character | Shootout goals. |
| `shootout_percentage` | character | Shootout scoring percentage. |
| `shooting_percentage` | character | Shooting percentage. |
| `shootout_winning_goals` | character | Shootout game-winning goals. |
| `points_per_game` | character | Points per game. |
| `short_handed_goals` | character | Short-handed goals. |
| `short_handed_assists` | character | Short-handed assists. |
| `game_winning_goals` | character | Game-winning goals. |
| `game_tieing_goals` | character | Game-tying goals. |
| `faceoff_wins` | character | Faceoff wins. |
| `faceoff_attempts` | character | Faceoff attempts. |
| `faceoff_pct` | character | Faceoff win percentage. |
| `hits` | character | Hits. |
| `shots_on` | character | Shots on goal count. |
| `shots_wide` | character | Count of shot attempts by the player that missed the net wide, as tracked by OHL shot-location data. |
| `team_name` | character | Team name. |
| `team_code` | character | Team abbreviation. |
| `team_city` | character | Team city. |
| `team_nickname` | character | Team nickname. |
| `team_id` | character | Unique team identifier. |
| `active` | character | Whether athlete is currently active. |
| `first_goals` | character | First goals of a game. |
| `insurance_goals` | character | Insurance goals. |
| `overtime_goals` | character | Overtime goals. |
| `unassisted_goals` | character | Unassisted goals. |
| `empty_net_goals` | character | Empty-net goals. |
| `penalty_minutes_per_game` | character | Penalty minutes per game. |
| `division` | character | Division identifier. |
| `ice_time` | character | Total ice time. |
| `ice_time_minutes_seconds` | character | Ice time in minutes and seconds. |
| `shots_blocked_by_player` | character | Shots blocked by the player. |
| `stat_type` | character | Statistic type ("regular"/"playoff"). |

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
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `toi_seconds` | integer | Total time on ice for the player during the game or tracked period, expressed in seconds. |
| `num_shifts` | integer | Total number of shifts the player took during the game or tracked period. |
| `avg_shift_s` | double | Average duration of the player's individual shifts during the game or season, measured in seconds. |

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

One row per game: `game_id`, `game_date`, `game_status`, `home_team` / `home_team_id` / `home_score`, `away_team` / `away_team_id` / `away_score`, `venue`, `season_id` and `game_type` (all String). Only the requested season's games: a regular season, its playoffs and its preseason are separate season ids. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date. |
| `game_status` | character | Game status text. |
| `home_team` | character | Home team name. |
| `home_team_id` | character | Home team identifier. |
| `home_score` | character | Home team final score. |
| `away_team` | character | Away team name. |
| `away_team_id` | character | Away team identifier. |
| `away_score` | character | Away team final score. |
| `venue` | character | Venue where the game was played. |
| `season_id` | character | Season identifier. |
| `game_type` | character | Game type the row belongs to. |

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
| `team_code` | character | Team abbreviation. |
| `wins` | character | Wins. |
| `losses` | character | Losses. |
| `ot_losses` | character | Overtime losses. |
| `ot_wins` | character | Overtime wins. |
| `shootout_wins` | character | Shootout wins. |
| `shootout_losses` | character | Shootout losses. |
| `regulation_wins` | character | Wins in regulation. |
| `row` | character |  |
| `points` | integer | Total points (goals + assists). |
| `penalty_minutes` | character | Penalty minutes. |
| `streak` | character | Current streak value. |
| `goals_for` | character | Goals for. |
| `goals_against` | character | Goals against. |
| `goals_diff` | character | Net goal differential for the team (goals for minus goals against) displayed as a signed string. |
| `percentage` | character | Team points percentage expressed as a string, calculated as points earned divided by maximum possible points. |
| `overall_rank` | character |  |
| `games_played` | character | Games played. |
| `team_rank` | integer | Team rank in the standings. |
| `past_10` | character | Record over the team's most recent 10 games (feed header 'Past 10 Games') as a hyphenated string: W-L-OTL-SOL when four parts are shipped (e.g. '2-0-0-1'), W-L-OTL when three are (e.g. '0-2-1'). |
| `team` | character | Team name. |

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

One row per rostered player: `player_id`, `person_id`, names (`first_name`, `last_name`, `display_name`), `position`, `tp_jersey_number`, `shoots` / `catches`, `height` / `weight`, `birthdate`, home and birth places, `rookie`, `veteran_status`, `draft_status` and `player_image` (String, as the feed ships them). A pandas DataFrame when `return_as_pandas` is True.

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

One row per team: `team_id`, `team_name`, `team_code`, `team_nickname`, `team_label`, `division` and `team_logo` (String). A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `team_name` | character | Team name. |
| `team_id` | character | Unique team identifier. |
| `team_code` | character | Team abbreviation. |
| `team_nickname` | character | Team nickname. |
| `team_label` | character | Short city label. |
| `division` | character | Division identifier. |
| `team_logo` | character | URL to the team logo image. |

## Dates and seasons

### most_recent_ohl_season {#most_recent_ohl_season}

`most_recent_ohl_season() -> 'int'`

Newest OHL regular season as an end-year integer: the highest `season_yr` of a regular season that is not a one-off event, so a preseason listed first is not a default.

**Returns**

The newest regular season's END year (2026 = the 2025-26 season).

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
| `season_id` | integer | Season identifier. |
| `season_name` | character | Full season name (e.g., "2024-25 Regular Season"). |
| `season_short` | character | Short season name. |
| `career` | character | Whether this is a career-stats season. |
| `playoff` | character | Whether the row is playoff statistics. |
| `start_date` | character | Season start date. |
| `end_date` | character | Season end date. |
| `season_yr` | integer | End year of the season the row belongs to, read from its name: "2025-26", "2025/26" and "2025-2026" are 2026, "26-27" is 2027, a compact "2425" is 2025. A preseason or exhibition named with the one year it starts in belongs to the next season ("2026 Pre-season" is 2027). Null when the name holds no year. |
| `game_type_label` | character | Game type read from the season name, first match wins: "preseason" (pre-season, preseason), "playoffs" (playoff, post), "exhibition", else "regular". One-off events such as all-star games are labelled "regular" too; season resolution skips them. |
