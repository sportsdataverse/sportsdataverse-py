---
title: OHL — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# OHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.ohl`
not covered by the generated API-endpoint reference above.

## Utilities & helpers

### `most_recent_ohl_season() -> 'int'` {#most_recent_ohl_season}

Most-recent OHL season as an end-year integer (max `season_yr`), or 2026.

## Other

### `ohl_game_corsi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#ohl_game_corsi}

Player-level on-ice Corsi and Fenwick for a single OHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


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

### `ohl_game_shifts(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#ohl_game_shifts}

Parsed shift stints for a single OHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `ohl_game_summary(game_id: 'int') -> 'dict'` {#ohl_game_summary}

OHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |

### `ohl_leaders(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#ohl_leaders}

OHL statistical leaders for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


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
| `stat_formatted` | character | Human-readable string representation of the leader's statistical value for display purposes (e.g., '42', '1.85', '93.5%'). |
| `type_formatted` | character | Human-readable label describing the statistical category for which the player appears on the leaders list (e.g., 'Points', 'Goals', 'Save Percentage'). |
| `photo` | character | URL to the player photo. |
| `photo_small` | character | URL of a small-format headshot image of the player from the OHL HockeyTech feed. |
| `position` | character | Player position. |
| `division` | character | Division identifier. |

### `ohl_pbp(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#ohl_pbp}

OHL play-by-play — one row per event, fully enriched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `event` | character | Event description label. |
| `team_id` | character | Unique team identifier. |
| `period_of_game` | character | Period in which the event occurred. |
| `time_of_period` | character | Elapsed time within the period (MM:SS). |
| `x_coord` | double | Transformed x-coordinate of the event. |
| `y_coord` | double | Transformed y-coordinate of the event. |
| `player_id` | double | Unique player identifier. |
| `player_name_first` | character | Primary player first name. |
| `player_name_last` | character | Primary player last name. |
| `player_position` | character | Primary player position. |
| `goal` | logical | Flag for whether the event was a goal. |
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

### `ohl_player_stats(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#ohl_player_stats}

OHL player season stats across all seasons.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


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

### `ohl_player_toi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#ohl_player_toi}

Per-player time-on-ice totals for a single OHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `toi_seconds` | integer | Total time on ice for the player during the game or tracked period, expressed in seconds. |
| `num_shifts` | integer | Total number of shifts the player took during the game or tracked period. |
| `avg_shift_s` | double | Average duration of the player's individual shifts during the game or season, measured in seconds. |

### `ohl_schedule(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#ohl_schedule}

OHL schedule — one row per game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


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

### `ohl_season_id(return_as_pandas: 'bool' = False) -> 'Any'` {#ohl_season_id}

All OHL seasons with end-year + game-type labels.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season_id` | integer | Season identifier. |
| `season_name` | character | Full season name (e.g., "2024-25 Regular Season"). |
| `season_short` | character | Short season name. |
| `career` | character | Whether this is a career-stats season. |
| `playoff` | character | Whether the row is playoff statistics. |
| `start_date` | character | Season start date. |
| `end_date` | character | Season end date. |
| `season_yr` | integer | Year derived from the season name (concluding year). |
| `game_type_label` | character | Game type: "preseason", "regular", or "playoffs". |

### `ohl_standings(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#ohl_standings}

OHL standings — one row per team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


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
| `row` | character | Row index within the game grouping (sequencing helper). |
| `points` | character | Total points (goals + assists). |
| `penalty_minutes` | character | Penalty minutes. |
| `streak` | character | Current streak value. |
| `goals_for` | character | Goals for. |
| `goals_against` | character | Goals against. |
| `goals_diff` | character | Net goal differential for the team (goals for minus goals against) displayed as a signed string. |
| `percentage` | character | Team points percentage expressed as a string, calculated as points earned divided by maximum possible points. |
| `overall_rank` | character | Overall recruit ranking (top recruits only; may be `NA`). |
| `games_played` | character | Games played. |
| `team_rank` | integer | Team rank in the standings. |
| `past_10` | character | Team record over the most recent ten games, formatted as a W-L or W-OTL-L string. |
| `team` | character | Team name. |

### `ohl_team_roster(team_id: 'int', season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#ohl_team_roster}

OHL team roster for a given team + season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  |  |
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

### `ohl_teams(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#ohl_teams}

OHL teams for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `team_name` | character | Team name. |
| `team_id` | character | Unique team identifier. |
| `team_code` | character | Team abbreviation. |
| `team_nickname` | character | Team nickname. |
| `team_label` | character | Short city label. |
| `division` | character | Division identifier. |
| `team_logo` | character | URL to the team logo image. |
