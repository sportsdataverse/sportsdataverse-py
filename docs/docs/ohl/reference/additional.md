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

Most-recent OHL season as an end-year integer (max `season_yr`), or 0.

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
| `corsi_for` | integer |  |
| `corsi_against` | integer |  |
| `corsi_for_pct` | double |  |
| `fenwick_for` | integer |  |
| `fenwick_against` | integer |  |
| `fenwick_for_pct` | double |  |
| `corsi_includes_missed` | logical |  |
| `toi_seconds` | integer |  |
| `corsi_for_per60` | double |  |

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
| `rank` | integer | Position of the school within the poll for the given week (1 = top-ranked). |
| `player_id` | character | Unique player identifier. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `name` | character | Display name. |
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_code` | character | Internal team code. |
| `team_logo` | character | Team logo image URL. |
| `team_logo_small` | character |  |
| `stat_formatted` | character |  |
| `type_formatted` | character |  |
| `photo` | character | URL to the player photo. |
| `photo_small` | character |  |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `division` | character | Team division. |

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
| `event` | character | Binary flag indicating the row is a counted game event (excludes end markers). |
| `team_id` | character | Unique team identifier. |
| `period_of_game` | character | Period in which the event occurred. |
| `time_of_period` | character | Elapsed time within the period (MM:SS). |
| `x_coord` | double | Transformed x-coordinate of the event. |
| `y_coord` | double | Transformed y-coordinate of the event. |
| `player_id` | double | Unique player identifier. |
| `player_name_first` | character | Primary player first name. |
| `player_name_last` | character | Primary player last name. |
| `player_position` | character | Position of the player accordinng to NGS |
| `goal` | logical | Flag for whether the event was a goal. |
| `goalie_id` | double | Goalie identifier on the play. |
| `goalie_first` | character | Goalie first name. |
| `goalie_last` | character | Goalie last name. |
| `home_win` | character | Home team's win. |
| `player_team_id` | character | Unique team identifier of the primary player. |
| `event_type` | character | Event / play type code (V2 PBP). |
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
| `game_date` | character | Game date (YYYY-MM-DD). |
| `game_season` | integer | Season (concluding year, YYYY). |
| `game_season_id` | character | HockeyTech season identifier. |
| `home_team` | character | Home team name. |
| `home_team_id` | character | Unique identifier for the home team. |
| `away_team` | character | Away team name. |
| `away_team_id` | character | Unique identifier for the away team. |
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
| `clock` | character | Game clock value. |
| `sec_from_start` | integer | Seconds elapsed since the start of the game. |
| `shot_distance` | double | Shot distance from the basket, in feet. |
| `shot_angle` | double | Angle of the shot relative to the net. |
| `scoring_chance` | logical |  |
| `on_ice_home` | character |  |
| `on_ice_away` | character |  |

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
| `season_id` | character | Unique season identifier. |
| `season_name` | character | Full season name (e.g., "2024-25 Regular Season"). |
| `shortname` | character | Player short name. |
| `playoff` | character | Whether the row is playoff statistics. |
| `career` | character | Whether this is a career-stats season. |
| `sopt_track_faceoffs` | character |  |
| `max_start_date` | character | Latest game start date for the season. |
| `veteran_status` | character | Player veteran status. |
| `veteran` | character | Whether the player is a veteran. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `goals` | character | Goals scored. |
| `games_played` | character | Games played. |
| `assists` | character | Total assists. |
| `points` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
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
| `shots_wide` | character |  |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_code` | character | Internal team code. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_nickname` | character | Team nickname. |
| `team_id` | character | Unique team identifier. |
| `active` | character | TRUE if the row represents an active record (player / team / season). |
| `first_goals` | character | First goals of a game. |
| `insurance_goals` | character | Insurance goals. |
| `overtime_goals` | character | Overtime goals. |
| `unassisted_goals` | character | Unassisted goals. |
| `empty_net_goals` | character | Empty-net goals. |
| `penalty_minutes_per_game` | character | Penalty minutes per game. |
| `division` | character | Team division. |
| `ice_time` | character | Total ice time. |
| `ice_time_minutes_seconds` | character | Ice time in minutes and seconds. |
| `shots_blocked_by_player` | character | Shots blocked by the player. |
| `stat_type` | character | Stat type code (e.g. "win", "loss"). |

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
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `toi_seconds` | integer |  |
| `num_shifts` | integer |  |
| `avg_shift_s` | double |  |

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
| `game_date` | character | Game date (YYYY-MM-DD). |
| `game_status` | character | Game status label. |
| `home_team` | character | Home team name. |
| `home_team_id` | character | Unique identifier for the home team. |
| `home_score` | character | Home team score at the time of the play. |
| `away_team` | character | Away team name. |
| `away_team_id` | character | Unique identifier for the away team. |
| `away_score` | character | Away team score at the time of the play. |
| `venue` | character | Venue name. |
| `season_id` | character | Unique season identifier. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |

### `ohl_season_id(return_as_pandas: 'bool' = False) -> 'Any'` {#ohl_season_id}

All OHL seasons with end-year + game-type labels.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season_id` | integer | Unique season identifier. |
| `season_name` | character | Full season name (e.g., "2024-25 Regular Season"). |
| `season_short` | character | Short season name. |
| `career` | character | Whether this is a career-stats season. |
| `playoff` | character | Whether the row is playoff statistics. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `end_date` | character | End date (YYYY-MM-DD). |
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
| `team_code` | character | Internal team code. |
| `wins` | character | Total wins. |
| `losses` | character | Total losses. |
| `ot_losses` | character | Overtime losses. |
| `ot_wins` | character | Overtime wins. |
| `shootout_wins` | character | Shootout wins. |
| `shootout_losses` | character | Shootout losses. |
| `regulation_wins` | character | Wins in regulation. |
| `row` | character | Row index within the game grouping (sequencing helper). |
| `points` | character | Points scored. |
| `penalty_minutes` | character | Penalty minutes. |
| `streak` | character | Current streak (e.g. 'W3' for three-game win streak). |
| `goals_for` | character | Goals for. |
| `goals_against` | character | Goals against. |
| `goals_diff` | character |  |
| `percentage` | character |  |
| `overall_rank` | character | Overall recruit ranking (top recruits only; may be `NA`). |
| `games_played` | character | Games played. |
| `team_rank` | integer | Team rank in the standings. |
| `past_10` | character |  |
| `team` | character | Team-side label or team identifier. |

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
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_id` | character | Unique team identifier. |
| `team_code` | character | Internal team code. |
| `team_nickname` | character | Team nickname. |
| `team_label` | character | Short city label. |
| `division` | character | Team division. |
| `team_logo` | character | Team logo image URL. |
