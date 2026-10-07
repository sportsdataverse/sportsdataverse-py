---
title: AHL — additional Python functions
sidebar_label: Additional functions
description: "AHL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# AHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.ahl`
not covered by the generated API-endpoint reference above.

## HockeyTech / LeagueStat

### ahl_game_corsi {#ahl_game_corsi}

`ahl_game_corsi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

Player-level on-ice Corsi and Fenwick for a single AHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player on ice for a shot attempt: `player_id` (String), `corsi_for` / `corsi_against` / `corsi_for_pct`, `fenwick_for` / `fenwick_against` / `fenwick_for_pct`, `corsi_includes_missed` (Boolean), `toi_seconds` (Int64) and `corsi_for_per60` (Float64, null without time on ice). A pandas DataFrame when `return_as_pandas` is True.

### ahl_game_shifts {#ahl_game_shifts}

`ahl_game_shifts(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

Parsed shift stints for a single AHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per shift: `game_id` and `player_id` (Int64), `first_name`, `last_name`, `jersey_number`, `home` (Int64, 1 = home), `period`, `start_time` / `end_time` / `length` (clock strings), `start_s` / `end_s` (Int64 seconds) and the `goal_on_shift` / `penalty_on_shift` flags. A pandas DataFrame when `return_as_pandas` is True.

### ahl_game_summary {#ahl_game_summary}

`ahl_game_summary(game_id: 'int') -> 'dict'`

AHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |

**Returns**

`game` (one row: `game_id`, `date`, `status`, `venue`, `attendance`, both teams and scores), `goals` (one row per goal with the scorer, both assists and the plus / minus skaters), `penalties` (one row per penalty), `shots_by_period` (`side`, `period`, `shots`) and `three_stars`. When the league denies the summary view, the event frames are empty and `game` is a `game_id` stub row.

### ahl_leaders {#ahl_leaders}

`ahl_leaders(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

AHL statistical leaders for a given season.

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
| `team_logo_small` | character | URL of the small-format team logo image for the AHL leader-board entry. |
| `stat_formatted` | character | The leader's value in the type_formatted category as a display string; for the Points and Goals leaderboards requested here it is a whole-number count (e.g. '29'), and '0' on every row when the season has no games yet. |
| `type_formatted` | character | Human-readable label for the statistical category driving the leader-board ranking (e.g., "Goals", "Points"). |
| `photo` | character | URL to the player photo. |
| `photo_small` | character | URL of the small-format headshot image for the AHL leader-board player. |
| `position` | character | Player position. |
| `division` | character | Division identifier. |

### ahl_pbp {#ahl_pbp}

`ahl_pbp(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

AHL play-by-play — one row per event, fully enriched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per event (shot, goal, penalty, faceoff, hit, goalie change ...): `game_id`, `event`, `team_id`, `period_of_game`, `time_of_period`, rink `x_coord` / `y_coord` (Float64, hockeytech_a canvas), the primary / second / third player and goalie ids and names, the plus / minus skaters on a goal, game metadata from the game summary, and derived `shot_distance` / `shot_angle` / `scoring_chance` and the `on_ice_home` / `on_ice_away` skaters from the shift feed. Player ids are Float64 here. Some leagues (USHL, MJHL) publish only goals, penalties and goalie changes, with no coordinates. A pandas DataFrame when `return_as_pandas` is True.

### ahl_player_stats {#ahl_player_stats}

`ahl_player_stats(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

AHL player season stats across all seasons.

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
| `sopt_track_faceoffs` | character | Flag indicating whether faceoffs are tracked for this player's season entry in the AHL shot-on-post statistics. |
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

### ahl_player_toi {#ahl_player_toi}

`ahl_player_toi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

Per-player time-on-ice totals for a single AHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player: `player_id` (Int64), `first_name`, `last_name`, `toi_seconds` (Int64), `num_shifts` and `avg_shift_s` (Float64). A pandas DataFrame when `return_as_pandas` is True.

### ahl_schedule {#ahl_schedule}

`ahl_schedule(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

AHL schedule — one row per game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per game: `game_id`, `game_date`, `game_status`, `home_team` / `home_team_id` / `home_score`, `away_team` / `away_team_id` / `away_score`, `venue`, `season_id` and `game_type` (all String). With no season given, the feed's whole recent window. A pandas DataFrame when `return_as_pandas` is True.

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

### ahl_standings {#ahl_standings}

`ahl_standings(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

AHL standings — one row per team.

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
| `shootout_losses` | character | Shootout losses. |
| `regulation_wins` | character | Wins in regulation. |
| `row` | character |  |
| `points` | integer | Total points (goals + assists). |
| `penalty_minutes` | character | Penalty minutes. |
| `streak` | character | Current streak value. |
| `goals_for` | character | Goals for. |
| `goals_against` | character | Goals against. |
| `games_remaining` | character | Games remaining in the season. |
| `percentage` | character | Points percentage earned by the team (points divided by maximum possible points), expressed as a decimal between 0 and 1. |
| `overall_rank` | character |  |
| `games_played` | character | Games played. |
| `team_rank` | integer | Team rank in the standings. |
| `past_10` | character | Win-loss-overtime record across the team's most recent 10 games, typically formatted as W-L-OTL. |
| `team` | character | Team name. |

### ahl_team_roster {#ahl_team_roster}

`ahl_team_roster(team_id: 'int', season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

AHL team roster for a given team + season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  | The HockeyTech team id. |
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per rostered player: `player_id`, `person_id`, names (`first_name`, `last_name`, `display_name`), `position`, `tp_jersey_number`, `shoots` / `catches`, `height` / `weight`, `birthdate`, home and birth places, `rookie`, `veteran_status`, `draft_status` and `player_image` (String, as the feed ships them). A pandas DataFrame when `return_as_pandas` is True.

### ahl_teams {#ahl_teams}

`ahl_teams(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

AHL teams for a given season.

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

### ahl_season_id {#ahl_season_id}

`ahl_season_id(return_as_pandas: 'bool' = False) -> 'Any'`

All AHL seasons with end-year + game-type labels.

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

### most_recent_ahl_season {#most_recent_ahl_season}

`most_recent_ahl_season() -> 'int'`

Newest AHL regular season as an end-year integer: the highest `season_yr` of a regular season that is not a one-off event, so a preseason listed first is not a default.

**Returns**

The newest regular season's END year (2026 = the 2025-26 season).
