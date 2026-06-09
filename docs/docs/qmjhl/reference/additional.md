---
title: QMJHL — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# QMJHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.qmjhl`
not covered by the generated API-endpoint reference above.

## Utilities & helpers

### `most_recent_qmjhl_season() -> 'int'` {#most_recent_qmjhl_season}

Most-recent QMJHL season as an end-year integer (max `season_yr`), or 2026.

## Other

### `qmjhl_game_corsi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#qmjhl_game_corsi}

Player-level on-ice Corsi and Fenwick for a single QMJHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `qmjhl_game_shifts(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#qmjhl_game_shifts}

Parsed shift stints for a single QMJHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `qmjhl_game_summary(game_id: 'int') -> 'dict'` {#qmjhl_game_summary}

QMJHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |

### `qmjhl_leaders(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#qmjhl_leaders}

QMJHL statistical leaders for a given season.

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

### `qmjhl_pbp(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#qmjhl_pbp}

QMJHL play-by-play — one row per event, fully enriched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `qmjhl_player_stats(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#qmjhl_player_stats}

QMJHL player season stats across all seasons.

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

### `qmjhl_player_toi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#qmjhl_player_toi}

Per-player time-on-ice totals for a single QMJHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `qmjhl_schedule(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#qmjhl_schedule}

QMJHL schedule — one row per game.

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

### `qmjhl_season_id(return_as_pandas: 'bool' = False) -> 'Any'` {#qmjhl_season_id}

All QMJHL seasons with end-year + game-type labels.

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

### `qmjhl_standings(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#qmjhl_standings}

QMJHL standings — one row per team.

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

### `qmjhl_team_roster(team_id: 'int', season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#qmjhl_team_roster}

QMJHL team roster for a given team + season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  |  |
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `id` | character | ID of the player in the 'name' column. |
| `person_id` | character | Unique player identifier (V3 endpoints). |
| `active` | character | TRUE if the row represents an active record (player / team / season). |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `phonetic_name` | character | Phonetic spelling of the player name. |
| `display_name` | character | Display name. |
| `shoots` | character | Shooting hand. |
| `hometown` | character | Prospect hometown. |
| `homeprov` | character | Player home province/state. |
| `homecntry` | character | Player home country. |
| `homeplace` | character | Player home place description. |
| `birthtown` | character | Player birth town. |
| `birthprov` | character | Player birth province/state. |
| `birthcntry` | character | Player birth country. |
| `birthplace` | character |  |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | character | Player weight in pounds. |
| `height_hyphenated` | character |  |
| `hidden` | character |  |
| `current_team` | character |  |
| `player_id` | character | Unique player identifier. |
| `status` | character | Status label. |
| `birthdate` | character | Date of birth. |
| `birthdate_year` | character | Player birth year. |
| `rawbirthdate` | character |  |
| `latest_team_id` | character | Most recent team identifier. |
| `veteran_status` | character | Player veteran status. |
| `veteran_description` | character |  |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `division` | character | Team division. |
| `tp_jersey_number` | character |  |
| `rookie` | character | Whether the player is a rookie. |
| `position_id` | character | Unique position identifier. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `nhlteam` | character |  |
| `player_id_1` | character |  |
| `is_rookie` | character | Whether the player is a rookie. |
| `h` | character | Hits. |
| `w` | character | Wins. |
| `draft_status` | character |  |
| `name` | character | Display name. |
| `player_image` | character |  |
| `catches` | character | Catching hand (goalies). |

### `qmjhl_teams(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#qmjhl_teams}

QMJHL teams for a given season.

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
