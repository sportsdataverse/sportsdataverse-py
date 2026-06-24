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
| `rank` | integer | Rank of the streak. |
| `player_id` | character | Unique player identifier. |
| `jersey_number` | character | Jersey number. |
| `name` | character | Team mascot name. |
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Team name. |
| `team_code` | character | Team abbreviation. |
| `team_logo` | character | URL to the team logo image. |
| `team_logo_small` | character | URL of the small-format team logo image for the player's QMJHL club. |
| `stat_formatted` | character | Human-readable string representation of the leader's statistical value for display purposes (e.g., '42', '1.85', '93.5%'). |
| `type_formatted` | character | Human-readable label describing the statistical category for which the player appears on the leaders list (e.g., 'Points', 'Goals', 'Save Percentage'). |
| `photo` | character | URL to the player photo. |
| `photo_small` | character | URL of a small-format headshot image of the player from the QMJHL HockeyTech feed. |
| `position` | character | Player position. |
| `division` | character | Division identifier. |

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
| `season_id` | character | Season identifier. |
| `season_name` | character | Full season name (e.g., "2024-25 Regular Season"). |
| `shortname` | character | Player short name. |
| `playoff` | character | Whether the row is playoff statistics. |
| `career` | character | Whether this is a career-stats season. |
| `sopt_track_faceoffs` | character | Flag or value indicating whether faceoff tracking is enabled for this player's statistical record in the QMJHL HockeyTech system. |
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
| `shots_wide` | character | Number of shots the player directed wide of the net during the season, as tracked in the QMJHL HockeyTech feed. |
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

### `qmjhl_season_id(return_as_pandas: 'bool' = False) -> 'Any'` {#qmjhl_season_id}

All QMJHL seasons with end-year + game-type labels.

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
| `team_code` | character | Team abbreviation. |
| `wins` | character | Wins. |
| `losses` | character | Losses. |
| `ot_losses` | character | Overtime losses. |
| `ot_wins` | character | Overtime wins. |
| `shootout_wins` | character | Shootout wins. |
| `shootout_losses` | character | Shootout losses. |
| `row` | character | Row index within the game grouping (sequencing helper). |
| `points` | character | Total points (goals + assists). |
| `penalty_minutes` | character | Penalty minutes. |
| `streak` | character | Current streak value. |
| `goals_for` | character | Goals for. |
| `goals_against` | character | Goals against. |
| `goals_diff` | character | Goals-for minus goals-against differential for the team through the current point in the QMJHL season. |
| `percentage` | character | Points percentage representing the fraction of available points earned by the team in the QMJHL standings. |
| `overall_rank` | character | Overall recruit ranking (top recruits only; may be `NA`). |
| `games_played` | character | Games played. |
| `team_rank` | integer | Team rank in the standings. |
| `past_10` | character | Win-loss-overtime record string for the team over its most recent ten games (e.g., "7-2-1"). |
| `team` | character | Team name. |

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
| `id` | character | Unique player identifier. |
| `person_id` | character | Unique person identifier. |
| `active` | character | Whether athlete is currently active. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `phonetic_name` | character | Phonetic spelling of the player name. |
| `display_name` | character | Player display name. |
| `shoots` | character | Shooting hand. |
| `hometown` | character | Prospect hometown. |
| `homeprov` | character | Player home province/state. |
| `homecntry` | character | Player home country. |
| `homeplace` | character | Player home place description. |
| `birthtown` | character | Player birth town. |
| `birthprov` | character | Player birth province/state. |
| `birthcntry` | character | Player birth country. |
| `birthplace` | character | Combined birthplace string (typically city and country) for the player as stored in the HockeyTech roster feed. |
| `height` | character | Player height in inches. |
| `weight` | character | Player weight in pounds. |
| `height_hyphenated` | character | Player height formatted as a hyphenated feet-inches string (e.g., "6-1") as returned by the HockeyTech feed. |
| `hidden` | character | Flag indicating whether this roster entry is suppressed from public-facing displays in the HockeyTech system. |
| `current_team` | character | Name or identifier of the team to which the player is currently assigned within the HockeyTech system. |
| `player_id` | character | Unique player identifier. |
| `status` | character | Status string (e.g. captain markers). |
| `birthdate` | character | Date of birth. |
| `birthdate_year` | character | Player birth year. |
| `rawbirthdate` | character | Player's date of birth as a raw string in the format returned by the HockeyTech feed (e.g., "YYYY-MM-DD"). |
| `latest_team_id` | character | Most recent team identifier. |
| `veteran_status` | character | Player veteran status. |
| `veteran_description` | character | Text label describing the player's veteran status or classification under QMJHL roster rules. |
| `team_name` | character | Team name. |
| `division` | character | Division identifier. |
| `tp_jersey_number` | character | Jersey number assigned to the player on the QMJHL team's official roster (may differ from current game-night number). |
| `rookie` | character | Whether the player is a rookie. |
| `position_id` | character | Official position identifier. |
| `position` | character | Player position. |
| `nhlteam` | character | Name or abbreviation of the NHL team that holds the player's NHL rights, if applicable. |
| `player_id_1` | character | Primary alternate player identifier used by the HockeyTech system to cross-reference player records. |
| `is_rookie` | character | Whether the player is a rookie. |
| `h` | character | Hits. |
| `w` | character | Wins. |
| `draft_status` | character | Description of the player's NHL or league draft eligibility or draft history (e.g., "Drafted", "Eligible"). |
| `name` | character | Team mascot name. |
| `player_image` | character | URL of the player's headshot or profile image as stored in the HockeyTech roster feed. |
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
| `team_name` | character | Team name. |
| `team_id` | character | Unique team identifier. |
| `team_code` | character | Team abbreviation. |
| `team_nickname` | character | Team nickname. |
| `team_label` | character | Short city label. |
| `division` | character | Division identifier. |
| `team_logo` | character | URL to the team logo image. |
