---
title: PWHL — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# PWHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.pwhl`
not covered by the generated API-endpoint reference above.

## Dataset loaders

### `load_pwhl_games(return_as_pandas: 'bool' = False)` {#load_pwhl_games}

Load the PWHL games-in-data-repo manifest (no `seasons` argument).

Mirrors fastRhockey (R) `load_pwhl_games()` which reads a manifest of every
PWHL game that has processed data in the data repository.

Tries the sportsdataverse-data release asset first; falls back to the raw
fastRhockey-data GitHub path.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame of all games in the data repository.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `season` | integer | Season year. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `game_status` | character | Game status label. |
| `home_team` | character | Home team name. |
| `home_team_id` | character | Unique identifier for the home team. |
| `away_team` | character | Away team name. |
| `away_team_id` | character | Unique identifier for the away team. |
| `home_score` | character | Home team score at the time of the play. |
| `away_score` | character | Away team score at the time of the play. |
| `winner` | character | Winner. |
| `venue` | character | Venue name. |
| `venue_url` | character | URL for the venue. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `game_json` | logical | Whether processed game JSON is available. |
| `game_json_url` | character | URL to the processed game JSON. |
| `PBP` | logical | Whether play-by-play data is available. |
| `player_box` | logical | Whether player box score data is available. |
| `skater_box` | logical | Whether skater box data is available. |
| `goalie_box` | logical | Whether goalie box data is available. |
| `team_box` | logical | Whether team box score data is available. |
| `game_info` | logical | Whether game info data is available. |
| `game_rosters` | logical | Whether game rosters data is available. |
| `scoring_summary` | logical | Whether scoring summary data is available. |
| `penalty_summary` | logical | Whether penalty summary data is available. |
| `three_stars` | logical | Whether three stars data is available. |
| `officials` | logical | Whether officials data is available. |
| `shots_by_period` | logical | Whether shots-by-period data is available. |
| `shootout` | logical | Whether shootout data is available. |

**Example**

```python
>>> load_pwhl_games()
```

### `load_pwhl_goalie_box(seasons, return_as_pandas: 'bool' = False)` {#load_pwhl_goalie_box}

Alias of load_pwhl_goalie_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_pwhl_player_box(seasons, return_as_pandas: 'bool' = False)` {#load_pwhl_player_box}

Alias of load_pwhl_player_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_pwhl_schedule(seasons, return_as_pandas: 'bool' = False)` {#load_pwhl_schedule}

Alias of load_pwhl_schedules() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_pwhl_skater_box(seasons, return_as_pandas: 'bool' = False)` {#load_pwhl_skater_box}

Alias of load_pwhl_skater_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_pwhl_team_box(seasons, return_as_pandas: 'bool' = False)` {#load_pwhl_team_box}

Alias of load_pwhl_team_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

## Utilities & helpers

### `most_recent_pwhl_season() -> 'int'` {#most_recent_pwhl_season}

Most-recent PWHL season as an end-year integer (max `season_yr`).

## Other

### `pwhl_game_corsi(game_id: 'Any', return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_game_corsi}

Player-level on-ice Corsi and Fenwick for a single PWHL game.

Computes shot-attempt counts for every player found on ice during a
shot/blocked_shot/goal event, then joins their time-on-ice so per-60
rates are available.

**Corsi/Fenwick note**: the HockeyTech feed has no missed-shot event,
so both metrics are proxies that count only shot + blocked_shot + goal.
Every output row carries `corsi_includes_missed = False`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Any` |  | HockeyTech game identifier (integer or string). |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

One row per on-ice player with columns: - `player_id` (Utf8) - `corsi_for`, `corsi_against` (Int64) - `corsi_for_pct` (Float64) - `fenwick_for`, `fenwick_against` (Int64) - `fenwick_for_pct` (Float64) - `toi_seconds` (Int64, from shifts; null if player not in shift data) - `corsi_for_per60` (Float64) - `corsi_includes_missed` (Boolean, always False)

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
| `toi_seconds` | double |  |
| `corsi_for_per60` | double |  |

### `pwhl_game_shifts(game_id: 'Any', return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_game_shifts}

Parsed shift stints for a single PWHL game.

Calls the HockeyTech `modulekit/gameshifts` endpoint and returns one
row per player-shift stint via `~sportsdataverse.hockeytech._parsers.parse_shifts`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Any` |  | HockeyTech game identifier (integer or string). |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

Columns include `player_id`, `first_name`, `last_name`, `home`, `period`, `start_time`, `end_time`, `start_s`, `end_s`, `goal_on_shift`, `penalty_on_shift`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `home` | integer | Home. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `start_time` | character | Kickoff time in eastern time zone. |
| `end_time` | character | End time (local). |
| `length` | character | Length of the streak in games. |
| `start_s` | integer |  |
| `end_s` | double |  |
| `goal_on_shift` | integer |  |
| `penalty_on_shift` | integer |  |

### `pwhl_game_summary(game_id: 'int') -> 'dict'` {#pwhl_game_summary}

PWHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |

### `pwhl_leaders(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_leaders}

PWHL statistical leaders for a given season.

NOTE: the `leadersExtended` endpoint uses `season_id` (integer) to filter
by season, not `season` (name string). The resolved integer is passed as the
`season_id` param so historical-season requests return results.

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

### `pwhl_player_box(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_player_box}

PWHL player box score for a single game.

NOTE: returns an empty frame pending a captured fixture + correct endpoint wiring
(A1.8 follow-up); not yet functional.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `pwhl_player_game_log(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_player_game_log}

PWHL player game-by-game log.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `pwhl_player_info(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_player_info}

PWHL player biographical info.

NOTE: returns an empty frame pending a captured fixture + correct endpoint wiring
(A1.8 follow-up); not yet functional.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `pwhl_player_search(name: 'str', return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_player_search}

Search for PWHL players by name.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `person_id` | character | Unique player identifier (V3 endpoints). |
| `player_id` | character | Unique player identifier. |
| `active` | character | TRUE if the row represents an active record (player / team / season). |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `phonetic_name` | character | Phonetic spelling of the player name. |
| `shoots` | character | Shooting hand. |
| `catches` | character | Catching hand (goalies). |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | character | Player weight in pounds. |
| `rawbirthdate` | character |  |
| `birthdate` | character | Date of birth. |
| `birthtown` | character | Player birth town. |
| `birthprov` | character | Player birth province/state. |
| `birthcntry` | character | Player birth country. |
| `team_id` | character | Unique team identifier. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `role_id` | character |  |
| `season_id` | character | Unique season identifier. |
| `role_name` | character |  |
| `all_roles` | character |  |
| `last_team_name` | character |  |
| `last_team_code` | character |  |
| `division` | character | Team division. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `profile_image` | character |  |
| `score` | character | Final score string. |
| `last_active_date` | character |  |

### `pwhl_player_stats(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_player_stats}

PWHL player season stats across all seasons.

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

### `pwhl_player_toi(game_id: 'Any', return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_player_toi}

Per-player time-on-ice totals for a single PWHL game.

Fetches shifts via `pwhl_game_shifts` then aggregates via
`~sportsdataverse.hockeytech._analytics.player_toi`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Any` |  | HockeyTech game identifier (integer or string). |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

One row per player with `player_id`, `first_name`, `last_name`, `toi_seconds`, `num_shifts`, `avg_shift_s`, sorted by `toi_seconds` descending.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `toi_seconds` | double |  |
| `num_shifts` | integer |  |
| `avg_shift_s` | double |  |

### `pwhl_playoff_bracket(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_playoff_bracket}

PWHL playoff bracket for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

### `pwhl_schedule(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_schedule}

PWHL schedule — one row per game (matches fastRhockey `pwhl_schedule`).

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

### `pwhl_scorebar(return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_scorebar}

PWHL live scorebar (today ± 3 days).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `id` | character | ID of the player in the 'name' column. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `game_number` | character | Game number within a doubleheader. |
| `game_letter` | character |  |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `quick_score` | character |  |
| `date` | character | Date in YYYY-MM-DD format. |
| `flo_core_event_id` | character |  |
| `flo_live_event_id` | character |  |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `game_date_iso8601` | character |  |
| `scheduled_time` | character |  |
| `scheduled_formatted_time` | character |  |
| `timezone` | character | Time zone in which the venue resides (i.e. Eastern Time -> "America/New_York"). |
| `ticket_url` | character |  |
| `home_id` | character | Unique identifier for home. |
| `home_code` | character |  |
| `home_city` | character | Hometown of the athlete. |
| `home_nickname` | character |  |
| `home_long_name` | character |  |
| `home_division` | character | Home team division. |
| `home_goals` | character | Home goals in the period. |
| `home_audio_url` | character |  |
| `home_video_url` | character |  |
| `home_webcast_url` | character |  |
| `visitor_id` | character |  |
| `visitor_code` | character |  |
| `visitor_city` | character |  |
| `visitor_nickname` | character |  |
| `visitor_long_name` | character |  |
| `visiting_division` | character | Visiting team division. |
| `visitor_goals` | character |  |
| `visitor_audio_url` | character |  |
| `visitor_video_url` | character |  |
| `visitor_webcast_url` | character |  |
| `period` | character | Period of the game (1-4 quarters; 5+ for OT). |
| `period_name_short` | character |  |
| `period_name_long` | character |  |
| `game_clock` | character | Game clock. |
| `game_summary_url` | character |  |
| `home_wins` | character | Home team's wins. |
| `home_regulation_losses` | character |  |
| `home_ot_losses` | character | Home overtime losses. |
| `home_shootout_losses` | character |  |
| `visitor_wins` | character |  |
| `visitor_regulation_losses` | character |  |
| `visitor_ot_losses` | character |  |
| `visitor_shootout_losses` | character |  |
| `game_status` | character | Game status label. |
| `intermission` | character |  |
| `game_status_string` | character |  |
| `game_status_string_long` | character |  |
| `ord` | character |  |
| `venue_name` | character | Venue name. |
| `venue_location` | character |  |
| `league_name` | character | League name. |
| `league_code` | character |  |
| `timezone_short` | character |  |
| `home_logo` | character | Home team logo URL. |
| `visitor_logo` | character |  |
| `flo_hockey_url` | character |  |
| `combined_client_code` | character |  |

### `pwhl_season_id(return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_season_id}

All PWHL seasons with end-year + game-type labels (HockeyTech `seasons`).

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

### `pwhl_standings(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_standings}

PWHL standings — one row per team.

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
| `losses` | character | Total losses. |
| `regulation_wins` | character | Wins in regulation. |
| `points` | character | Points scored. |
| `goals_for` | character | Goals for. |
| `goals_against` | character | Goals against. |
| `non_reg_wins` | character | Non-regulation wins. |
| `non_reg_losses` | character | Non-regulation losses. |
| `games_remaining` | character | Games remaining in the season. |
| `percentage` | character |  |
| `overall_rank` | character | Overall recruit ranking (top recruits only; may be `NA`). |
| `games_played` | character | Games played. |
| `team_rank` | integer | Team rank in the standings. |
| `team` | character | Team-side label or team identifier. |
| `wins` | integer | Total wins. |

### `pwhl_stats(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, position: 'str' = 'skaters', return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_stats}

PWHL aggregate stats by season and position.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` |  |
| `season_id` | `Optional[int]` | `None` |  |
| `position` | `str` | `'skaters'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `shortname` | character | Player short name. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `name` | character | Display name. |
| `phonetic_name` | character | Phonetic spelling of the player name. |
| `active` | character | TRUE if the row represents an active record (player / team / season). |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | character | Player weight in pounds. |
| `last_years_club` | character | Player's club in the previous season. |
| `age` | character | Player age (in years). |
| `shoots` | character | Shooting hand. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `suspension_games_remaining` | character | Suspension games remaining. |
| `suspension_indefinite` | character | Whether the suspension is indefinite. |
| `rookie` | character | Whether the player is a rookie. |
| `veteran` | character | Whether the player is a veteran. |
| `draft_eligible` | character | Whether the player is draft eligible. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_code` | character | Internal team code. |
| `team_id` | character | Unique team identifier. |
| `division` | character | Team division. |
| `birthdate` | character | Date of birth. |
| `birthdate_year` | character | Player birth year. |
| `hometown` | character | Prospect hometown. |
| `homeprov` | character | Player home province/state. |
| `homecntry` | character | Player home country. |
| `birthtown` | character | Player birth town. |
| `birthprov` | character | Player birth province/state. |
| `birthcntry` | character | Player birth country. |
| `hometownprov` | character | Player hometown and province/state. |
| `homeplace` | character | Player home place description. |
| `games_played` | character | Games played. |
| `game_winning_goals` | character | Game-winning goals. |
| `game_tieing_goals` | character | Game-tying goals. |
| `first_goals` | character | First goals of a game. |
| `insurance_goals` | character | Insurance goals. |
| `unassisted_goals` | character | Unassisted goals. |
| `empty_net_goals` | character | Empty-net goals. |
| `overtime_goals` | character | Overtime goals. |
| `ice_time` | character | Total ice time. |
| `ice_time_avg` | character | Average ice time. |
| `goals` | character | Goals scored. |
| `shots` | character | Shots on goal. |
| `loose_ball_recoveries` | character | Loose ball recoveries. |
| `caused_turnovers` | character | Turnovers caused. |
| `turnovers` | character | Total turnovers. |
| `hits` | character | Hits. |
| `shots_blocked_by_player` | character | Shots blocked by the player. |
| `ice_time_minutes_seconds` | character | Ice time in minutes and seconds. |
| `shooting_percentage` | character | Shooting percentage. |
| `assists` | character | Total assists. |
| `points` | character | Points scored. |
| `points_per_game` | character | Points per game. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `penalty_minutes` | character | Penalty minutes. |
| `penalty_minutes_per_game` | character | Penalty minutes per game. |
| `ice_time_per_game_avg` | character | Average ice time per game. |
| `hits_per_game_avg` | character | Average hits per game. |
| `minor_penalties` | character | Minor penalties. |
| `major_penalties` | character | Major penalties. |
| `power_play_goals` | character | Power-play goals. |
| `power_play_assists` | character | Power-play assists. |
| `power_play_points` | character | Power play points. |
| `short_handed_goals` | character | Short-handed goals. |
| `short_handed_assists` | character | Short-handed assists. |
| `short_handed_points` | character | Short-handed points. |
| `shootout_goals` | character | Shootout goals. |
| `shootout_attempts` | character | Shootout attempts. |
| `shootout_winning_goals` | character | Shootout game-winning goals. |
| `shootout_games_played` | character | Games played that went to a shootout. |
| `faceoff_attempts` | character | Faceoff attempts. |
| `faceoff_wins` | character | Faceoff wins. |
| `faceoff_pct` | character | Faceoff win percentage. |
| `faceoff_wa` | character | Faceoff wins-to-attempts metric. |
| `shots_on` | character | Shots on goal count. |
| `shootout_percentage` | character | Shootout scoring percentage. |
| `latest_team_id` | character | Most recent team identifier. |
| `num_teams` | character | Number of teams the player has played for. |
| `logo` | character | Team or league logo URL. |
| `rank` | integer | Position of the school within the poll for the given week (1 = top-ranked). |
| `player_page_link` | character | URL to the player page. |
| `player_image` | character |  |
| `namelink` | character | HTML link for the player name. |
| `teamlink` | character | HTML link for the team. |
| `team_breakdown` | integer | Per-team statistical breakdown. |
| `is_total` | double | Whether the row is a season total. |

### `pwhl_streaks(return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_streaks}

Current PWHL player/team streaks.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

### `pwhl_team_roster(team_id: 'int', season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_team_roster}

PWHL team roster for a given team + season.

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

### `pwhl_teams(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_teams}

PWHL teams for a given season.

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

### `pwhl_transactions(return_as_pandas: 'bool' = False) -> 'Any'` {#pwhl_transactions}

PWHL roster transactions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |
