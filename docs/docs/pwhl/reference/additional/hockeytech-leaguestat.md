---
title: "PWHL — additional Python functions — HockeyTech / LeagueStat"
sidebar_label: "HockeyTech / LeagueStat"
sidebar_position: 2
description: "PWHL — additional Python functions — HockeyTech / LeagueStat — function reference in sdv-py, the SportsDataverse Python package."
---
# PWHL — additional Python functions — HockeyTech / LeagueStat

### pwhl_game_corsi {#pwhl_game_corsi}

`pwhl_game_corsi(game_id: 'int', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

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
| `game_id` | `int` |  | HockeyTech game identifier (integer or string). |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

One row per on-ice player with columns: - `player_id` (Utf8) - `corsi_for`, `corsi_against` (Int64) - `corsi_for_pct` (Float64) - `fenwick_for`, `fenwick_against` (Int64) - `fenwick_for_pct` (Float64) - `toi_seconds` (Int64, from shifts; null if player not in shift data) - `corsi_for_per60` (Float64) - `corsi_includes_missed` (Boolean, always False)

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `corsi_for` | integer | Total shot attempts (on goal, missed, and blocked) directed toward the opponent by this team while at the tracked strength in the PWHL game. |
| `corsi_against` | integer | Total shot attempts (on goal, missed, and blocked) directed against this team while at the tracked strength in the PWHL game. |
| `corsi_for_pct` | double | Share of all tracked shot attempts belonging to this team, expressed as a decimal (Corsi For percentage). |
| `fenwick_for` | integer | Unblocked shot attempts (on goal and missed only) directed toward the opponent by this team at the tracked strength in the PWHL game. |
| `fenwick_against` | integer | Unblocked shot attempts (on goal and missed only) directed against this team at the tracked strength in the PWHL game. |
| `fenwick_for_pct` | double | Share of all unblocked shot attempts belonging to this team, expressed as a decimal (Fenwick For percentage). |
| `corsi_includes_missed` | logical | Boolean flag indicating whether missed shots are included in the Corsi totals for this record. |
| `toi_seconds` | double | Total time on ice in seconds for this team at the tracked strength during the PWHL game. |
| `corsi_for_per60` | double | This team's Corsi For rate projected to a full 60 minutes of ice time. |

### pwhl_game_info {#pwhl_game_info}

`pwhl_game_info(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

PWHL single-game metadata.

NOTE: returns an empty frame pending a captured fixture + correct endpoint wiring
(A1.8 follow-up); not yet functional.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

A zero-row frame today -- the `statviewfeed` reply is not a `SiteKit` envelope, so there is nothing to parse (see the note above). A pandas DataFrame when `return_as_pandas` is True.

### pwhl_game_shifts {#pwhl_game_shifts}

`pwhl_game_shifts(game_id: 'int', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Parsed shift stints for a single PWHL game.

Calls the HockeyTech `modulekit/gameshifts` endpoint and returns one
row per player-shift stint via `~sportsdataverse.hockeytech._parsers.parse_shifts`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | HockeyTech game identifier (integer or string). |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

Columns include `player_id`, `first_name`, `last_name`, `home`, `period`, `start_time`, `end_time`, `start_s`, `end_s`, `goal_on_shift`, `penalty_on_shift`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `jersey_number` | character | Jersey number. |
| `home` | integer | Whether the player's team was home. |
| `period` | integer | Period number. |
| `start_time` | character | Shift start time (MM:SS countdown clock). |
| `end_time` | character | Shift end time (MM:SS countdown clock). |
| `length` | character | Length of the streak in games. |
| `start_s` | integer | Shift start time in seconds elapsed from the start of the period. |
| `end_s` | double | Shift end time in seconds elapsed from the start of the period. |
| `goal_on_shift` | integer | Number of goals scored while this shift was active (0 or 1 in most cases). |
| `penalty_on_shift` | integer | Number of penalties called while this shift was active. |

### pwhl_game_summary {#pwhl_game_summary}

`pwhl_game_summary(game_id: 'int') -> 'dict'`

PWHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |

**Returns**

`game` (one row: `game_id`, `date`, `status`, `venue`, `attendance`, both teams and scores), `goals` (one row per goal with the scorer, both assists and the plus / minus skaters), `penalties` (one row per penalty), `shots_by_period` (`side`, `period`, `shots`) and `three_stars`.

### pwhl_leaders {#pwhl_leaders}

`pwhl_leaders(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL statistical leaders for a given season.

NOTE: the `leadersExtended` endpoint uses `season_id` (integer) to filter
by season, not `season` (name string). The resolved integer is passed as the
`season_id` param so historical-season requests return results.

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
| `name` | character | Player full name (e.g. 'Hilary Knight'). |
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Team name. |
| `team_code` | character | Team abbreviation. |
| `team_logo` | character | URL to the team logo image. |
| `team_logo_small` | character | URL of the small-format team logo image for the PWHL leader-board entry. |
| `stat_formatted` | character | The leader's value in the type_formatted category as a display string; for the Points and Goals leaderboards requested here it is a whole-number count (e.g. '29'), and '0' on every row when the season has no games yet. |
| `type_formatted` | character | Human-readable label for the statistical category driving the leader-board ranking (e.g., "Goals", "Save Percentage"). |
| `photo` | character | URL to the player photo. |
| `photo_small` | character | URL of the small-format headshot image for the PWHL leader-board player. |
| `position` | character | Player position. |
| `division` | character | Division identifier. |

### pwhl_pbp {#pwhl_pbp}

`pwhl_pbp(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

PWHL play-by-play — one row per event, fully enriched.

Matches fastRhockey `pwhl_pbp` column parity, adding:

- Coordinate transforms (`*_original`, `*_neutral`, `*_fixed`,
  `*_right`, `*_vertical`).
- Clock columns (`minute_start`, `second_start`, `clock`,
  `sec_from_start`).
- Shot geometry (`shot_distance`, `shot_angle`, `scoring_chance`).
- Game-meta join (`game_date`, `game_season`, `game_season_id`,
  `home_team`, `home_team_id`, `away_team`, `away_team_id`).
- On-ice player strings (`on_ice_home`, `on_ice_away`) derived from
  shift data.

Goal double-rowing: the HockeyTech feed emits both a `goal` row and a
twin `shot` row for (nearly) every goal. The twin shot row is flagged
`is_goal_twin = True` — shot rows (twins included) match the official
boxscore shots-on-goal totals, while dropping flagged rows yields a
deduplicated event stream. See `sportsdataverse.hockeytech._parsers.parse_pbp`.

The three network fetches (PBP payload, game summary meta, and shift data)
all go through the module-level `hockeytech_api` reference so tests can
monkeypatch `sportsdataverse.pwhl.pwhl_api.hockeytech_api` to intercept
all calls without touching the shared core.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per event (shot, goal, penalty, faceoff, hit, goalie change ...): `game_id`, `event`, `team_id`, `period_of_game`, `time_of_period`, rink `x_coord` / `y_coord` (Float64, raw 600×300 coordinates) and the transforms above, the primary / second / third player and goalie ids (Float64) and names, the plus / minus skaters on a goal, `is_goal_twin`, and the clock, geometry, game-meta and on-ice columns listed above. A pandas DataFrame when `return_as_pandas` is True.

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
| `is_goal_twin` | logical |  |
| `goalie_id` | double | Goalie identifier on the play. |
| `goalie_first` | character | Goalie first name. |
| `goalie_last` | character | Goalie last name. |
| `home_win` | character | Whether the home player won the faceoff. |
| `player_team_id` | character | Unique team identifier of the primary player. |
| `event_type` | character | Standardized event type code. |
| `shot_quality` | character | Shot quality descriptor. |
| `empty_net` | character | Whether the net was empty. |
| `game_winner` | character | Whether the goal was the game-winning goal. |
| `penalty_shot` | character | Whether the goal came on a penalty shot. |
| `insurance` | character | Whether the goal was an insurance goal. |
| `short_handed` | character | Whether the event occurred while short-handed. |
| `power_play` | character | Whether the event occurred on a power play. |
| `player_two_id` | double | Second player's unique identifier. |
| `player_two_name_first` | character | Second player first name. |
| `player_two_name_last` | character | Second player last name. |
| `player_two_position` | character | Second player position. |
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
| `penalty_length` | character | Penalty length in minutes. |
| `player_three_id` | double | Third player's unique identifier. |
| `player_three_name_first` | character | Third player first name. |
| `player_three_name_last` | character | Third player last name. |
| `player_three_position` | character | Third player position. |
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
| `scoring_chance` | logical | TRUE when event is a shot-type within 25 ft of the net. |
| `on_ice_home` | character | Comma-joined sorted player_ids on ice for the home team. |
| `on_ice_away` | character | Comma-joined sorted player_ids on ice for the away team. |

### pwhl_player_box {#pwhl_player_box}

`pwhl_player_box(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

PWHL player box score for a single game.

NOTE: returns an empty frame pending a captured fixture + correct endpoint wiring
(A1.8 follow-up); not yet functional.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

A zero-row frame today -- the `statviewfeed` reply is not a `SiteKit` envelope, so there is nothing to parse (see the note above). A pandas DataFrame when `return_as_pandas` is True.

### pwhl_player_game_log {#pwhl_player_game_log}

`pwhl_player_game_log(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

PWHL player game-by-game log.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  | The HockeyTech player id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per game played: `id` (the game id), `date_played`, `home_team_code` / `visiting_team_code` and names, `player_team`, `goals`, `assists`, `points`, `plus_minus`, `shots`, `hits`, `penalty_minutes`, `ice_time_minutes_seconds`, faceoff, power-play, short-handed and shootout counts. Counts arrive as strings except `points` and the percentages (Int64). A pandas DataFrame when `return_as_pandas` is True; a zero-row frame for a player with no games.

### pwhl_player_info {#pwhl_player_info}

`pwhl_player_info(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

PWHL player biographical info.

NOTE: returns an empty frame pending a captured fixture + correct endpoint wiring
(A1.8 follow-up); not yet functional.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  | The HockeyTech player id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

A zero-row frame today -- the `statviewfeed` reply is not a `SiteKit` envelope, so there is nothing to parse (see the note above). A pandas DataFrame when `return_as_pandas` is True.

### pwhl_player_search {#pwhl_player_search}

`pwhl_player_search(name: 'str', return_as_pandas: 'bool' = False) -> 'Any'`

Search for PWHL players by name.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The search text, matched against player names. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per matching player (String): `person_id`, `player_id`, `first_name` / `last_name`, `position`, `shoots` / `catches`, `height` / `weight`, `birthdate` and birthplace, `last_team_name` / `last_team_code`, `role_name`, `active`, `last_active_date` and the match `score`. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `person_id` | character | Unique person identifier. |
| `player_id` | character | Unique player identifier. |
| `active` | character | Whether athlete is currently active. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `phonetic_name` | character | Phonetic spelling of the player name. |
| `shoots` | character | Shooting hand. |
| `catches` | character | Catching hand (goalies). |
| `height` | character | Player height in inches. |
| `weight` | character | Player weight in pounds. |
| `rawbirthdate` | character | Player's birth date as a raw string in the format returned by the PWHL HockeyTech API, typically YYYY-MM-DD. |
| `birthdate` | character | Date of birth. |
| `birthtown` | character | Player birth town. |
| `birthprov` | character | Player birth province/state. |
| `birthcntry` | character | Player birth country. |
| `team_id` | character | Unique team identifier. |
| `jersey_number` | character | Jersey number. |
| `role_id` | character | Id of the person's role in the HockeyTech person record ('1' = 'Player' in the captures): player or staff, not a playing position (see position). |
| `season_id` | character | Season identifier. |
| `role_name` | character | Label of the person's role in the HockeyTech person record (e.g. 'Player'), not a playing position (see position). |
| `all_roles` | character | Pipe- or comma-delimited string listing every positional or roster role associated with the player in the PWHL HockeyTech system. |
| `last_team_name` | character | Full name of the PWHL team on which the player most recently appeared. |
| `last_team_code` | character | Short abbreviation code for the PWHL team on which the player most recently appeared. |
| `division` | character | Division identifier. |
| `position` | character | Player position. |
| `profile_image` | character | File name of the player's profile photo, not a URL (e.g. '7dea378a91cedc9d5d14b1222802322c.jpg'). |
| `score` | character | Search relevance score of the match as a decimal string (e.g. '5.911529541015625'), not a game score. |
| `last_active_date` | character | Date (YYYY-MM-DD) the feed reports the person as last active; for the active players captured it equals the capture date, so it is not the date of the player's last game. |

### pwhl_player_stats {#pwhl_player_stats}

`pwhl_player_stats(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

PWHL player season stats across all seasons.

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

### pwhl_player_toi {#pwhl_player_toi}

`pwhl_player_toi(game_id: 'int', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-player time-on-ice totals for a single PWHL game.

Fetches shifts via `pwhl_game_shifts` then aggregates via
`~sportsdataverse.hockeytech._analytics.player_toi`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | HockeyTech game identifier (integer or string). |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

One row per player with `player_id`, `first_name`, `last_name`, `toi_seconds`, `num_shifts`, `avg_shift_s`, sorted by `toi_seconds` descending.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `toi_seconds` | double | Total time on ice for the PWHL player during the game or reporting period, recorded in seconds. |
| `num_shifts` | integer | Total number of shifts the player took during the game or reporting period. |
| `avg_shift_s` | double | Average duration of a single shift for the player during the game or reporting period, measured in seconds. |

### pwhl_playoff_bracket {#pwhl_playoff_bracket}

`pwhl_playoff_bracket(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL playoff bracket for a given season.

With neither `season` nor `season_id`, the newest season that has playoffs:
the newest season overall is usually still before its playoffs, with no bracket.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). |
| `season_id` | `Optional[int]` | `None` | The HockeyTech playoff season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per playoff series: `round` / `round_name` / `round_type_name`, `series_letter` / `series_name`, `team1` / `team2` (team ids), `team1_wins` / `team2_wins` (Int64), `winner` (the winning team id, but the feed often leaves it empty even after a series ends, so read the result from the win counts), `feeder_series1` / `feeder_series2`, and `games` (a list of structs, one per game: ids, both teams, goal counts, status, date). A pandas DataFrame when `return_as_pandas` is True.

### pwhl_schedule {#pwhl_schedule}

`pwhl_schedule(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL schedule — one row per game of one season (matches fastRhockey `pwhl_schedule`).

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

### pwhl_scorebar {#pwhl_scorebar}

`pwhl_scorebar(return_as_pandas: 'bool' = False) -> 'Any'`

PWHL live scorebar (today ± 3 days).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per game in the window, as the feed ships it (String): `id`, `season_id`, `game_date` / `game_date_iso8601`, `scheduled_time`, `home_id` / `home_code` / `home_long_name` / `home_goals`, the same `visitor_*` columns, `period`, `game_clock`, `game_status` / `game_status_string`, `venue_name`, both teams' records, and broadcast URLs. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `id` | character | HockeyTech game id, the game_id the single-game functions (pwhl_pbp, pwhl_game_summary) take. |
| `season_id` | character | Season identifier. |
| `league_id` | character | HockeyTech league id of the game within the feed (e.g. '1'; AHL '4', SJHL '3'); not always the league_id the standings call sends. |
| `game_number` | character | Game number within the schedule. |
| `game_letter` | character | Playoff-series letter (A, B, C, ... one per series), set on every playoff game and empty on regular-season games. |
| `game_type` | character | Game type the row belongs to. |
| `quick_score` | character | Unused by the feed: '0' in every captured row (final and scheduled games, 17 leagues), never a score. |
| `date` | character | Game date as YYYY-MM-DD (date only, no time). |
| `flo_core_event_id` | character | FloSports core event identifier linking this PWHL game to its FloSports broadcast event record. |
| `flo_live_event_id` | character | FloSports live-stream event identifier for this PWHL game. |
| `game_date` | character | Game date. |
| `game_date_iso8601` | character | Scheduled start as an ISO 8601 date-time with UTC offset (e.g. '2023-12-04T13:00:00-05:00'). |
| `scheduled_time` | character | Raw scheduled start time for the game as returned by the HockeyTech feed, typically in HH:MM:SS format. |
| `scheduled_formatted_time` | character | Human-readable local game start time string formatted for display (e.g., "7:00 PM ET"). |
| `timezone` | character | Time zone of the scheduled start as a tz database name (e.g. 'Canada/Eastern'). |
| `ticket_url` | character | URL to the official ticketing page where fans can purchase tickets for this game. |
| `home_id` | character | HockeyTech team id of the home team (the team_id of pwhl_teams, e.g. '1' = Boston), not an ESPN id. |
| `home_code` | character | Short team code (abbreviation) for the home team (e.g., "BOS", "MIN"). |
| `home_city` | character | City name of the home team (e.g. 'Boston', 'Minnesota'). |
| `home_nickname` | character | Franchise nickname for the home team (e.g., "Fleet", "Frost"). |
| `home_long_name` | character | Full name including city and franchise for the home team (e.g., "Boston Fleet"). |
| `home_division` | character | Home team division. |
| `home_goals` | character | Goals scored by the home team in the game so far; the final score once game_status is '4'. |
| `home_audio_url` | character | URL of the home-team radio or audio broadcast stream for this game. |
| `home_video_url` | character | URL of the home-team video broadcast stream for this game. |
| `home_webcast_url` | character | URL of the home-team webcast for online viewing of this game. |
| `visitor_id` | character | HockeyTech team identifier for the visiting team in this game. |
| `visitor_code` | character | Short team code (abbreviation) for the visiting team (e.g., "NYR", "OTT"). |
| `visitor_city` | character | City name of the visiting team (e.g., "New York", "Ottawa"). |
| `visitor_nickname` | character | Franchise nickname for the visiting team (e.g., "Charge", "Sceptres"). |
| `visitor_long_name` | character | Full name including city and franchise for the visiting team (e.g., "Ottawa Charge"). |
| `visiting_division` | character | Visiting team division. |
| `visitor_goals` | character | Number of goals scored by the visiting team at the current point in the game. |
| `visitor_audio_url` | character | URL of the visiting-team radio or audio broadcast stream for this game. |
| `visitor_video_url` | character | URL of the visiting-team video broadcast stream for this game. |
| `visitor_webcast_url` | character | URL of the visiting-team webcast for online viewing of this game. |
| `period` | character | Period number. |
| `period_name_short` | character | Abbreviated name of the current or final game period (e.g., "3rd", "OT"). |
| `period_name_long` | character | Verbose name of the current or final game period (e.g., "Third Period", "Overtime"). |
| `game_clock` | character |  |
| `game_summary_url` | character | Game-summary link target, not a full URL: the bare game id in some leagues (e.g. PWHL '74') and a site-relative path in others (e.g. '/game-center/?game_id=4896'). |
| `home_wins` | character | Home team's wins in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `home_regulation_losses` | character | Home team's regulation losses in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `home_ot_losses` | character | Home team's overtime losses in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `home_shootout_losses` | character | Home team's shootout losses in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `visitor_wins` | character | Visiting team's wins in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `visitor_regulation_losses` | character | Visiting team's regulation losses in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `visitor_ot_losses` | character | Visiting team's overtime losses in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `visitor_shootout_losses` | character | Visiting team's shootout losses in its record for this game's season (season_id) as the feed reports it when fetched: the same value on every row of that team-season, not the record as of the game date. |
| `game_status` | character | Numeric game-status code as text: '1' for a scheduled game, '4' for a final. The label is in game_status_string ('Final', or the start time of a scheduled game). |
| `intermission` | character | Flag or string indicating whether the game is currently in an intermission period. |
| `game_status_string` | character | Short status label for the game's current state (e.g., "Final", "In Progress", "Scheduled"). |
| `game_status_string_long` | character | Verbose status description for the game's current state, including period or overtime context. |
| `ord` | character | Ordinal sort key used by the HockeyTech scorebar feed to order games within a day. |
| `venue_name` | character | Name of the venue. |
| `venue_location` | character | City and/or arena name indicating the physical location where the game is played. |
| `league_name` | character | League name. |
| `league_code` | character | Short code identifying the league for this scorebar record (e.g., "PWHL"). |
| `time_tbd` | character | Scorebar flag string named for a to-be-determined start time; '0' in every sampled row, so the non-zero case was not observed. |
| `date_tbd` | character | Scorebar flag string named for a to-be-determined game date; '0' in every sampled row, so the non-zero case was not observed. |
| `timezone_short` | character | Abbreviated timezone label for the game's scheduled start time (e.g., "ET", "CT"). |
| `home_logo` | character | Home team logo URL. |
| `visitor_logo` | character | URL of the logo image for the visiting team. |
| `flo_hockey_url` | character | URL to the FloHockey streaming page for this PWHL game. |
| `combined_client_code` | character | Combined league-and-client identifier string used by the HockeyTech feed to distinguish multi-tenant deployments. |

### pwhl_skater_rapm {#pwhl_skater_rapm}

`pwhl_skater_rapm(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, **kwargs: 'Any') -> "'pl.DataFrame | pd.DataFrame'"`

② PWHL skater xG RAPM -- shim over `nhl_skater_rapm` with `league='pwhl'`.

Guards on PWHL shift-chart coverage (see the module docstring); returns a documented
empty frame + `cli_warn` rather than fitting a degenerate ridge when `shifts` is
too thin.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a PWHL play-by-play frame shaped like `load_nhl_pbp_full`. |
| `shifts` | `DataFrame` |  | a PWHL shift-chart frame shaped like `load_nhl_shifts`. |
| `model_dir` | `str \| None` | `None` | booster directory passed through to the borrowed NHL xG boosters. |

**Returns**

Same schema as `nhl_skater_rapm`: `player_id:Int64, xg_rapm_off:Float64, xg_rapm_def:Float64, xg_rapm:Float64, toi_minutes:Float64`. A zero-row frame with this schema (+ a `cli_warn`) when PWHL shift coverage is insufficient.

No returns table is published for this function: no capture: it expects NHL-style play-by-play columns (secondary_type, x_fixed, strength_state) that no PWHL loader provides, so it raises ColumnNotFoundError on real PWHL games.

**Example**

```python
from sportsdataverse.pwhl.pwhl_player_impact import pwhl_skater_rapm
rapm = pwhl_skater_rapm(pbp, shifts)
```

### pwhl_skater_war {#pwhl_skater_war}

`pwhl_skater_war(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, **kwargs: 'Any') -> "'pl.DataFrame | pd.DataFrame'"`

③ PWHL GAR/WAR composite -- shim over `nhl_skater_war` with `league='pwhl'`.

Guards on PWHL shift-chart coverage (see the module docstring).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a PWHL play-by-play frame shaped like `load_nhl_pbp_full`. |
| `shifts` | `DataFrame` |  | a PWHL shift-chart frame shaped like `load_nhl_shifts`. |
| `model_dir` | `str \| None` | `None` | booster directory passed through to the borrowed NHL xG boosters. |

**Returns**

Same schema as `nhl_skater_war`: `player_id:Int64, ev_off:Float64, ev_def:Float64, pp:Float64, pk:Float64, pens:Float64, faceoffs:Float64, gar:Float64, war:Float64`. A zero-row frame with this schema (+ a `cli_warn`) when PWHL shift coverage is insufficient.

No returns table is published for this function: no capture: it expects NHL-style play-by-play columns (secondary_type, x_fixed, strength_state) that no PWHL loader provides, so it raises ColumnNotFoundError on real PWHL games.

**Example**

```python
from sportsdataverse.pwhl.pwhl_player_impact import pwhl_skater_war
war = pwhl_skater_war(pbp, shifts)
```

### pwhl_special_teams_value {#pwhl_special_teams_value}

`pwhl_special_teams_value(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, **kwargs: 'Any') -> "'pl.DataFrame | pd.DataFrame'"`

⑥ PWHL special-teams value -- shim over `nhl_special_teams_value` with `league='pwhl'`.

Guards on PWHL shift-chart coverage (see the module docstring).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a PWHL play-by-play frame shaped like `load_nhl_pbp_full`. |
| `shifts` | `DataFrame` |  | a PWHL shift-chart frame shaped like `load_nhl_shifts`. |
| `model_dir` | `str \| None` | `None` | booster directory passed through to the borrowed NHL xG boosters. |

**Returns**

Same schema as `nhl_special_teams_value`: `player_id:Int64, pp_toi_minutes:Float64, pk_toi_minutes:Float64, pp_value:Float64, pk_value:Float64`. A zero-row frame with this schema (+ a `cli_warn`) when PWHL shift coverage is insufficient.

No returns table is published for this function: no capture: it expects NHL-style play-by-play columns (secondary_type, x_fixed, strength_state) that no PWHL loader provides, so it raises ColumnNotFoundError on real PWHL games.

**Example**

```python
from sportsdataverse.pwhl.pwhl_player_impact import pwhl_special_teams_value
st = pwhl_special_teams_value(pbp, shifts)
```

### pwhl_standings {#pwhl_standings}

`pwhl_standings(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL standings — one row per team.

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
| `losses` | character | Losses. |
| `regulation_wins` | character | Wins in regulation. |
| `points` | integer | Total points (goals + assists). |
| `streak_wl` | character | Present but unpopulated in the sampled PWHL standings (empty string in every row); the name suggests a win/loss streak, but no value was observed to confirm a format. |
| `goals_for` | character | Goals for. |
| `goals_against` | character | Goals against. |
| `non_reg_wins` | character | Non-regulation wins. |
| `non_reg_losses` | character | Non-regulation losses. |
| `games_remaining` | character | Games remaining in the season. |
| `percentage` | character | Points percentage earned by the PWHL team (points divided by maximum possible points), expressed as a decimal between 0 and 1. |
| `overall_rank` | character |  |
| `games_played` | character | Games played. |
| `team_rank` | integer | Team rank in the standings. |
| `team` | character | Team name. |
| `wins` | integer | Wins. |

### pwhl_stats {#pwhl_stats}

`pwhl_stats(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, position: 'str' = 'skaters', return_as_pandas: 'bool' = False) -> 'Any'`

PWHL aggregate stats by season and position.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `position` | `str` | `'skaters'` | `"skaters"` (default) or `"goalies"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player for that season and position, as the feed ships it (String, ~90 columns): `player_id`, `name`, `team_id` / `team_name` / `team_code`, `position`, `games_played`, and the counting, per-game and ice-time stats of that position (skaters: `goals`, `assists`, `points`, `plus_minus`, `shots`, `hits`, `penalty_minutes` ...). A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `shortname` | character | Player short name. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `name` | character | Player full name (e.g. 'Jessie Eldridge'). |
| `phonetic_name` | character | Phonetic spelling of the player name. |
| `active` | character | Whether athlete is currently active. |
| `height` | character | Player height as feet-and-inches text (e.g. 5'11), not a number of inches. |
| `weight` | character | Player weight in pounds. |
| `last_years_club` | character | Player's club in the previous season. |
| `age` | character | Player age. |
| `shoots` | character | Shooting hand. |
| `position` | character | Player position. |
| `suspension_games_remaining` | character | Suspension games remaining. |
| `suspension_indefinite` | character | Whether the suspension is indefinite. |
| `rookie` | character | Whether the player is a rookie. |
| `veteran` | character | HockeyTech veteran-status code as text ('1' or '2' in the captured rows; the feed does not document the codes), not a boolean. |
| `draft_eligible` | character | Whether the player is draft eligible. |
| `jersey_number` | character | Jersey number. |
| `team_name` | character | Team name. |
| `team_code` | character | Team abbreviation. |
| `team_id` | character | Unique team identifier. |
| `division` | character | Division name as text (e.g. 'PWHL'), not an identifier. |
| `birthdate` | character | Date of birth. |
| `birthdate_year` | character | Player birth year. |
| `hometown` | character | Player hometown as city and province, state or country abbreviation (e.g. 'Barrie, ON'). |
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
| `turnovers` | character | Turnovers committed. |
| `hits` | character | Hits. |
| `shots_blocked_by_player` | character | Shots blocked by the player. |
| `ice_time_minutes_seconds` | character | Ice time in minutes and seconds. |
| `shooting_percentage` | character | Shooting percentage. |
| `assists` | character | Assists. |
| `points` | character | Total points (goals + assists). |
| `points_per_game` | character | Points per game. |
| `plus_minus` | character | Plus/minus rating. |
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
| `logo` | character | URL to the team logo. |
| `rank` | integer | 1-based position of the player in the feed's sorted stat table. |
| `player_page_link` | character | URL to the player page. |
| `player_image` | character | URL to the player's headshot image as served by the HockeyTech/PWHL data feed. |
| `namelink` | character | Player name as plain text (the text of the feed's player link; equals name in the captures), not HTML. |
| `teamlink` | character | HTML link for the team. |
| `team_breakdown` | integer | Per-team statistical breakdown. |
| `is_total` | double | Whether the row is a season total. |

### pwhl_streaks {#pwhl_streaks}

`pwhl_streaks(return_as_pandas: 'bool' = False) -> 'Any'`

Current PWHL player/team streaks — **non-functional: no such upstream view**.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If `True` return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

An empty frame (the upstream view does not exist).

### pwhl_team_roster {#pwhl_team_roster}

`pwhl_team_roster(team_id: 'int', season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL team roster for a given team + season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  | The HockeyTech team id. |
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per rostered player: `player_id`, `person_id`, names (`first_name`, `last_name`, `display_name`), `position`, `tp_jersey_number`, `shoots` / `catches`, `height` / `weight`, `birthdate`, home and birth places, `rookie`, `veteran_status`, `draft_status` and `player_image` (String, as the feed ships them). A pandas DataFrame when `return_as_pandas` is True.

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
| `birthplace` | character | City, province/state, or country where the player was born, as recorded in the PWHL HockeyTech roster. |
| `height` | character | Player height in inches. |
| `weight` | character | Player weight in pounds. |
| `height_hyphenated` | character | Player's height formatted as feet-inches with a hyphen separator (e.g., '5-9'), as supplied by the PWHL HockeyTech API. |
| `hidden` | character | Flag indicating whether the player's roster entry is suppressed from public-facing displays in the PWHL HockeyTech system. |
| `current_team` | character | Name or identifier of the PWHL team to which the player is currently rostered. |
| `player_id` | character | Unique player identifier. |
| `status` | character | Status string (e.g. captain markers). |
| `birthdate` | character | Date of birth. |
| `birthdate_year` | character | Player birth year. |
| `rawbirthdate` | character | Player's birth date as a raw string in the format returned by the PWHL HockeyTech API, typically YYYY-MM-DD. |
| `latest_team_id` | character | Most recent team identifier. |
| `veteran_status` | character | Player veteran status. |
| `veteran_description` | character | Text label or descriptor indicating the player's veteran status or experience classification in the PWHL. |
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Team name. |
| `division` | character | Division identifier. |
| `tp_jersey_number` | character | Jersey number assigned to the player on the current PWHL team roster, as provided by the HockeyTech feed. |
| `rookie` | character | Whether the player is a rookie. |
| `position_id` | character | Official position identifier. |
| `position` | character | Player position. |
| `nhlteam` | character | Name or identifier of the NHL organization that holds the player's NHL rights, if applicable. |
| `player_id_1` | character | Alternate or secondary HockeyTech player identifier, distinct from the primary person_id and player_id fields. |
| `is_rookie` | character | Whether the player is a rookie. |
| `h` | character |  |
| `w` | character |  |
| `draft_status` | character | Text description of the player's draft history or eligibility status (e.g., undrafted, drafted year and round). |
| `name` | character | Player full name, first then last (e.g. 'Megan Keller'). |
| `player_image` | character | URL of the player's official roster photograph from the PWHL HockeyTech feed. |
| `catches` | character | Catching hand (goalies). |

### pwhl_teams {#pwhl_teams}

`pwhl_teams(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL teams for a given season.

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

### pwhl_transactions {#pwhl_transactions}

`pwhl_transactions(return_as_pandas: 'bool' = False) -> 'Any'`

PWHL roster transactions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per transaction on the feed's current page (the newest 20): `transaction_date` / `transaction_time`, `transaction_type` / `ttype_text`, `title`, `detail`, `player_id` / `player_name` / `position`, and `team_id` / `team_name` / `team_code` / `team_city` (String). A pandas DataFrame when `return_as_pandas` is True.

### pwhl_unit_ratings {#pwhl_unit_ratings}

`pwhl_unit_ratings(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, **kwargs: 'Any') -> "'pl.DataFrame | pd.DataFrame'"`

⑤ PWHL line/pair ratings -- shim over `nhl_unit_ratings` with `league='pwhl'`.

Guards on PWHL shift-chart coverage (see the module docstring).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a PWHL play-by-play frame shaped like `load_nhl_pbp_full`. |
| `shifts` | `DataFrame` |  | a PWHL shift-chart frame shaped like `load_nhl_shifts`. |
| `model_dir` | `str \| None` | `None` | booster directory passed through to the borrowed NHL xG boosters. |

**Returns**

Same schema as `nhl_unit_ratings`: `team:Utf8, unit_ids:Utf8, unit_players:Utf8, toi_minutes:Float64, on_ice_xgf:Float64, on_ice_xga:Float64, on_ice_xgf_pct:Float64, summed_rapm:Float64, unit_value:Float64`. A zero-row frame with this schema (+ a `cli_warn`) when PWHL shift coverage is insufficient.

No returns table is published for this function: no capture: it expects NHL-style play-by-play columns (secondary_type, x_fixed, strength_state) that no PWHL loader provides, so it raises ColumnNotFoundError on real PWHL games.

**Example**

```python
from sportsdataverse.pwhl.pwhl_player_impact import pwhl_unit_ratings
units = pwhl_unit_ratings(pbp, shifts)
```
