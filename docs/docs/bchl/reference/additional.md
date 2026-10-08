---
title: BCHL — additional Python functions
sidebar_label: Additional functions
description: "BCHL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# BCHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.bchl`
not covered by the generated API-endpoint reference above.

## HockeyTech / LeagueStat

### bchl_game_corsi {#bchl_game_corsi}

`bchl_game_corsi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

Player-level on-ice Corsi and Fenwick for a single BCHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player on ice for a shot attempt: `player_id` (String), `corsi_for` / `corsi_against` / `corsi_for_pct`, `fenwick_for` / `fenwick_against` / `fenwick_for_pct`, `corsi_includes_missed` (Boolean), `toi_seconds` (Int64) and `corsi_for_per60` (Float64, null without time on ice). A pandas DataFrame when `return_as_pandas` is True.

### bchl_game_shifts {#bchl_game_shifts}

`bchl_game_shifts(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

Parsed shift stints for a single BCHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per shift: `game_id` and `player_id` (Int64), `first_name`, `last_name`, `jersey_number`, `home` (Int64, 1 = home), `period`, `start_time` / `end_time` / `length` (clock strings), `start_s` / `end_s` (Int64 seconds) and the `goal_on_shift` / `penalty_on_shift` flags. A pandas DataFrame when `return_as_pandas` is True.

### bchl_game_summary {#bchl_game_summary}

`bchl_game_summary(game_id: 'int') -> 'dict'`

BCHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |

**Returns**

`game` (one row: `game_id`, `date`, `status`, `venue`, `attendance`, both teams and scores), `goals` (one row per goal with the scorer, both assists and the plus / minus skaters), `penalties` (one row per penalty), `shots_by_period` (`side`, `period`, `shots`) and `three_stars`. When the league denies the summary view, the event frames are empty and `game` is a `game_id` stub row.

### bchl_leaders {#bchl_leaders}

`bchl_leaders(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

BCHL statistical leaders for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per ranked skater (points and goals leaders): `rank` (Int64), `player_id`, `name`, `jersey_number`, `position`, `team_id` / `team_name` / `team_code`, `stat_formatted` and `type_formatted` (String), plus photo and logo URLs. A pandas DataFrame when `return_as_pandas` is True.

### bchl_pbp {#bchl_pbp}

`bchl_pbp(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

BCHL play-by-play — one row per event, fully enriched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per event (shot, goal, penalty, faceoff, hit, goalie change ...): `game_id`, `event`, `team_id`, `period_of_game`, `time_of_period`, rink `x_coord` / `y_coord` (Float64, raw 600×300 coordinates), the primary / second / third player and goalie ids and names, the plus / minus skaters on a goal, game metadata from the game summary, and derived `shot_distance` / `shot_angle` / `scoring_chance` and the `on_ice_home` / `on_ice_away` skaters from the shift feed. Player ids are Float64 here. Some leagues (USHL, MJHL) publish only goals, penalties and goalie changes, with no coordinates. A pandas DataFrame when `return_as_pandas` is True.

### bchl_player_stats {#bchl_player_stats}

`bchl_player_stats(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

BCHL player season stats across all seasons.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  | The HockeyTech player id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per season (and team) the player played: `season_id`, `season_name`, `playoff`, `team_id` / `team_name` / `team_code`, `games_played`, `goals`, `assists`, `points`, `plus_minus`, `penalty_minutes`, power-play / short-handed / shootout splits, `shots`, `faceoff_wins` / `faceoff_attempts`, `ice_time` and `stat_type` (String, as the feed ships them). A pandas DataFrame when `return_as_pandas` is True.

### bchl_player_toi {#bchl_player_toi}

`bchl_player_toi(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

Per-player time-on-ice totals for a single BCHL game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player: `player_id` (Int64), `first_name`, `last_name`, `toi_seconds` (Int64), `num_shifts` and `avg_shift_s` (Float64). A pandas DataFrame when `return_as_pandas` is True.

### bchl_schedule {#bchl_schedule}

`bchl_schedule(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

BCHL schedule — one row per game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). With neither `season` nor `season_id`, no season filter is sent and the feed's whole recent window comes back. |
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

### bchl_standings {#bchl_standings}

`bchl_standings(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

BCHL standings — one row per team.

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
| `ties` | character | Total ties. |
| `ot_losses` | character | Overtime losses. |
| `ot_wins` | character | Overtime wins. |
| `shootout_losses` | character | Shootout losses. |
| `points` | integer | Total points (goals + assists). |
| `penalty_minutes` | character | Penalty minutes. |
| `goals_for` | character | Goals for. |
| `goals_against` | character | Goals against. |
| `goals_diff` | character | Goal differential, goals_for minus goals_against (feed header 'Goal Differential'), as a signed integer string such as '14' or '-10'. |
| `percentage` | character | Points percentage (feed header 'Percentage'): points earned divided by the maximum points available from games played, as a three-decimal string, e.g. '0.833' for 5 of 6 possible points; '0.000' before a team has played. |
| `games_played` | character | Games played. |
| `team_rank` | integer | Team rank in the standings. |
| `team` | character | Team name. |

### bchl_team_roster {#bchl_team_roster}

`bchl_team_roster(team_id: 'int', season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

BCHL team roster for a given team + season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  | The HockeyTech team id. |
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per rostered player: `player_id`, `person_id`, names (`first_name`, `last_name`, `display_name`), `position`, `tp_jersey_number`, `shoots` / `catches`, `height` / `weight`, `birthdate`, home and birth places, `rookie`, `veteran_status`, `draft_status` and `player_image` (String, as the feed ships them). A pandas DataFrame when `return_as_pandas` is True.

### bchl_teams {#bchl_teams}

`bchl_teams(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

BCHL teams for a given season.

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

### build_family {#build_family}

`build_family(league: 'str') -> 'dict[str, Any]'`

Return a dict of public callables for *league*.

All callables are fully independent closures over the single `league`
string; none share mutable state.  The dict is ready to be spread into
a module namespace via `globals().update(...)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | HockeyTech league code: `"ahl"`, `"ohl"`, `"whl"`, or `"qmjhl"`. |

**Returns**

Keys are the public function names (e.g. `"ahl_schedule"`).

## Dates and seasons

### bchl_season_id {#bchl_season_id}

`bchl_season_id(return_as_pandas: 'bool' = False) -> 'Any'`

All BCHL seasons with end-year + game-type labels.

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

### most_recent_bchl_season {#most_recent_bchl_season}

`most_recent_bchl_season() -> 'int'`

Newest BCHL regular season as an end-year integer: the highest `season_yr` of a regular season that is not a one-off event, so a preseason listed first is not a default.

**Returns**

The newest regular season's END year (2026 = the 2025-26 season).
