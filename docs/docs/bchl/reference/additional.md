---
title: BCHL — additional Python functions
sidebar_label: Additional functions
description: "BCHL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# BCHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.hockey.bchl` that are not
generated API-endpoint wrappers (those have their own reference pages).

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

One row per player on ice for a shot attempt: `player_id` (Int64), `corsi_for` / `corsi_against` / `corsi_for_pct`, `fenwick_for` / `fenwick_against` / `fenwick_for_pct`, `corsi_includes_missed` (Boolean), `toi_seconds` (Int64) and `corsi_for_per60` (Float64, null without time on ice). A pandas DataFrame when `return_as_pandas` is True.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

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

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

### bchl_game_summary {#bchl_game_summary}

`bchl_game_summary(game_id: 'int') -> 'dict'`

BCHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).

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

One row per ranked skater (points and goals leaders): `rank` (Int64), `player_id`, `name`, `jersey_number`, `position`, `team_id` / `team_name` / `team_code`, `stat_formatted` and `type_formatted` (`player_id` / `team_id` Int64, the rest String), plus photo and logo URLs. A pandas DataFrame when `return_as_pandas` is True.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

### bchl_pbp {#bchl_pbp}

`bchl_pbp(game_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

BCHL play-by-play — one row per event, fully enriched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | The HockeyTech game id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per event (shot, goal, penalty, faceoff, hit, goalie change ...): `game_id`, `event`, `team_id`, `period_of_game`, `time_of_period`, rink `x_coord` / `y_coord` (Float64, feet, centre ice at (0, 0); the raw 600×300 feed canvas values are `x_coord_original` / `y_coord_original`), the feet frames `x_coord_fixed` / `y_coord_fixed` (home team shoots right) and `x_coord_right` / `y_coord_right` (every team shoots right), the primary / second / third player and goalie ids (Int64) and names, the plus / minus skaters on a goal, game metadata from the game summary, and derived `shot_distance` / `shot_angle` / `scoring_chance` and the `on_ice_home` / `on_ice_away` skaters from the shift feed. Some leagues (USHL, MJHL) publish only goals, penalties and goalie changes, with no coordinates. A pandas DataFrame when `return_as_pandas` is True.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

### bchl_player_stats {#bchl_player_stats}

`bchl_player_stats(player_id: 'int', return_as_pandas: 'bool' = False) -> 'Any'`

BCHL player season stats across all seasons.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  | The HockeyTech player id. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per season (and team) the player played: `season_id`, `season_name`, `playoff`, `team_id` / `team_name` / `team_code`, `games_played`, `goals`, `assists`, `points`, `plus_minus`, `penalty_minutes`, power-play / short-handed / shootout splits, `shots`, `faceoff_wins` / `faceoff_attempts`, `ice_time` and `stat_type`. `season_id` and `team_id` are Int64; the rest are String, as the feed ships them. A pandas DataFrame when `return_as_pandas` is True.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

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

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

### bchl_schedule {#bchl_schedule}

`bchl_schedule(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

BCHL schedule — one row per game of one season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per game: `game_id`, `game_date`, `game_status`, `home_team` / `home_team_id` / `home_score`, `away_team` / `away_team_id` / `away_score`, `venue`, `season_id` and `game_type`; the `*_id` columns are Int64, the rest String. Only the requested season's games: a regular season, its playoffs and its preseason are separate season ids, so pass `season_id` (from `bchl_season_id()`) for the playoffs. A pandas DataFrame when `return_as_pandas` is True.

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
| `team_code` | String | Team abbreviation. |
| `wins` | String | Wins. |
| `losses` | String | Losses. |
| `ties` | String | Total ties. |
| `ot_losses` | String | Overtime losses. |
| `ot_wins` | String | Overtime wins. |
| `shootout_losses` | String | Shootout losses. |
| `points` | Int64 | Total points (goals + assists). |
| `penalty_minutes` | String | Penalty minutes. |
| `goals_for` | String | Goals for. |
| `goals_against` | String | Goals against. |
| `goals_diff` | String | Goal differential, goals_for minus goals_against (feed header 'Goal Differential'), as a signed integer string such as '14' or '-10'. |
| `percentage` | String | Points percentage (feed header 'Percentage'): points earned divided by the maximum points available from games played, as a three-decimal string, e.g. '0.833' for 5 of 6 possible points; '0.000' before a team has played. |
| `games_played` | String | Games played. |
| `team_rank` | Int64 | Team rank in the standings. |
| `team` | String | Team name. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

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

One row per rostered player: `player_id`, `person_id`, names (`first_name`, `last_name`, `display_name`), `position`, `tp_jersey_number`, `shoots` / `catches`, `height` / `weight`, `birthdate`, home and birth places, `rookie`, `veteran_status`, `draft_status` and `player_image`. The `id` / `*_id` columns are Int64; the rest are String, as the feed ships them. A pandas DataFrame when `return_as_pandas` is True.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

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

### most_recent_bchl_season {#most_recent_bchl_season}

`most_recent_bchl_season() -> 'int'`

Newest BCHL regular season as an end-year integer: the highest `season_yr` of a regular season that is not a one-off event, so a preseason listed first is not a default.

**Returns**

The newest regular season's END year (2026 = the 2025-26 season).

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The seasons feed lists no regular season. |
| `AssetFetchError` | The seasons feed failed. |
