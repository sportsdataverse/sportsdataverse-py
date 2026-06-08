---
title: NBA — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# NBA — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nba`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_nba_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull an NBA athlete's ESPN **season** stat line as one wide row.

See :func:`sportsdataverse.wbb.espn_wbb_player_stats` for full documentation of the wide return shape, the ``{category}_{stat}`` stat columns, the athlete / team metadata blocks, and the ``season_type`` / ``total`` parameters. For the richer multi-category web-v3 payload use :func:`sportsdataverse.nba.espn_nba_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN NBA athlete identifier (e.g. ``1966`` for LeBron James). |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | ``"regular"`` (type 2) or ``"postseason"`` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When ``raw=True`` returns the raw statistics JSON ``dict``.

| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `season_type` | character |  |
| `total` | logical |  |
| `athlete_id` | integer |  |
| `athlete_uid` | character |  |
| `athlete_guid` | character |  |
| `athlete_type` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `full_name` | character |  |
| `display_name` | character |  |
| `short_name` | character |  |
| `weight` | double |  |
| `display_weight` | character |  |
| `height` | double |  |
| `display_height` | character |  |
| `age` | integer |  |
| `date_of_birth` | character |  |
| `jersey` | character |  |
| `slug` | character |  |
| `active` | logical |  |
| `position_id` | integer |  |
| `position_name` | character |  |
| `position_display_name` | character |  |
| `position_abbreviation` | character |  |
| `college_name` | character |  |
| `status_id` | integer |  |
| `status_name` | character |  |
| `defensive_blocks` | double |  |
| `defensive_defensive_rebounds` | double |  |
| `defensive_steals` | double |  |
| `defensive_def_rebound_rate` | double |  |
| `defensive_avg_defensive_rebounds` | double |  |
| `defensive_avg_blocks` | double |  |
| `defensive_avg_steals` | double |  |
| `defensive_avg48_defensive_rebounds` | double |  |
| `defensive_avg48_blocks` | double |  |
| `defensive_avg48_steals` | double |  |
| `defensive_drpm` | double |  |
| `general_disqualifications` | double |  |
| `general_flagrant_fouls` | double |  |
| `general_fouls` | double |  |
| `general_per` | double |  |
| `general_rebound_rate` | double |  |
| `general_ejections` | double |  |
| `general_technical_fouls` | double |  |
| `general_rebounds` | double |  |
| `general_vorp` | double |  |
| `general_warp` | double |  |
| `general_rpm` | double |  |
| `general_minutes` | double |  |
| `general_avg_minutes` | double |  |
| `general_nba_rating` | double |  |
| `general_plus_minus` | double |  |
| `general_avg_rebounds` | double |  |
| `general_avg_fouls` | double |  |
| `general_avg_flagrant_fouls` | double |  |
| `general_avg_technical_fouls` | double |  |
| `general_avg_ejections` | double |  |
| `general_avg_disqualifications` | double |  |
| `general_assist_turnover_ratio` | double |  |
| `general_steal_foul_ratio` | double |  |
| `general_block_foul_ratio` | double |  |
| `general_avg_team_rebounds` | double |  |
| `general_total_rebounds` | double |  |
| `general_total_technical_fouls` | double |  |
| `general_team_assist_turnover_ratio` | double |  |
| `general_steal_turnover_ratio` | double |  |
| `general_avg48_rebounds` | double |  |
| `general_avg48_fouls` | double |  |
| `general_avg48_flagrant_fouls` | double |  |
| `general_avg48_technical_fouls` | double |  |
| `general_avg48_ejections` | double |  |
| `general_avg48_disqualifications` | double |  |
| `general_r40` | double |  |
| `general_games_played` | double |  |
| `general_games_started` | double |  |
| `general_double_double` | double |  |
| `general_triple_double` | double |  |
| `offensive_assists` | double |  |
| `offensive_effective_fg_pct` | double |  |
| `offensive_field_goals` | double |  |
| `offensive_field_goals_attempted` | double |  |
| `offensive_field_goals_made` | double |  |
| `offensive_field_goal_pct` | double |  |
| `offensive_free_throws` | double |  |
| `offensive_free_throw_pct` | double |  |
| `offensive_free_throws_attempted` | double |  |
| `offensive_free_throws_made` | double |  |
| `offensive_offensive_rebounds` | double |  |
| `offensive_points` | double |  |
| `offensive_turnovers` | double |  |
| `offensive_three_point_pct` | double |  |
| `offensive_three_point_field_goals_attempted` | double |  |
| `offensive_three_point_field_goals_made` | double |  |
| `offensive_true_shooting_pct` | double |  |
| `offensive_total_turnovers` | double |  |
| `offensive_assist_ratio` | double |  |
| `offensive_points_in_paint` | double |  |
| `offensive_off_rebound_rate` | double |  |
| `offensive_turnover_ratio` | double |  |
| `offensive_brick_index` | double |  |
| `offensive_usage_rate` | double |  |
| `offensive_avg_field_goals_made` | double |  |
| `offensive_avg_field_goals_attempted` | double |  |
| `offensive_avg_three_point_field_goals_made` | double |  |
| `offensive_avg_three_point_field_goals_attempted` | double |  |
| `offensive_avg_free_throws_made` | double |  |
| `offensive_avg_free_throws_attempted` | double |  |
| `offensive_avg_points` | double |  |
| `offensive_avg_offensive_rebounds` | double |  |
| `offensive_avg_assists` | double |  |
| `offensive_avg_turnovers` | double |  |
| `offensive_offensive_rebound_pct` | double |  |
| `offensive_estimated_possessions` | double |  |
| `offensive_avg_estimated_possessions` | double |  |
| `offensive_points_per_estimated_possessions` | double |  |
| `offensive_avg_team_turnovers` | double |  |
| `offensive_avg_total_turnovers` | double |  |
| `offensive_three_point_field_goal_pct` | double |  |
| `offensive_two_point_field_goals_made` | double |  |
| `offensive_two_point_field_goals_attempted` | double |  |
| `offensive_avg_two_point_field_goals_made` | double |  |
| `offensive_avg_two_point_field_goals_attempted` | double |  |
| `offensive_two_point_field_goal_pct` | double |  |
| `offensive_shooting_efficiency` | double |  |
| `offensive_scoring_efficiency` | double |  |
| `offensive_avg48_field_goals_made` | double |  |
| `offensive_avg48_field_goals_attempted` | double |  |
| `offensive_avg48_three_point_field_goals_made` | double |  |
| `offensive_avg48_three_point_field_goals_attempted` | double |  |
| `offensive_avg48_free_throws_made` | double |  |
| `offensive_avg48_free_throws_attempted` | double |  |
| `offensive_avg48_points` | double |  |
| `offensive_avg48_offensive_rebounds` | double |  |
| `offensive_avg48_assists` | double |  |
| `offensive_avg48_turnovers` | double |  |
| `offensive_p40` | double |  |
| `offensive_a40` | double |  |
| `offensive_orpm` | double |  |
| `team_id` | integer |  |
| `team_uid` | character |  |
| `team_guid` | character |  |
| `team_slug` | character |  |
| `team_location` | character |  |
| `team_name` | character |  |
| `team_abbreviation` | character |  |
| `team_display_name` | character |  |
| `team_short_display_name` | character |  |
| `team_color` | character |  |
| `team_alternate_color` | character |  |
| `team_is_active` | logical |  |
| `team_logo_href` | character |  |

**Example**

```python
from sportsdataverse.nba import espn_nba_player_stats
df = espn_nba_player_stats(athlete_id=1966, season=2023)
df.select(["full_name", "team_display_name", "offensive_points"])
```

### `espn_nba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nba_schedule - look up the NBA schedule for a given date from ESPN

Args: dates (int): Used to define different seasons. 2002 is the earliest available season. season_type (int): season type, 1 for pre-season, 2 for regular season, 3 for post-season, 4 for all-star, 5 for off-season limit (int): number of records to return, default: 500. return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe. Returns: pl.DataFrame: Polars dataframe containing schedule dates for the requested season. Returns None if no games Example: Quick start (today's slate):: from sportsdataverse.nba import espn_nba_schedule slate = espn_nba_schedule() print(slate.shape) Pull a specific date:: jan2 = espn_nba_schedule(dates=20230102, season_type=2) Pipeline next step (extract finals only):: import polars as pl finals = espn_nba_schedule(dates=20230102).filter( pl.col("status_type_completed") == True ) See Also: * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for NBA data * `nba_api <https://github.com/swar/nba_api>`_ -- Python alternative to the NBA Stats API

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` |  | `None` |  |
| `season_type` |  | `None` |  |
| `limit` |  | `500` |  |
| `return_as_pandas` |  | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `id` | character |  |
| `uid` | character |  |
| `date` | character |  |
| `attendance` | integer |  |
| `time_valid` | logical |  |
| `neutral_site` | logical |  |
| `conference_competition` | logical |  |
| `play_by_play_available` | logical |  |
| `recent` | logical |  |
| `start_date` | character |  |
| `broadcast` | character |  |
| `highlights` | character |  |
| `notes_type` | character |  |
| `notes_headline` | character |  |
| `broadcast_market` | character |  |
| `broadcast_name` | character |  |
| `type_id` | character |  |
| `type_abbreviation` | character |  |
| `venue_id` | character |  |
| `venue_full_name` | character |  |
| `venue_address_city` | character |  |
| `venue_address_state` | character |  |
| `venue_indoor` | logical |  |
| `status_clock` | double |  |
| `status_display_clock` | character |  |
| `status_period` | integer |  |
| `status_type_id` | character |  |
| `status_type_name` | character |  |
| `status_type_state` | character |  |
| `status_type_completed` | logical |  |
| `status_type_description` | character |  |
| `status_type_detail` | character |  |
| `status_type_short_detail` | character |  |
| `format_regulation_periods` | integer |  |
| `home_id` | character |  |
| `home_uid` | character |  |
| `home_location` | character |  |
| `home_name` | character |  |
| `home_abbreviation` | character |  |
| `home_display_name` | character |  |
| `home_short_display_name` | character |  |
| `home_color` | character |  |
| `home_alternate_color` | character |  |
| `home_is_active` | logical |  |
| `home_venue_id` | character |  |
| `home_logo` | character |  |
| `home_score` | character |  |
| `home_linescores` | integer |  |
| `home_records` | character |  |
| `away_id` | character |  |
| `away_uid` | character |  |
| `away_location` | character |  |
| `away_name` | character |  |
| `away_abbreviation` | character |  |
| `away_display_name` | character |  |
| `away_short_display_name` | character |  |
| `away_color` | character |  |
| `away_alternate_color` | character |  |
| `away_is_active` | logical |  |
| `away_venue_id` | character |  |
| `away_logo` | character |  |
| `away_score` | character |  |
| `away_linescores` | integer |  |
| `away_records` | character |  |
| `game_id` | integer |  |
| `season` | integer |  |
| `season_type` | integer |  |

## Utilities & helpers

### `most_recent_nba_season()`

Return the most recent NBA season year based on today's date.

The NBA season crosses calendar years -- a season started in October of year Y is reported as season Y+1. If today is in October or later, this returns next calendar year; otherwise it returns the current calendar year.

**Returns**

The most recent NBA season year (e.g. 2024 for the 2023-24 season).

**Example**

```python
from sportsdataverse.nba import most_recent_nba_season
year = most_recent_nba_season()
print(year)

Combine with the loaders for a "current season" pull::

from sportsdataverse.nba import load_nba_schedule, most_recent_nba_season
sched = load_nba_schedule(seasons=[most_recent_nba_season()])
```

### `year_to_season(year)`

Convert a season-end year (e.g. 2024) to the NBA's hyphenated label

(e.g. ``"2023-24"``). Handles century rollover (1999 -> ``"1999-00"``) and zero-pads the second half of the label.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `int` |  | The starting calendar year of the season (e.g. 2023 for the 2023-24 season). |

**Returns**

NBA-style season label.

**Example**

```python
from sportsdataverse.nba import year_to_season
label = year_to_season(2023)
print(label)  # "2023-24"

Century rollover::

print(year_to_season(1999))  # "1999-00"
```

## Other

### `espn_nba_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nba_teams - look up NBA teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.nba.espn_nba_teams.clear_cache().

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character |  |
| `team_alternate_color` | character |  |
| `team_color` | character |  |
| `team_display_name` | character |  |
| `team_id` | character |  |
| `team_is_active` | logical |  |
| `team_is_all_star` | logical |  |
| `team_location` | character |  |
| `team_logos` | integer |  |
| `team_name` | character |  |
| `team_nickname` | character |  |
| `team_short_display_name` | character |  |
| `team_slug` | character |  |
| `team_uid` | character |  |

**Example**

```python
from sportsdataverse.nba import espn_nba_teams
teams = espn_nba_teams()
print(teams.shape)

Pandas round-trip::

teams_pd = espn_nba_teams(return_as_pandas=True)
teams_pd.head()

Pipeline next step (build a team_id to abbreviation map)::

teams = espn_nba_teams()
abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))
```

### `nba_pbp_disk(game_id, path_to_json)`

Load a previously cached ESPN NBA summary JSON for a game from disk.

Reads ``{path_to_json}/{game_id}.json``.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game / event identifier. |
| `path_to_json` | `str` |  | Directory containing the cached JSON file. |

**Returns**

Parsed JSON contents.

**Example**

```python
from sportsdataverse.nba import nba_pbp_disk
pbp = nba_pbp_disk(game_id=401585183, path_to_json="./cache")
print(list(pbp.keys()))
```

### `scoreboard_event_parsing(event)`

Internal helper that flattens an ESPN NBA scoreboard event dict into a

shape suitable for ``pd.json_normalize``.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `dict` |  | A single scoreboard ``events[*]`` entry from the ESPN NBA scoreboard API. |

**Returns**

The same event dict, mutated in place with ``home``/``away`` copies of the competitors and trimmed of unused link/odds keys.

**Example**

```python
from sportsdataverse.nba import espn_nba_schedule
sched = espn_nba_schedule(dates=20230102)
```
