---
title: WNBA — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# WNBA — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.wnba`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_wnba_game_officials(game_id: 'int', season: 'int | None' = None, *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull the officials assigned to a WNBA game.

See :func:`sportsdataverse.wbb.espn_wbb_game_officials` for full documentation of the column set, the empty-frame fallback when ESPN ships no officials, and the ``raw`` / ``return_as_pandas`` flag semantics.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN WNBA event identifier (e.g. ``401620238`` for Game 1 of the 2024 WNBA Finals). |
| `season` | `int \| None` | `None` | Season year (recorded as output column only). |
| `raw` | `bool` | `False` | If True, returns the parsed JSON dict before any flattening. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |

**Returns**

Polars (or pandas) DataFrame with the same columns documented in :func:`sportsdataverse.wbb.espn_wbb_game_officials`. If ``raw=True``, returns the raw response dict.

| col_name | type | description |
|---|---|---|
| `game_id` | integer |  |
| `season` | integer |  |
| `official_id` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `full_name` | character |  |
| `display_name` | character |  |
| `position_id` | character |  |
| `position_name` | character |  |
| `position_display_name` | character |  |
| `order` | integer |  |

**Example**

```python
from sportsdataverse.wnba import espn_wnba_game_officials
refs = espn_wnba_game_officials(game_id=401620238, season=2024)
print(refs.shape)
refs.select(["full_name", "position_name", "order"]).head()

Pandas round-trip::

refs_pd = espn_wnba_game_officials(
    game_id=401620238, season=2024, return_as_pandas=True
)
refs_pd[["full_name", "position_name"]].head()

Inspect the raw ESPN payload (e.g. for fields not flattened)::

payload = espn_wnba_game_officials(game_id=401620238, season=2024, raw=True)
list(payload.keys())[:8]
```

### `espn_wnba_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull a WNBA athlete's ESPN **season** stat line.

See :func:`sportsdataverse.wbb.espn_wbb_player_stats` for full documentation of the wide return shape, the ``{category}_{stat}`` stat columns, the athlete / team metadata blocks, and the ``season_type`` / ``total`` parameters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN WNBA athlete identifier (e.g. ``3149391`` for A'ja Wilson). |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | ``"regular"`` (type 2) or ``"postseason"`` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When ``raw=True`` returns the raw statistics JSON ``dict``. See :func:`sportsdataverse.wbb.espn_wbb_player_stats` for the column layout.

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
| `defensive_avg_defensive_rebounds` | double |  |
| `defensive_avg_blocks` | double |  |
| `defensive_avg_steals` | double |  |
| `defensive_avg48_defensive_rebounds` | double |  |
| `defensive_avg48_blocks` | double |  |
| `defensive_avg48_steals` | double |  |
| `general_disqualifications` | double |  |
| `general_flagrant_fouls` | double |  |
| `general_fouls` | double |  |
| `general_ejections` | double |  |
| `general_technical_fouls` | double |  |
| `general_rebounds` | double |  |
| `general_vorp` | double |  |
| `general_minutes` | double |  |
| `general_avg_minutes` | double |  |
| `general_fantasy_rating` | double |  |
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
| `general_games_played` | double |  |
| `general_games_started` | double |  |
| `general_double_double` | double |  |
| `general_triple_double` | double |  |
| `offensive_assists` | double |  |
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
| `offensive_total_turnovers` | double |  |
| `offensive_points_in_paint` | double |  |
| `offensive_brick_index` | double |  |
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
from sportsdataverse.wnba import espn_wnba_player_stats
df = espn_wnba_player_stats(athlete_id=3149391, season=2024)
df.select(["full_name", "team_display_name", "offensive_points"])
```

### `espn_wnba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wnba_schedule - look up the WNBA schedule for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `season_type` | `int` | `None` | 2 for regular season, 3 for post-season, 4 for off-season. |
| `limit` | `int` | `500` | number of records to return, default: 500. |
| `return_as_pandas` |  | `False` |  |

**Returns**

Polars dataframe containing schedule dates for the requested season. Returns None if no games

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
| `highlights` | integer |  |
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
| `home_winner` | logical |  |
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
| `away_winner` | logical |  |
| `away_linescores` | integer |  |
| `away_records` | character |  |
| `game_id` | integer |  |
| `season` | integer |  |
| `season_type` | integer |  |

**Example**

```python
from sportsdataverse.wnba import espn_wnba_schedule
sched = espn_wnba_schedule(dates=20241011)  # 2024 WNBA Finals Game 1
print(sched.shape)
sched.select(["game_id", "home_name", "away_name", "status_type_description"]).head()

Pull a full regular season's worth of games::

reg = espn_wnba_schedule(dates=2024, season_type=2, limit=500)
reg.group_by("status_type_description").len().sort("len", descending=True)

Pandas round-trip for a single date::

espn_wnba_schedule(dates=20241011, return_as_pandas=True).head()
```

### `espn_wnba_team_stats(team_id: 'int', season: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame] | dict[str, Any]'`

Pull ESPN team season stats for a WNBA team.

See :func:`sportsdataverse.wbb.espn_wbb_team_stats` for full documentation of the return shape, the canonical three category keys (``"Averages"``, ``"Totals"``, ``"Misc"``), the per-category column set, and the ``"Other"`` fallback bucket.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  | ESPN WNBA team identifier (e.g. ``17`` for the Las Vegas Aces). |
| `season` | `int` |  | Season year, forwarded to ESPN as ``?season=YYYY``. |
| `raw` | `bool` | `False` | If True, returns the parsed JSON dict before any flattening. |
| `return_as_pandas` | `bool` | `False` | If True, returns a dict of pandas DataFrames; otherwise polars. |

**Returns**

Dict with one DataFrame per stat category — see :func:`sportsdataverse.wbb.espn_wbb_team_stats` for the full column / key documentation. If ``raw=True``, returns the raw response dict.

**Example**

```python
from sportsdataverse.wnba import espn_wnba_team_stats
frames = espn_wnba_team_stats(team_id=17, season=2024)
sorted(frames.keys())  # 'Averages', 'Totals', 'Misc' (plus optional 'Other')
frames["Averages"].head()

Compare per-game and totals at a glance::

avgs = frames["Averages"]
totals = frames["Totals"]
print(avgs.shape, totals.shape)
avgs.select(["games_played", "points_per_game", "rebounds_per_game"])

Pandas round-trip::

frames_pd = espn_wnba_team_stats(team_id=17, season=2024, return_as_pandas=True)
frames_pd["Misc"].head()
```

## Utilities & helpers

### `most_recent_wnba_season()`

most_recent_wnba_season - return the most recent (likely-completed) WNBA season year.

Returns the current calendar year if it's May or later (the WNBA regular season has tipped off), otherwise the previous calendar year.

**Returns**

Year (e.g. ``2024``) suitable for passing as a ``season`` argument to schedule / loader functions.

**Example**

```python
from sportsdataverse.wnba import most_recent_wnba_season, espn_wnba_calendar
season = most_recent_wnba_season()
cal = espn_wnba_calendar(season=season)
print(season, cal.height)
```

## Other

### `espn_wnba_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wnba_teams - look up WNBA teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.wnba.espn_wnba_teams.clear_cache().

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
from sportsdataverse.wnba import espn_wnba_teams
teams = espn_wnba_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

Find Las Vegas Aces (team_id 17)::

teams.filter(__import__("polars").col("team_id") == "17").to_dicts()

Refresh the cache (the call is ``lru_cache``'d)::

espn_wnba_teams.cache_clear()  # cached at function-level
teams_pd = espn_wnba_teams(return_as_pandas=True)
```

### `scoreboard_event_parsing(event)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  |  |

### `wnba_pbp_disk(game_id, path_to_json)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |
