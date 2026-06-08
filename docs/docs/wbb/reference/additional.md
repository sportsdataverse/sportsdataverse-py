---
title: WBB — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# WBB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.wbb`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_wbb_game_officials(game_id: 'int', season: 'int | None' = None, *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull the officials assigned to a women's-college-basketball game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN event identifier (e.g. ``401637613`` for the 2024 NCAA Division I women's championship game). |
| `season` | `int \| None` | `None` | Season year. Recorded as the ``season`` column on the output; does NOT alter the request URL because ESPN's officials endpoint keys on event ID alone. |
| `raw` | `bool` | `False` | If True, returns the parsed JSON dict before any flattening. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |

**Returns**

Polars (or pandas) DataFrame with one row per official: ``game_id``, ``season``, ``official_id``, ``first_name``, ``last_name``, ``full_name``, ``display_name``, ``position_id``, ``position_name``, ``position_display_name``, ``order``. When ESPN ships no officials for the game (often for unscheduled or future events), an empty frame with the documented schema is returned so callers see a stable column set. If ``raw=True``, returns the raw response dict.

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
from sportsdataverse.wbb import espn_wbb_game_officials
officials = espn_wbb_game_officials(game_id=401587902, season=2024)
print(officials.shape)
officials.select(["full_name", "position_display_name", "order"]).head()

Pandas round-trip::

officials_pd = espn_wbb_game_officials(
    game_id=401587902, season=2024, return_as_pandas=True
)
officials_pd.head()

Raw payload (skip the cleaning pipeline)::

raw = espn_wbb_game_officials(
    game_id=401587902, season=2024, raw=True
)
sorted(raw.keys())
```

### `espn_wbb_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wbb_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from wbb_schedule(). |
| `raw` |  | `False` |  |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_nickname', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'team_alternate_ids_sdr', 'logo_href', 'logo_dark_href', 'game_id'

**Example**

```python
from sportsdataverse.wbb import espn_wbb_game_rosters
roster = espn_wbb_game_rosters(game_id=401587902)
print(roster.shape)

Identify starters::

import polars as pl
starters = roster.filter(pl.col("starter") == True).select(
    ["full_name", "jersey", "team_display_name"]
)

Pandas round-trip::

roster_pd = espn_wbb_game_rosters(game_id=401587902, return_as_pandas=True)
roster_pd.head()
```

### `espn_wbb_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'`

espn_wbb_pbp() - Pull the game by id. Data from API endpoints - `womens-college-basketball/playbyplay`,

`womens-college-basketball/summary`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from wbb_schedule(). |
| `raw` | `bool` | `False` | If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets. |

**Returns**

Dictionary of game data with keys - "gameId", "plays", "winprobability", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "timeouts", "pickcenter", "againstTheSpread", "odds", "predictor","espnWP", "gameInfo", "season"

**Example**

```python
from sportsdataverse.wbb import espn_wbb_pbp
game = espn_wbb_pbp(game_id=401587902)
print(game["gameId"])
print(len(game["plays"]))

Convert plays to a DataFrame and filter shooting plays::

import polars as pl
plays = pl.DataFrame(game["plays"])
shots = plays.filter(pl.col("scoring_play") | pl.col("shooting_play"))
shots.select(["period_number", "clock_display_value", "team_id", "coordinate_x", "coordinate_y", "score_value", "text"]).head()

Convert to pandas for downstream analysis::

import pandas as pd
shots_pd = pd.DataFrame(game["plays"])
shots_pd[shots_pd["shooting_play"] == True].head()

Raw payload (skip the cleaning pipeline) for debugging::

raw = espn_wbb_pbp(game_id=401587902, raw=True)
sorted(raw.keys())
```

### `espn_wbb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull a women's-college-basketball athlete's ESPN **season** stat line.

Returns **one wide row** combining athlete identity, the season stat line pivoted as ``{category}_{stat}`` columns, and team identity. For the richer multi-category web-v3 payload use :func:`espn_wbb_player_stats_v3` instead.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN athlete identifier (e.g. ``4433985``). |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | ``"regular"`` (type 2) or ``"postseason"`` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). Columns: identity / echo (``season``, ``season_type``, ``total``), athlete metadata (``athlete_id``, ``full_name``, ``position_*``, ...), the season stat line as ``{category}_{stat}`` numeric columns (e.g. ``offensive_points``, ``defensive_blocks``), and team metadata (``team_id``, ``team_display_name``, ...). When ``raw=True`` returns the raw statistics JSON ``dict``.

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
| `weight` | character |  |
| `display_weight` | character |  |
| `height` | double |  |
| `display_height` | character |  |
| `age` | character |  |
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
| `defensive_turnover_points` | double |  |
| `defensive_avg_defensive_rebounds` | double |  |
| `defensive_avg_blocks` | double |  |
| `defensive_avg_steals` | double |  |
| `general_disqualifications` | double |  |
| `general_flagrant_fouls` | double |  |
| `general_fouls` | double |  |
| `general_per` | double |  |
| `general_ejections` | double |  |
| `general_technical_fouls` | double |  |
| `general_rebounds` | double |  |
| `general_minutes` | double |  |
| `general_avg_minutes` | double |  |
| `general_fantasy_rating` | double |  |
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
| `general_steal_turnover_ratio` | double |  |
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
| `offensive_three_point_field_goals_attempted` | double |  |
| `offensive_three_point_field_goals_made` | double |  |
| `offensive_total_turnovers` | double |  |
| `offensive_points_in_paint` | double |  |
| `offensive_second_chance_points` | double |  |
| `offensive_fast_break_points` | double |  |
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
from sportsdataverse.wbb import espn_wbb_player_stats
df = espn_wbb_player_stats(athlete_id=4433985, season=2025)
df.select(["full_name", "team_display_name", "offensive_points"])
```

### `espn_wbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wbb_schedule - look up the women's college basketball schedule for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `groups` | `int` | `50` | Used to define different divisions. 50 is Division I, 51 is Division II/Division III. |
| `season_type` | `int` | `None` | 2 for regular season, 3 for post-season, 4 for off-season. |
| `limit` | `int` | `500` | number of records to return, default: 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

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
| `tournament_id` | integer |  |
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
| `home_conference_id` | character |  |
| `home_score` | character |  |
| `home_winner` | logical |  |
| `home_current_rank` | integer |  |
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
| `away_conference_id` | character |  |
| `away_score` | character |  |
| `away_winner` | logical |  |
| `away_current_rank` | integer |  |
| `away_linescores` | integer |  |
| `away_records` | character |  |
| `game_id` | integer |  |
| `season` | integer |  |
| `season_type` | integer |  |

**Example**

```python
from sportsdataverse.wbb import espn_wbb_schedule
day = espn_wbb_schedule(dates=20240407)
print(day.shape)

Season-level pull (2024 season)::

season = espn_wbb_schedule(dates=2024, limit=1500)
print(season.shape)

Filter to a specific team (UConn ``team_id=2509``)::

import polars as pl
uconn = season.filter(
    (pl.col("home_id") == "2509") | (pl.col("away_id") == "2509")
)

Pandas round-trip::

season_pd = espn_wbb_schedule(dates=2024, return_as_pandas=True)
season_pd.head()
```

### `espn_wbb_team_stats(team_id: 'int', season: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame] | dict[str, Any]'`

Pull ESPN team season stats for a women's-college-basketball team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  | ESPN team identifier (e.g. ``2509`` for UConn). |
| `season` | `int` |  | Season year, forwarded to ESPN as ``?season=YYYY``. |
| `raw` | `bool` | `False` | If True, returns the parsed JSON dict before any flattening. |
| `return_as_pandas` | `bool` | `False` | If True, returns a dict of pandas DataFrames; otherwise polars. |

**Returns**

Dict with one DataFrame per stat category. The canonical keys ``"Averages"``, ``"Totals"``, ``"Misc"`` are ALWAYS present; missing categories come back as empty frames carrying the documented schema. Any ESPN-shipped category whose name does not match one of the three canonical keys is collected under an additional ``"Other"`` key (only added if non-empty). Per-category column set (one row per stat): * ``stat_name`` (Utf8) * ``abbreviation`` (Utf8) * ``display_value`` (Utf8) * ``value`` (Float64) * ``description`` (Utf8) * ``category`` (Utf8, constant per frame) * ``team_id`` (Int64, constant) * ``season`` (Int32, constant) If ``raw=True``, returns the raw response dict.

**Example**

```python
from sportsdataverse.wbb import espn_wbb_team_stats
frames = espn_wbb_team_stats(team_id=2509, season=2025)
print(sorted(frames.keys()))

Index into a specific table::

averages = frames["Averages"]
print(averages.shape)
averages.select(["stat_name", "display_value", "value"]).head()

Iterate the canonical categories::

for cat in ("Averages", "Totals", "Misc"):
    print(cat, frames[cat].shape)

``Other`` fallback bucket (only present when ESPN ships a category
that does not map onto one of the three canonical keys)::

if "Other" in frames:
    frames["Other"].select(["category", "stat_name", "value"])

Pandas round-trip::

frames_pd = espn_wbb_team_stats(
    team_id=2579, season=2025, return_as_pandas=True
)  # team_id 2579 = South Carolina
frames_pd["Averages"].head()
```

## Utilities & helpers

### `most_recent_wbb_season()`

Return the most recent women's college basketball season year.

The women's college basketball season spans late October through early April; for any month October-December the "current season" is the following calendar year (e.g. October 2025 returns ``2026``).

**Returns**

The most recent / current season year.

**Example**

```python
from sportsdataverse.wbb import most_recent_wbb_season, espn_wbb_schedule
season = most_recent_wbb_season()
sched = espn_wbb_schedule(dates=season)
```

## Other

### `espn_wbb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wbb_teams - look up the women's college basketball teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `groups` | `int` | `None` | Used to define different divisions. 50 is Division I, 51 is Division II/Division III. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.wbb.espn_wbb_teams.clear_cache().

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
from sportsdataverse.wbb import espn_wbb_teams
teams = espn_wbb_teams()
print(teams.shape)
print(teams.columns[:8])

Walk every team-id (handy for batched scrapes)::

team_ids = teams["team_id"].to_list()
print(len(team_ids), "D1 teams")

Pandas round-trip + Division II/III::

d2_d3 = espn_wbb_teams(groups=51, return_as_pandas=True)
d2_d3.head()
```

### `scoreboard_event_parsing(event)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  |  |

### `wbb_pbp_disk(game_id, path_to_json)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |
