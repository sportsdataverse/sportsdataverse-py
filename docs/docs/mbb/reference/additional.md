---
title: MBB — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# MBB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.mbb`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_mbb_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mbb_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from mbb_schedule(). |
| `raw` |  | `False` |  |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_nickname', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'team_alternate_ids_sdr', 'logo_href', 'logo_dark_href', 'game_id'

**Example**

```python
from sportsdataverse.mbb import espn_mbb_game_rosters
roster = espn_mbb_game_rosters(game_id=401638637)
print(roster.shape)

Identify starters::

import polars as pl
starters = roster.filter(pl.col("starter") == True).select(
    ["full_name", "jersey", "team_display_name"]
)

Pandas round-trip::

roster_pd = espn_mbb_game_rosters(game_id=401638637, return_as_pandas=True)
roster_pd.head()
```

### `espn_mbb_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'`

espn_mbb_pbp() - Pull the game by id. Data from API endpoints: `mens-college-basketball/playbyplay`, `mens-college-basketball/summary`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from mbb_schedule(). |
| `raw` | `bool` | `False` | If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets. |

**Returns**

Dictionary of game data with keys: "gameId", "plays", "winprobability", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "timeouts", "pickcenter", "againstTheSpread", "odds", "predictor", "espnWP", "gameInfo", "season"

**Example**

```python
from sportsdataverse.mbb import espn_mbb_pbp
game = espn_mbb_pbp(game_id=401638637)
print(game["gameId"])
print(len(game["plays"]))

Filter shooting plays for a basic shot chart::

import polars as pl
plays = pl.DataFrame(game["plays"])
shots = plays.filter(pl.col("shooting_play") == True)
shots.select(
    [
        "period_number",
        "clock_display_value",
        "team_id",
        "coordinate_x",
        "coordinate_y",
        "score_value",
        "text",
    ]
).head()

Convert to pandas::

import pandas as pd
plays_pd = pd.DataFrame(game["plays"])
plays_pd[plays_pd["shooting_play"] == True].head()

Raw payload (skip the cleaning pipeline) for debugging::

raw = espn_mbb_pbp(game_id=401638637, raw=True)
sorted(raw.keys())
```

### `espn_mbb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull a men's-college-basketball athlete's ESPN **season** stat line.

See :func:`sportsdataverse.wbb.espn_wbb_player_stats` for full documentation of the wide return shape, the ``{category}_{stat}`` stat columns, the athlete / team metadata blocks, and the ``season_type`` / ``total`` parameters. For the richer web-v3 payload use :func:`sportsdataverse.mbb.espn_mbb_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN men's-college-basketball athlete identifier. |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | ``"regular"`` (type 2) or ``"postseason"`` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When ``raw=True`` returns the raw statistics JSON ``dict``.

**Example**

```python
from sportsdataverse.mbb import espn_mbb_player_stats
df = espn_mbb_player_stats(athlete_id=4395624, season=2023)
df.select(["full_name", "team_display_name", "offensive_points"])
```

### `espn_mbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mbb_schedule - look up the men's college basketball scheduler for a given season

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

**Example**

```python
from sportsdataverse.mbb import espn_mbb_schedule
day = espn_mbb_schedule(dates=20240408)
print(day.shape)

Season-level pull (2024 season)::

season = espn_mbb_schedule(dates=2024, limit=1500)
print(season.shape)

Filter to a specific team (Duke ``team_id=150``)::

import polars as pl
duke = season.filter(
    (pl.col("home_id") == "150") | (pl.col("away_id") == "150")
)

Pandas round-trip::

season_pd = espn_mbb_schedule(dates=2024, return_as_pandas=True)
season_pd.head()
```

## Utilities & helpers

### `most_recent_mbb_season()`

Return the most recent men's college basketball season year.

The men's college basketball season spans early November through early April; for any month October-December the "current season" is the following calendar year (e.g. October 2025 returns ``2026``).

**Returns**

The most recent / current season year.

**Example**

```python
from sportsdataverse.mbb import most_recent_mbb_season, espn_mbb_schedule
season = most_recent_mbb_season()
sched = espn_mbb_schedule(dates=season)
```

## Other

### `espn_mbb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mbb_teams - look up the men's college basketball teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `groups` | `int` | `None` | Used to define different divisions. 50 is Division I, 51 is Division II/Division III. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.mbb.espn_mbb_teams.clear_cache().

**Example**

```python
from sportsdataverse.mbb import espn_mbb_teams
teams = espn_mbb_teams()
print(teams.shape)
print(teams.columns[:8])

Walk every team-id (handy for batched scrapes)::

team_ids = teams["team_id"].to_list()
print(len(team_ids), "D1 teams")

Pandas round-trip + Division II/III::

d2_d3 = espn_mbb_teams(groups=51, return_as_pandas=True)
d2_d3.head()
```

### `mbb_pbp_disk(game_id, path_to_json)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

### `scoreboard_event_parsing(event)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  |  |
