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
