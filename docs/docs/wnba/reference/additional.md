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
