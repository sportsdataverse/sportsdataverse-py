---
title: "NFL — additional Python functions — Utilities & helpers"
sidebar_label: "Utilities & helpers"
sidebar_position: 13
description: "NFL — additional Python functions — Utilities & helpers — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Utilities & helpers

### NFLPlayProcess {#NFLPlayProcess}

`NFLPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, **kwargs)`

Process ESPN NFL play-by-play feeds into a tidy game-level dictionary.

Wraps the ESPN `summary` endpoint (or a local JSON dump) and pipes the
result through a chain of feature-engineering steps -- down/distance,
play-type flags, EPA, WPA, QBR, drive aggregation, and an advanced
box score. Use `run_processing_pipeline()` for the full feature set
or `run_cleaning_pipeline()` for a lighter clean.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gameId` | `int` | `0` | ESPN `event` id (e.g. `401671801`). |
| `raw` | `bool` | `False` | If `True`, `espn_nfl_pbp()` returns the ESPN payload untouched. If `False` (default), it normalizes keys. |
| `path_to_json` | `str` | `'/'` | Directory containing `{gameId}.json` for the `nfl_pbp_disk()` flow (offline replay). |
| `return_keys` | `list[str] \| None` | `None` | If supplied, `run_processing_pipeline` returns only the listed keys from the result dict. |

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
len(result["plays"])

# Offline replay from a JSON dump

proc = NFLPlayProcess(gameId=401671801, path_to_json="./pbp_dump")
proc.nfl_pbp_disk()
cleaned = proc.run_cleaning_pipeline()

# Subset the return payload

proc = NFLPlayProcess(gameId=401671801, return_keys=["plays", "boxscore"])
proc.espn_nfl_pbp()
slim = proc.run_processing_pipeline()
sorted(slim.keys())  # ['boxscore', 'plays']
```

**Methods**

#### NFLPlayProcess.corrupt_pbp_check

`NFLPlayProcess.corrupt_pbp_check()`

Detect ESPN payloads that look corrupt or partial.

Returns `True` when one of three guard conditions trips:

* No plays at all.
* Fewer than 50 plays for a game ESPN reports as completed.
* More than 500 plays for a game ESPN reports as completed.

`run_processing_pipeline()` and `run_cleaning_pipeline()` use
this to skip feature engineering on obviously broken payloads.

**Returns**

`True` if the payload looks corrupt; `False` otherwise.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
if not proc.corrupt_pbp_check():
    result = proc.run_processing_pipeline()
```

#### NFLPlayProcess.create_box_score

`NFLPlayProcess.create_box_score(play_df)`

Build the advanced box score (passer / rusher / receiver / team / situational / defensive / turnover / drives)

from a feature-engineered plays DataFrame.

This is normally called by `run_processing_pipeline()` -- it
auto-runs the pipeline first if it hasn't been triggered yet, so
callers can also invoke it directly on a freshly-instantiated
processor.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `play_df` | `pl.DataFrame` |  | The plays frame produced after the full feature-engineering chain (downs, play-type flags, EPA, WPA, drive aggregation). |

**Returns**

Box score keyed by `"pass"`, `"rush"`, `"receiver"`, `"team"`, `"situational"`, `"defensive"`, `"turnover"`, `"drives"` -- each value a list of dicts ready to be serialized.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
box = result["advBoxScore"]
sorted(box.keys())
```

#### NFLPlayProcess.espn_nfl_pbp

`NFLPlayProcess.espn_nfl_pbp(summary=None, **kwargs)`

espn_nfl_pbp() - Pull the game by id. Data from API endpoints: `nfl/playbyplay`, `nfl/summary`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `summary` | `dict` | `None` | A previously fetched ESPN summary payload. When given, no request is made -- the offline path for committed raw libraries -- and the pipeline joins participants only if `participants=` was passed at construction (it never fetches them, nor a roster, for a supplied summary). |

**Returns**

Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "timeouts", "homeTeamSpread", "overUnder", "pickcenter", "againstTheSpread", "odds", "predictor", "winprobability", "espnWP", "gameInfo", "season"

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403)
payload = proc.espn_nfl_pbp()
sorted(payload.keys())[:5]

# Raw ESPN passthrough (no key normalization)

proc_raw = NFLPlayProcess(gameId=401220403, raw=True)
espn_dump = proc_raw.espn_nfl_pbp()

# Chain into the full processing pipeline

proc = NFLPlayProcess(gameId=401220403)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
```

#### NFLPlayProcess.nfl_pbp_disk

`NFLPlayProcess.nfl_pbp_disk()`

Load a previously-saved ESPN payload from `{path_to_json}/{gameId}.json`.

Use this to replay an old game offline without hitting the ESPN
endpoint -- handy for snapshot-driven tests and reproducible
feature engineering.

**Returns**

The parsed JSON content; also stored on `self.json`.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403, path_to_json="./pbp_dump")
proc.nfl_pbp_disk()
result = proc.run_processing_pipeline()
```

#### NFLPlayProcess.nfl_pbp_json

`NFLPlayProcess.nfl_pbp_json(**kwargs)`

Return the JSON payload currently attached to this `NFLPlayProcess` instance.

`espn_nfl_pbp()` (live, or `summary=` offline) and `nfl_pbp_disk()`
attach the payload; this returns it unchanged.

**Returns**

dict | None: The attached payload (`self.json`); `None` before one is attached.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403)
proc.espn_nfl_pbp()
payload = proc.nfl_pbp_json()
```

#### NFLPlayProcess.run_cleaning_pipeline

`NFLPlayProcess.run_cleaning_pipeline()`

Run the lighter cleaning pipeline against `self.json`.

Identical to `run_processing_pipeline()` up through the
add_spread_time` step but stops short of EPA / WPA / QBR /
drive aggregation and the advanced box score. Use this when you
want clean play structure without the modeled features.

**Returns**

The cleaned game dict (or the subset specified by `return_keys` at construction).

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
cleaned = proc.run_cleaning_pipeline()
"plays" in cleaned and "advBoxScore" not in cleaned
```

#### NFLPlayProcess.run_processing_pipeline

`NFLPlayProcess.run_processing_pipeline(validate: 'bool' = False)`

Run the full feature-engineering pipeline against `self.json`.

Pipes the plays frame through the chain of helpers: downs,
play-type flags, rush/pass flags, team-score variables, new play
types, penalties, play-category flags, yardage cols, player cols,
post-play cols, spread time, EPA, WPA, drive data, and QBR --
followed by the advanced box score build.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `validate` | `bool` | `False` | when True, score the processed frame with the packaged per-game gate (`sportsdataverse.validation`) and attach its report dict under the `"validation"` key of the processed game (`{}` when the pipeline produced no plays). Name `"validation"` in `return_keys` to get it back when a subset was requested. Off by default -- the gate costs a few milliseconds and most callers do not read it. |

**Returns**

Dict | None: The full processed game dict (or the subset specified by `return_keys` at construction). Returns the partial result when `corrupt_pbp_check()` short-circuits.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
len(result["plays"]), len(result["drives"])

# Subset returned keys for downstream serialization

proc = NFLPlayProcess(
    gameId=401671801,
    return_keys=["plays", "advBoxScore", "winprobability"],
)
proc.espn_nfl_pbp()
slim = proc.run_processing_pipeline()
sorted(slim.keys())
```

### get_current_nfl_season {#get_current_nfl_season}

`get_current_nfl_season(roster: 'bool' = False) -> 'int'`

Return the current NFL season year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` | If True, use roster-year logic (current calendar year on/after March 15, otherwise previous year). If False, use season logic (current calendar year on/after the Thursday following Labor Day, otherwise previous year). |

**Returns**

The current season (or roster) year.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_season
season = get_current_nfl_season()
print(season)

# Roster-year semantics (March 15 cutover)

roster_year = get_current_nfl_season(roster=True)

# Pair with a loader to fetch only the active season

from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])
```

### get_current_nfl_week {#get_current_nfl_week}

`get_current_nfl_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'`

Return the current NFL week (1-22).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `use_date` | `bool` | `True` | If True (default), compute the week purely from the calendar (number of weeks since the first Thursday of September of the current season). If False, hit the live schedule via `load_nfl_schedule()` and return the week of the next unplayed game (matches nflreadpy's `use_date=False` path). |
| `roster` | `bool` | `False` | Forwarded to `get_current_nfl_season()` for season inference. |

**Returns**

The current week, capped at 22.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_week
week = get_current_nfl_week()

# Schedule-driven week (hits the live schedule parquet)

week_live = get_current_nfl_week(use_date=False)

# Roster-year season inference

week_roster = get_current_nfl_week(roster=True)

# Pair with a PBP fetch to grab only the most recent season+week

import polars as pl
from sportsdataverse.nfl import (
    get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
)
current_pbp = (
    load_nfl_pbp(seasons=[get_current_nfl_season()])
    .filter(pl.col("week") == get_current_nfl_week())
)
```

### get_current_season {#get_current_season}

`get_current_season(roster: 'bool' = False) -> 'int'`

Return the current NFL season year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` | If True, use roster-year logic (current calendar year on/after March 15, otherwise previous year). If False, use season logic (current calendar year on/after the Thursday following Labor Day, otherwise previous year). |

**Returns**

The current season (or roster) year.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_season
season = get_current_nfl_season()
print(season)

# Roster-year semantics (March 15 cutover)

roster_year = get_current_nfl_season(roster=True)

# Pair with a loader to fetch only the active season

from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])
```

### get_current_week {#get_current_week}

`get_current_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'`

Return the current NFL week (1-22).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `use_date` | `bool` | `True` | If True (default), compute the week purely from the calendar (number of weeks since the first Thursday of September of the current season). If False, hit the live schedule via `load_nfl_schedule()` and return the week of the next unplayed game (matches nflreadpy's `use_date=False` path). |
| `roster` | `bool` | `False` | Forwarded to `get_current_nfl_season()` for season inference. |

**Returns**

The current week, capped at 22.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_week
week = get_current_nfl_week()

# Schedule-driven week (hits the live schedule parquet)

week_live = get_current_nfl_week(use_date=False)

# Roster-year season inference

week_roster = get_current_nfl_week(roster=True)

# Pair with a PBP fetch to grab only the most recent season+week

import polars as pl
from sportsdataverse.nfl import (
    get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
)
current_pbp = (
    load_nfl_pbp(seasons=[get_current_nfl_season()])
    .filter(pl.col("week") == get_current_nfl_week())
)
```

### most_recent_nfl_season {#most_recent_nfl_season}

`most_recent_nfl_season(roster: 'bool' = False) -> 'int'`

Alias for `get_current_nfl_season()` mirroring nflreadr's

`most_recent_season()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl.utils_date import most_recent_nfl_season
season = most_recent_nfl_season()

# Roster-year flavor

roster_year = most_recent_nfl_season(roster=True)
```
