---
title: "NFL — additional Python functions — Dates and seasons"
sidebar_label: "Dates and seasons"
sidebar_position: 15
description: "NFL — additional Python functions — Dates and seasons — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Dates and seasons

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
| `roster` | `bool` | `False` | Use roster-year logic (the current year from March 15) instead of season logic. |

**Returns**

The current NFL season (or roster) year, from `get_current_nfl_season`.

**Example**

```python
from sportsdataverse.nfl.utils_date import most_recent_nfl_season
season = most_recent_nfl_season()

# Roster-year flavor

roster_year = most_recent_nfl_season(roster=True)
```
