---
title: "WNBA — additional Python functions — sportsdataverse-data releases"
sidebar_label: "sportsdataverse-data releases"
sidebar_position: 2
description: "WNBA — additional Python functions — sportsdataverse-data releases — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — sportsdataverse-data releases

### load_wnba_stats_leaguedash {#load_wnba_stats_leaguedash}

`load_wnba_stats_leaguedash(family: 'str', seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load one asset family of the `wnba_stats_leaguedash` release.

`wnba_stats_leaguedash` is a parameter cube: one asset per
(family, season) pair rather than one per season, so a family must be named.
The valid families are exported as `WNBA_STATS_LEAGUEDASH_FAMILIES` --
import that tuple to discover them rather than passing a bare string; an
unknown family raises `ValueError` listing every valid value. This is the
non-deprecated way to reach the cube; the four `load_wnba_stats_*` shims
below only reconstruct retired tags' stacked shapes from it.

Column sets are family-specific (a `lineups_*` frame keys on `group_id`,
a `player_*` frame on `player_id`), so this loader documents no fixed
returns table. `player_id` / `team_id` are `Int64` in every family and
season, so cross-family joins need no dtype reconciliation.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `family` | `str` |  | Asset family, e.g. `"player_stats_advanced"`. Must be one of `WNBA_STATS_LEAGUEDASH_FAMILIES`. |
| `seasons` | `int \| Iterable[int]` |  | Season, or iterable of seasons, to load. WNBA seasons are single calendar years. 1997 is the earliest season on the tag. A requested season the family does not publish is warned about and skipped, not an error. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe with one row per player / team / lineup per requested season for the requested family; an empty frame when no requested season is published.

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_leaguedash
adv = load_wnba_stats_leaguedash("player_stats_advanced", seasons=2025)
print(adv.shape)

# Discover the valid families

from sportsdataverse.wnba import WNBA_STATS_LEAGUEDASH_FAMILIES
print(WNBA_STATS_LEAGUEDASH_FAMILIES)

# Multi-season, pandas round-trip

team_pd = load_wnba_stats_leaguedash(
    "team_stats_base", seasons=range(2020, 2026), return_as_pandas=True
)

# Pipeline next step (best net rating in 2025)

import polars as pl
load_wnba_stats_leaguedash("team_stats_advanced", seasons=2025).sort(
    "net_rating", descending=True
).head()
```

### load_wnba_stats_lineups {#load_wnba_stats_lineups}

`load_wnba_stats_lineups(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load season-level WNBA 5-man lineup statistics (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per lineup-season-measure_type, stacked from the `wnba_stats_leaguedash` cube's `lineups_{base, advanced}` assets filtered to `group_quantity == 5` — matching the old `wnba_stats_lineups` tag's 5-man-only, Base+Advanced-only coverage. Call the cube's `lineups_*` assets directly (unfiltered) for 2/3/4-man lineups or the other 4 measure types.

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_lineups
df = load_wnba_stats_lineups(seasons=2026)
print(df.shape)
```

### load_wnba_stats_player_season_stats {#load_wnba_stats_player_season_stats}

`load_wnba_stats_player_season_stats(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load season-level WNBA player statistics (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per player-season-measure_type, stacked from the `wnba_stats_leaguedash` cube's `player_stats_*` assets (`Base`/`Advanced`/`Misc`/`Scoring`/`Usage`/`Defense` — matches the old `wnba_stats_player_season_stats` tag's coverage; player-level `Opponent`/`Four Factors` are empty upstream and were never populated by either version).

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_player_season_stats
df = load_wnba_stats_player_season_stats(seasons=2026)
print(df.shape)

# Pipeline next step (Advanced-only rows)

import polars as pl
adv = df.filter(pl.col("measure_type") == "Advanced")
```

### load_wnba_stats_standings {#load_wnba_stats_standings}

`load_wnba_stats_standings(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load season-level WNBA standings (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per team-season, read from the `wnba_stats_leaguedash` cube's `standings` asset -- the same underlying `leaguestandingsv3` endpoint/params as the old `wnba_stats_standings` tag, so this is close to a pure passthrough.

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_standings
df = load_wnba_stats_standings(seasons=2026)
print(df.shape)
```

### load_wnba_stats_team_season_stats {#load_wnba_stats_team_season_stats}

`load_wnba_stats_team_season_stats(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load season-level WNBA team statistics (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per team-season-measure_type, stacked from the `wnba_stats_leaguedash` cube's `team_stats_*` assets (`Base`/`Advanced`/`Misc`/`Scoring`/`Defense`/ `Opponent` — matches the old `wnba_stats_team_season_stats` tag's coverage; team-level `Usage`/`Four Factors` are empty upstream).

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_team_season_stats
df = load_wnba_stats_team_season_stats(seasons=2026)
print(df.shape)
```
