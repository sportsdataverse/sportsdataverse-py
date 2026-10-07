---
title: "NBA — additional Python functions — sportsdataverse-data releases"
sidebar_label: "sportsdataverse-data releases"
sidebar_position: 3
description: "NBA — additional Python functions — sportsdataverse-data releases — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — sportsdataverse-data releases

### load_nba_stats_leaguedash {#load_nba_stats_leaguedash}

`load_nba_stats_leaguedash(family: 'str', seasons: 'int | Iterable[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load one asset family of the `nba_stats_leaguedash` release.

`nba_stats_leaguedash` is a parameter cube: one asset per
(family, season) pair rather than one per season, so a family must be named.
The valid families are exported as
`NBA_STATS_LEAGUEDASH_FAMILIES` -- import that tuple to discover them
rather than passing a bare string; an unknown family raises `ValueError`
listing every valid value.

Column sets are family-specific (a `lineups_*` frame keys on `group_id`,
a `player_*` frame on `player_id`), so this loader documents no fixed
returns table. `player_id` / `team_id` are `Int64` in every family and
season, so cross-family joins need no dtype reconciliation.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `family` | `str` |  | Asset family, e.g. `"player_stats_advanced"`. Must be one of `NBA_STATS_LEAGUEDASH_FAMILIES`. |
| `seasons` | `int \| Iterable[int]` |  | Season, or iterable of seasons, to load. Seasons are END years (`2024` = the 2023-24 NBA season). 1996 is the earliest season on the tag; per-family coverage starts later (`lineups_*` 2008, most `player_tracking_*` 2014). A requested season the family does not publish is warned about and skipped, not an error. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe with one row per player / team / lineup per requested season for the requested family; an empty frame when no requested season is published.

**Example**

```python
from sportsdataverse.nba import load_nba_stats_leaguedash
adv = load_nba_stats_leaguedash("player_stats_advanced", seasons=2024)
print(adv.shape)

# Discover the valid families

from sportsdataverse.nba import NBA_STATS_LEAGUEDASH_FAMILIES
print([f for f in NBA_STATS_LEAGUEDASH_FAMILIES if f.startswith("player_tracking_")])

# Multi-season, pandas round-trip

drives_pd = load_nba_stats_leaguedash(
    "player_tracking_drives", seasons=range(2020, 2025), return_as_pandas=True
)

# Pipeline next step (top usage rates in 2024)

import polars as pl
usage = load_nba_stats_leaguedash("player_stats_usage", seasons=2024)
usage.sort("usg_pct", descending=True).head()
```
