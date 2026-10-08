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

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `nickname` | character | Team or athlete nickname. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `age` | double | Player age (in years). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | double | Wins percentage (0-1 decimal). |
| `min` | double | Minutes played. |
| `e_off_rating` | double |  |
| `off_rating` | double |  |
| `sp_work_off_rating` | double |  |
| `e_def_rating` | double |  |
| `def_rating` | double |  |
| `sp_work_def_rating` | double |  |
| `e_net_rating` | double |  |
| `net_rating` | double | Net rating (off rating - def rating). |
| `sp_work_net_rating` | double |  |
| `ast_pct` | double | Assist percentage. |
| `ast_to` | double |  |
| `ast_ratio` | double |  |
| `oreb_pct` | double |  |
| `dreb_pct` | double |  |
| `reb_pct` | double |  |
| `tm_tov_pct` | double |  |
| `e_tov_pct` | double |  |
| `efg_pct` | double |  |
| `ts_pct` | double | True shooting percentage (0-1). |
| `usg_pct` | double |  |
| `e_usg_pct` | double |  |
| `e_pace` | double |  |
| `pace` | double | Possessions per 48 minutes. |
| `pace_per40` | double | Pace per40. |
| `sp_work_pace` | double |  |
| `pie` | double | Player Impact Estimate (0-1). |
| `poss` | integer | Poss. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fgm_pg` | double |  |
| `fga_pg` | double |  |
| `fg_pct` | double | Field goal percentage (0-1). |
| `gp_rank` | integer |  |
| `w_rank` | integer |  |
| `l_rank` | integer |  |
| `w_pct_rank` | integer |  |
| `min_rank` | integer |  |
| `e_off_rating_rank` | integer |  |
| `off_rating_rank` | integer |  |
| `sp_work_off_rating_rank` | integer |  |
| `e_def_rating_rank` | integer |  |
| `def_rating_rank` | integer |  |
| `sp_work_def_rating_rank` | integer |  |
| `e_net_rating_rank` | integer |  |
| `net_rating_rank` | integer |  |
| `sp_work_net_rating_rank` | integer |  |
| `ast_pct_rank` | integer |  |
| `ast_to_rank` | integer |  |
| `ast_ratio_rank` | integer |  |
| `oreb_pct_rank` | integer |  |
| `dreb_pct_rank` | integer |  |
| `reb_pct_rank` | integer |  |
| `tm_tov_pct_rank` | integer |  |
| `e_tov_pct_rank` | integer |  |
| `efg_pct_rank` | integer |  |
| `ts_pct_rank` | integer |  |
| `usg_pct_rank` | integer |  |
| `e_usg_pct_rank` | integer |  |
| `e_pace_rank` | integer |  |
| `pace_rank` | integer |  |
| `sp_work_pace_rank` | integer |  |
| `pie_rank` | integer |  |
| `fgm_rank` | integer |  |
| `fga_rank` | integer |  |
| `fgm_pg_rank` | integer |  |
| `fga_pg_rank` | integer |  |
| `fg_pct_rank` | integer |  |
| `team_count` | integer |  |
| `season` | integer | Season year. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `per_mode` | character |  |

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
