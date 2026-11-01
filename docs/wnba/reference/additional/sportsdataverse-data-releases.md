# WNBA — additional Python functions — sportsdataverse-data releases

> WNBA — additional Python functions — sportsdataverse-data releases — function reference in sdv-py, the SportsDataverse Python package.

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

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `nickname` | String | Team or athlete nickname. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `age` | Float64 | Player age (in years). |
| `gp` | Int64 | Games played. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_pct` | Float64 | Wins percentage (0-1 decimal). |
| `min` | Float64 | Minutes played. |
| `e_off_rating` | Float64 |  |
| `off_rating` | Float64 |  |
| `sp_work_off_rating` | Float64 |  |
| `e_def_rating` | Float64 |  |
| `def_rating` | Float64 |  |
| `sp_work_def_rating` | Float64 |  |
| `e_net_rating` | Float64 |  |
| `net_rating` | Float64 | Net rating (off rating - def rating). |
| `sp_work_net_rating` | Float64 |  |
| `ast_pct` | Float64 | Assist percentage. |
| `ast_to` | Float64 |  |
| `ast_ratio` | Float64 |  |
| `oreb_pct` | Float64 |  |
| `dreb_pct` | Float64 |  |
| `reb_pct` | Float64 |  |
| `tm_tov_pct` | Float64 |  |
| `e_tov_pct` | Float64 |  |
| `efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `usg_pct` | Float64 |  |
| `e_usg_pct` | Float64 |  |
| `e_pace` | Float64 |  |
| `pace` | Float64 | Possessions per 48 minutes. |
| `pace_per40` | Float64 | Pace per40. |
| `sp_work_pace` | Float64 |  |
| `pie` | Float64 | Player Impact Estimate (0-1). |
| `poss` | Int64 | Poss. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fgm_pg` | Float64 |  |
| `fga_pg` | Float64 |  |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `gp_rank` | Int64 |  |
| `w_rank` | Int64 |  |
| `l_rank` | Int64 |  |
| `w_pct_rank` | Int64 |  |
| `min_rank` | Int64 |  |
| `e_off_rating_rank` | Int64 |  |
| `off_rating_rank` | Int64 |  |
| `sp_work_off_rating_rank` | Int64 |  |
| `e_def_rating_rank` | Int64 |  |
| `def_rating_rank` | Int64 |  |
| `sp_work_def_rating_rank` | Int64 |  |
| `e_net_rating_rank` | Int64 |  |
| `net_rating_rank` | Int64 |  |
| `sp_work_net_rating_rank` | Int64 |  |
| `ast_pct_rank` | Int64 |  |
| `ast_to_rank` | Int64 |  |
| `ast_ratio_rank` | Int64 |  |
| `oreb_pct_rank` | Int64 |  |
| `dreb_pct_rank` | Int64 |  |
| `reb_pct_rank` | Int64 |  |
| `tm_tov_pct_rank` | Int64 |  |
| `e_tov_pct_rank` | Int64 |  |
| `efg_pct_rank` | Int64 |  |
| `ts_pct_rank` | Int64 |  |
| `usg_pct_rank` | Int64 |  |
| `e_usg_pct_rank` | Int64 |  |
| `e_pace_rank` | Int64 |  |
| `pace_rank` | Int64 |  |
| `sp_work_pace_rank` | Int64 |  |
| `pie_rank` | Int64 |  |
| `fgm_rank` | Int64 |  |
| `fga_rank` | Int64 |  |
| `fgm_pg_rank` | Int64 |  |
| `fga_pg_rank` | Int64 |  |
| `fg_pct_rank` | Int64 |  |
| `team_count` | Int64 |  |
| `season` | Int64 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `league_id` | String | League identifier ('10' = WNBA). |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `per_mode` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `family` is not one of `WNBA_STATS_LEAGUEDASH_FAMILIES`. |
| `SeasonNotFoundError` | If any requested season is less than 1997. |
| `AssetFetchError` | If a release asset request fails (a 403 or an exhausted retry budget) -- distinct from an absent season, which is skipped. |

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

| col_name | type | description |
|---|---|---|
| `group_set` | String |  |
| `group_id` | String | ESPN group id. |
| `group_name` | String |  |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `gp` | Int64 | Games played. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_pct` | Float64 | Wins percentage (0-1 decimal). |
| `min` | Float64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Total rebounds. |
| `ast` | Int64 | Assists. |
| `tov` | Float64 | Turnovers. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `blka` | Int64 |  |
| `pf` | Int64 | Personal fouls. |
| `pfd` | Int64 |  |
| `pts` | Int64 | Points scored. |
| `plus_minus` | Float64 | Plus/minus point differential while on court. |
| `gp_rank` | Int64 |  |
| `w_rank` | Int64 |  |
| `l_rank` | Int64 |  |
| `w_pct_rank` | Int64 |  |
| `min_rank` | Int64 |  |
| `fgm_rank` | Int64 |  |
| `fga_rank` | Int64 |  |
| `fg_pct_rank` | Int64 |  |
| `fg3_m_rank` | Int64 |  |
| `fg3_a_rank` | Int64 |  |
| `fg3_pct_rank` | Int64 |  |
| `ftm_rank` | Int64 |  |
| `fta_rank` | Int64 |  |
| `ft_pct_rank` | Int64 |  |
| `oreb_rank` | Int64 |  |
| `dreb_rank` | Int64 |  |
| `reb_rank` | Int64 |  |
| `ast_rank` | Int64 |  |
| `tov_rank` | Int64 |  |
| `stl_rank` | Int64 |  |
| `blk_rank` | Int64 |  |
| `blka_rank` | Int64 |  |
| `pf_rank` | Int64 |  |
| `pfd_rank` | Int64 |  |
| `pts_rank` | Int64 |  |
| `plus_minus_rank` | Int64 |  |
| `sum_time_played` | Int64 |  |
| `season` | Int64 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `league_id` | String | League identifier ('10' = WNBA). |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `per_mode` | String |  |
| `group_quantity` | Int64 |  |
| `measure_type` | String |  |
| `e_off_rating` | Float64 |  |
| `off_rating` | Float64 |  |
| `e_def_rating` | Float64 |  |
| `def_rating` | Float64 |  |
| `e_net_rating` | Float64 |  |
| `net_rating` | Float64 | Net rating (off rating - def rating). |
| `ast_pct` | Float64 | Assist percentage. |
| `ast_to` | Float64 |  |
| `ast_ratio` | Float64 |  |
| `oreb_pct` | Float64 |  |
| `dreb_pct` | Float64 |  |
| `reb_pct` | Float64 |  |
| `tm_tov_pct` | Float64 |  |
| `efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `e_pace` | Float64 |  |
| `pace` | Float64 | Possessions per 48 minutes. |
| `pace_per40` | Float64 | Pace per40. |
| `poss` | Int64 | Poss. |
| `pie` | Float64 | Player Impact Estimate (0-1). |
| `off_rating_rank` | Int64 |  |
| `def_rating_rank` | Int64 |  |
| `net_rating_rank` | Int64 |  |
| `ast_pct_rank` | Int64 |  |
| `ast_to_rank` | Int64 |  |
| `ast_ratio_rank` | Int64 |  |
| `oreb_pct_rank` | Int64 |  |
| `dreb_pct_rank` | Int64 |  |
| `reb_pct_rank` | Int64 |  |
| `tm_tov_pct_rank` | Int64 |  |
| `efg_pct_rank` | Int64 |  |
| `ts_pct_rank` | Int64 |  |
| `pace_rank` | Int64 |  |
| `pie_rank` | Int64 |  |

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

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `nickname` | String | Team or athlete nickname. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `age` | Float64 | Player age (in years). |
| `gp` | Int64 | Games played. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_pct` | Float64 | Wins percentage (0-1 decimal). |
| `min` | Float64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Total rebounds. |
| `ast` | Int64 | Assists. |
| `tov` | Int64 | Turnovers. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `blka` | Int64 |  |
| `pf` | Int64 | Personal fouls. |
| `pfd` | Int64 |  |
| `pts` | Int64 | Points scored. |
| `plus_minus` | Int64 | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | Float64 |  |
| `dd2` | Int64 |  |
| `td3` | Int64 |  |
| `wnba_fantasy_pts` | Float64 |  |
| `gp_rank` | Int64 |  |
| `w_rank` | Int64 |  |
| `l_rank` | Int64 |  |
| `w_pct_rank` | Int64 |  |
| `min_rank` | Int64 |  |
| `fgm_rank` | Int64 |  |
| `fga_rank` | Int64 |  |
| `fg_pct_rank` | Int64 |  |
| `fg3_m_rank` | Int64 |  |
| `fg3_a_rank` | Int64 |  |
| `fg3_pct_rank` | Int64 |  |
| `ftm_rank` | Int64 |  |
| `fta_rank` | Int64 |  |
| `ft_pct_rank` | Int64 |  |
| `oreb_rank` | Int64 |  |
| `dreb_rank` | Int64 |  |
| `reb_rank` | Int64 |  |
| `ast_rank` | Int64 |  |
| `tov_rank` | Int64 |  |
| `stl_rank` | Int64 |  |
| `blk_rank` | Int64 |  |
| `blka_rank` | Int64 |  |
| `pf_rank` | Int64 |  |
| `pfd_rank` | Int64 |  |
| `pts_rank` | Int64 |  |
| `plus_minus_rank` | Int64 |  |
| `nba_fantasy_pts_rank` | Int64 |  |
| `dd2_rank` | Int64 |  |
| `td3_rank` | Int64 |  |
| `wnba_fantasy_pts_rank` | Int64 |  |
| `team_count` | Int64 |  |
| `season` | Int64 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `league_id` | String | League identifier ('10' = WNBA). |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `per_mode` | String |  |
| `measure_type` | String |  |
| `e_off_rating` | Float64 |  |
| `off_rating` | Float64 |  |
| `sp_work_off_rating` | Float64 |  |
| `e_def_rating` | Float64 |  |
| `def_rating` | Float64 |  |
| `sp_work_def_rating` | Float64 |  |
| `e_net_rating` | Float64 |  |
| `net_rating` | Float64 | Net rating (off rating - def rating). |
| `sp_work_net_rating` | Float64 |  |
| `ast_pct` | Float64 | Assist percentage. |
| `ast_to` | Float64 |  |
| `ast_ratio` | Float64 |  |
| `oreb_pct` | Float64 |  |
| `dreb_pct` | Float64 |  |
| `reb_pct` | Float64 |  |
| `tm_tov_pct` | Float64 |  |
| `e_tov_pct` | Float64 |  |
| `efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `usg_pct` | Float64 |  |
| `e_usg_pct` | Float64 |  |
| `e_pace` | Float64 |  |
| `pace` | Float64 | Possessions per 48 minutes. |
| `pace_per40` | Float64 | Pace per40. |
| `sp_work_pace` | Float64 |  |
| `pie` | Float64 | Player Impact Estimate (0-1). |
| `poss` | Int64 | Poss. |
| `fgm_pg` | Float64 |  |
| `fga_pg` | Float64 |  |
| `e_off_rating_rank` | Int64 |  |
| `off_rating_rank` | Int64 |  |
| `sp_work_off_rating_rank` | Int64 |  |
| `e_def_rating_rank` | Int64 |  |
| `def_rating_rank` | Int64 |  |
| `sp_work_def_rating_rank` | Int64 |  |
| `e_net_rating_rank` | Int64 |  |
| `net_rating_rank` | Int64 |  |
| `sp_work_net_rating_rank` | Int64 |  |
| `ast_pct_rank` | Int64 |  |
| `ast_to_rank` | Int64 |  |
| `ast_ratio_rank` | Int64 |  |
| `oreb_pct_rank` | Int64 |  |
| `dreb_pct_rank` | Int64 |  |
| `reb_pct_rank` | Int64 |  |
| `tm_tov_pct_rank` | Int64 |  |
| `e_tov_pct_rank` | Int64 |  |
| `efg_pct_rank` | Int64 |  |
| `ts_pct_rank` | Int64 |  |
| `usg_pct_rank` | Int64 |  |
| `e_usg_pct_rank` | Int64 |  |
| `e_pace_rank` | Int64 |  |
| `pace_rank` | Int64 |  |
| `sp_work_pace_rank` | Int64 |  |
| `pie_rank` | Int64 |  |
| `fgm_pg_rank` | Int64 |  |
| `fga_pg_rank` | Int64 |  |
| `pts_off_tov` | Int64 |  |
| `pts_2_nd_chance` | Int64 |  |
| `pts_fb` | Int64 |  |
| `pts_paint` | Int64 |  |
| `opp_pts_off_tov` | Float64 |  |
| `opp_pts_2_nd_chance` | Float64 |  |
| `opp_pts_fb` | Float64 |  |
| `opp_pts_paint` | Float64 |  |
| `pts_off_tov_rank` | Int64 |  |
| `pts_2_nd_chance_rank` | Int64 |  |
| `pts_fb_rank` | Int64 |  |
| `pts_paint_rank` | Int64 |  |
| `opp_pts_off_tov_rank` | Int64 |  |
| `opp_pts_2_nd_chance_rank` | Int64 |  |
| `opp_pts_fb_rank` | Int64 |  |
| `opp_pts_paint_rank` | Int64 |  |
| `pct_fga_2_pt` | Float64 |  |
| `pct_fga_3_pt` | Float64 |  |
| `pct_pts_2_pt` | Float64 |  |
| `pct_pts_2_pt_mr` | Float64 |  |
| `pct_pts_3_pt` | Float64 |  |
| `pct_pts_fb` | Float64 |  |
| `pct_pts_ft` | Float64 |  |
| `pct_pts_off_tov` | Float64 |  |
| `pct_pts_paint` | Float64 |  |
| `pct_ast_2_pm` | Float64 |  |
| `pct_uast_2_pm` | Float64 |  |
| `pct_ast_3_pm` | Float64 |  |
| `pct_uast_3_pm` | Float64 |  |
| `pct_ast_fgm` | Float64 |  |
| `pct_uast_fgm` | Float64 |  |
| `pct_fga_2_pt_rank` | Int64 |  |
| `pct_fga_3_pt_rank` | Int64 |  |
| `pct_pts_2_pt_rank` | Int64 |  |
| `pct_pts_2_pt_mr_rank` | Int64 |  |
| `pct_pts_3_pt_rank` | Int64 |  |
| `pct_pts_fb_rank` | Int64 |  |
| `pct_pts_ft_rank` | Int64 |  |
| `pct_pts_off_tov_rank` | Int64 |  |
| `pct_pts_paint_rank` | Int64 |  |
| `pct_ast_2_pm_rank` | Int64 |  |
| `pct_uast_2_pm_rank` | Int64 |  |
| `pct_ast_3_pm_rank` | Int64 |  |
| `pct_uast_3_pm_rank` | Int64 |  |
| `pct_ast_fgm_rank` | Int64 |  |
| `pct_uast_fgm_rank` | Int64 |  |
| `pct_fgm` | Float64 |  |
| `pct_fga` | Float64 |  |
| `pct_fg3_m` | Float64 |  |
| `pct_fg3_a` | Float64 |  |
| `pct_ftm` | Float64 |  |
| `pct_fta` | Float64 |  |
| `pct_oreb` | Float64 |  |
| `pct_dreb` | Float64 |  |
| `pct_reb` | Float64 |  |
| `pct_ast` | Float64 |  |
| `pct_tov` | Float64 |  |
| `pct_stl` | Float64 |  |
| `pct_blk` | Float64 |  |
| `pct_blka` | Float64 |  |
| `pct_pf` | Float64 |  |
| `pct_pfd` | Float64 |  |
| `pct_pts` | Float64 |  |
| `pct_fgm_rank` | Int64 |  |
| `pct_fga_rank` | Int64 |  |
| `pct_fg3_m_rank` | Int64 |  |
| `pct_fg3_a_rank` | Int64 |  |
| `pct_ftm_rank` | Int64 |  |
| `pct_fta_rank` | Int64 |  |
| `pct_oreb_rank` | Int64 |  |
| `pct_dreb_rank` | Int64 |  |
| `pct_reb_rank` | Int64 |  |
| `pct_ast_rank` | Int64 |  |
| `pct_tov_rank` | Int64 |  |
| `pct_stl_rank` | Int64 |  |
| `pct_blk_rank` | Int64 |  |
| `pct_blka_rank` | Int64 |  |
| `pct_pf_rank` | Int64 |  |
| `pct_pfd_rank` | Int64 |  |
| `pct_pts_rank` | Int64 |  |
| `def_ws` | Float64 |  |
| `def_ws_raw` | Float64 |  |
| `def_ws_rank` | Int64 |  |

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

| col_name | type | description |
|---|---|---|
| `league_id` | String | League identifier ('10' = WNBA). |
| `season_id` | String | Unique season identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `conference` | String | Filter players or teams by conference. |
| `conference_record` | String | Conference win-loss record. |
| `playoff_rank` | Int64 | League/season rank for playoff. |
| `clinch_indicator` | String | Playoff clinch indicator (e.g. 'x' clinched playoff, 'e' eliminated). |
| `division` | String | Team division. |
| `division_record` | String |  |
| `division_rank` | Int64 |  |
| `wins` | Int64 | Total wins. |
| `losses` | Int64 | Total losses. |
| `win_pct` | Float64 | Win percentage (0-1 decimal). |
| `league_rank` | Int64 |  |
| `record` | String | Record string (e.g. '12-4'). |
| `home` | String | Home. |
| `road` | String | Road. |
| `l10` | String | L10. |
| `last10_home` | String |  |
| `last10_road` | String |  |
| `ot` | String | Ot. |
| `three_pts_or_less` | String |  |
| `ten_pts_or_more` | String |  |
| `long_home_streak` | Int64 |  |
| `str_long_home_streak` | String |  |
| `long_road_streak` | Int64 |  |
| `str_long_road_streak` | String |  |
| `long_win_streak` | Int64 |  |
| `long_loss_streak` | Int64 |  |
| `current_home_streak` | Int64 |  |
| `str_current_home_streak` | String |  |
| `current_road_streak` | Int64 |  |
| `str_current_road_streak` | String |  |
| `current_streak` | Int64 |  |
| `str_current_streak` | String |  |
| `conference_games_back` | Float64 |  |
| `division_games_back` | Float64 |  |
| `clinched_conference_title` | Int64 |  |
| `clinched_division_title` | Int64 |  |
| `clinched_playoff_birth` | Int64 |  |
| `clinched_play_in` | Int64 |  |
| `eliminated_conference` | Int64 |  |
| `eliminated_division` | Int64 |  |
| `ahead_at_half` | String |  |
| `behind_at_half` | String |  |
| `tied_at_half` | String |  |
| `ahead_at_third` | String |  |
| `behind_at_third` | String |  |
| `tied_at_third` | String |  |
| `score100_pts` | String |  |
| `opp_score100_pts` | String |  |
| `opp_over500` | String |  |
| `lead_in_fgpct` | String |  |
| `lead_in_reb` | String |  |
| `fewer_turnovers` | String |  |
| `points_pg` | Float64 | Points pg. |
| `opp_points_pg` | Float64 | Opponent points pg. |
| `diff_points_pg` | Float64 | Diff points pg. |
| `vs_east` | String |  |
| `vs_atlantic` | String |  |
| `vs_central` | String |  |
| `vs_southeast` | String |  |
| `vs_west` | String |  |
| `vs_northwest` | String |  |
| `vs_pacific` | String |  |
| `vs_southwest` | String |  |
| `jan` | String |  |
| `feb` | String |  |
| `mar` | String |  |
| `apr` | String |  |
| `may` | String |  |
| `jun` | String |  |
| `jul` | String |  |
| `aug` | String |  |
| `sep` | String |  |
| `oct` | String |  |
| `nov` | String |  |
| `dec` | String |  |
| `score_80_plus` | String |  |
| `opp_score_80_plus` | String |  |
| `score_below_80` | String |  |
| `opp_score_below_80` | String |  |
| `total_points` | Int64 |  |
| `opp_total_points` | Int64 |  |
| `diff_total_points` | Int64 |  |
| `league_games_back` | Float64 |  |
| `playoff_seeding` | String |  |
| `clinched_post_season` | Int64 |  |
| `neutral` | String | Neutral. |
| `season` | Int64 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

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

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `gp` | Int64 | Games played. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_pct` | Float64 | Wins percentage (0-1 decimal). |
| `min` | Float64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Total rebounds. |
| `ast` | Int64 | Assists. |
| `tov` | Float64 | Turnovers. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `blka` | Int64 |  |
| `pf` | Int64 | Personal fouls. |
| `pfd` | Int64 |  |
| `pts` | Int64 | Points scored. |
| `plus_minus` | Float64 | Plus/minus point differential while on court. |
| `gp_rank` | Int64 |  |
| `w_rank` | Int64 |  |
| `l_rank` | Int64 |  |
| `w_pct_rank` | Int64 |  |
| `min_rank` | Int64 |  |
| `fgm_rank` | Int64 |  |
| `fga_rank` | Int64 |  |
| `fg_pct_rank` | Int64 |  |
| `fg3_m_rank` | Int64 |  |
| `fg3_a_rank` | Int64 |  |
| `fg3_pct_rank` | Int64 |  |
| `ftm_rank` | Int64 |  |
| `fta_rank` | Int64 |  |
| `ft_pct_rank` | Int64 |  |
| `oreb_rank` | Int64 |  |
| `dreb_rank` | Int64 |  |
| `reb_rank` | Int64 |  |
| `ast_rank` | Int64 |  |
| `tov_rank` | Int64 |  |
| `stl_rank` | Int64 |  |
| `blk_rank` | Int64 |  |
| `blka_rank` | Int64 |  |
| `pf_rank` | Int64 |  |
| `pfd_rank` | Int64 |  |
| `pts_rank` | Int64 |  |
| `plus_minus_rank` | Int64 |  |
| `season` | Int64 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `league_id` | String | League identifier ('10' = WNBA). |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `per_mode` | String |  |
| `measure_type` | String |  |
| `e_off_rating` | Float64 |  |
| `off_rating` | Float64 |  |
| `e_def_rating` | Float64 |  |
| `def_rating` | Float64 |  |
| `e_net_rating` | Float64 |  |
| `net_rating` | Float64 | Net rating (off rating - def rating). |
| `ast_pct` | Float64 | Assist percentage. |
| `ast_to` | Float64 |  |
| `ast_ratio` | Float64 |  |
| `oreb_pct` | Float64 |  |
| `dreb_pct` | Float64 |  |
| `reb_pct` | Float64 |  |
| `tm_tov_pct` | Float64 |  |
| `efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `e_pace` | Float64 |  |
| `pace` | Float64 | Possessions per 48 minutes. |
| `pace_per40` | Float64 | Pace per40. |
| `poss` | Int64 | Poss. |
| `pie` | Float64 | Player Impact Estimate (0-1). |
| `off_rating_rank` | Int64 |  |
| `def_rating_rank` | Int64 |  |
| `net_rating_rank` | Int64 |  |
| `ast_pct_rank` | Int64 |  |
| `ast_to_rank` | Int64 |  |
| `ast_ratio_rank` | Int64 |  |
| `oreb_pct_rank` | Int64 |  |
| `dreb_pct_rank` | Int64 |  |
| `reb_pct_rank` | Int64 |  |
| `tm_tov_pct_rank` | Int64 |  |
| `efg_pct_rank` | Int64 |  |
| `ts_pct_rank` | Int64 |  |
| `pace_rank` | Int64 |  |
| `pie_rank` | Int64 |  |
| `pts_off_tov` | Float64 |  |
| `pts_2_nd_chance` | Float64 |  |
| `pts_fb` | Float64 |  |
| `pts_paint` | Float64 |  |
| `opp_pts_off_tov` | Float64 |  |
| `opp_pts_2_nd_chance` | Float64 |  |
| `opp_pts_fb` | Float64 |  |
| `opp_pts_paint` | Float64 |  |
| `pts_off_tov_rank` | Int64 |  |
| `pts_2_nd_chance_rank` | Int64 |  |
| `pts_fb_rank` | Int64 |  |
| `pts_paint_rank` | Int64 |  |
| `opp_pts_off_tov_rank` | Int64 |  |
| `opp_pts_2_nd_chance_rank` | Int64 |  |
| `opp_pts_fb_rank` | Int64 |  |
| `opp_pts_paint_rank` | Int64 |  |
| `pct_fga_2_pt` | Float64 |  |
| `pct_fga_3_pt` | Float64 |  |
| `pct_pts_2_pt` | Float64 |  |
| `pct_pts_2_pt_mr` | Float64 |  |
| `pct_pts_3_pt` | Float64 |  |
| `pct_pts_fb` | Float64 |  |
| `pct_pts_ft` | Float64 |  |
| `pct_pts_off_tov` | Float64 |  |
| `pct_pts_paint` | Float64 |  |
| `pct_ast_2_pm` | Float64 |  |
| `pct_uast_2_pm` | Float64 |  |
| `pct_ast_3_pm` | Float64 |  |
| `pct_uast_3_pm` | Float64 |  |
| `pct_ast_fgm` | Float64 |  |
| `pct_uast_fgm` | Float64 |  |
| `pct_fga_2_pt_rank` | Int64 |  |
| `pct_fga_3_pt_rank` | Int64 |  |
| `pct_pts_2_pt_rank` | Int64 |  |
| `pct_pts_2_pt_mr_rank` | Int64 |  |
| `pct_pts_3_pt_rank` | Int64 |  |
| `pct_pts_fb_rank` | Int64 |  |
| `pct_pts_ft_rank` | Int64 |  |
| `pct_pts_off_tov_rank` | Int64 |  |
| `pct_pts_paint_rank` | Int64 |  |
| `pct_ast_2_pm_rank` | Int64 |  |
| `pct_uast_2_pm_rank` | Int64 |  |
| `pct_ast_3_pm_rank` | Int64 |  |
| `pct_uast_3_pm_rank` | Int64 |  |
| `pct_ast_fgm_rank` | Int64 |  |
| `pct_uast_fgm_rank` | Int64 |  |
| `opp_fgm` | Float64 |  |
| `opp_fga` | Float64 |  |
| `opp_fg_pct` | Float64 |  |
| `opp_fg3_m` | Float64 |  |
| `opp_fg3_a` | Float64 |  |
| `opp_fg3_pct` | Float64 |  |
| `opp_ftm` | Float64 |  |
| `opp_fta` | Float64 |  |
| `opp_ft_pct` | Float64 |  |
| `opp_oreb` | Float64 |  |
| `opp_dreb` | Float64 |  |
| `opp_reb` | Float64 |  |
| `opp_ast` | Float64 |  |
| `opp_tov` | Float64 |  |
| `opp_stl` | Float64 |  |
| `opp_blk` | Float64 |  |
| `opp_blka` | Float64 |  |
| `opp_pf` | Float64 |  |
| `opp_pfd` | Float64 |  |
| `opp_pts` | Float64 | Opponent points. |
| `opp_fgm_rank` | Int64 |  |
| `opp_fga_rank` | Int64 |  |
| `opp_fg_pct_rank` | Int64 |  |
| `opp_fg3_m_rank` | Int64 |  |
| `opp_fg3_a_rank` | Int64 |  |
| `opp_fg3_pct_rank` | Int64 |  |
| `opp_ftm_rank` | Int64 |  |
| `opp_fta_rank` | Int64 |  |
| `opp_ft_pct_rank` | Int64 |  |
| `opp_oreb_rank` | Int64 |  |
| `opp_dreb_rank` | Int64 |  |
| `opp_reb_rank` | Int64 |  |
| `opp_ast_rank` | Int64 |  |
| `opp_tov_rank` | Int64 |  |
| `opp_stl_rank` | Int64 |  |
| `opp_blk_rank` | Int64 |  |
| `opp_blka_rank` | Int64 |  |
| `opp_pf_rank` | Int64 |  |
| `opp_pfd_rank` | Int64 |  |
| `opp_pts_rank` | Int64 |  |

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_team_season_stats
df = load_wnba_stats_team_season_stats(seasons=2026)
print(df.shape)
```
