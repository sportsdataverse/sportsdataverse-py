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
| `season` | integer | Season identifier (4-digit year or 'YYYY-YY' string). |
| `league_id` | character | League identifier ('10' = WNBA). |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `per_mode` | character |  |

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
| `group_set` | character |  |
| `group_id` | character | ESPN group id. |
| `group_name` | character |  |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | double | Wins percentage (0-1 decimal). |
| `min` | double | Minutes played. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | double | Field goal percentage (0-1). |
| `fg3_m` | integer | Three-point field goals made. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | double | Three-point field goal percentage (0-1). |
| `ftm` | integer | Free throws made. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | double | Free throw percentage (0-1). |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `reb` | integer | Total rebounds. |
| `ast` | integer | Assists. |
| `tov` | double | Turnovers. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `blka` | integer |  |
| `pf` | integer | Personal fouls. |
| `pfd` | integer |  |
| `pts` | integer | Points scored. |
| `plus_minus` | double | Plus/minus point differential while on court. |
| `gp_rank` | integer |  |
| `w_rank` | integer |  |
| `l_rank` | integer |  |
| `w_pct_rank` | integer |  |
| `min_rank` | integer |  |
| `fgm_rank` | integer |  |
| `fga_rank` | integer |  |
| `fg_pct_rank` | integer |  |
| `fg3_m_rank` | integer |  |
| `fg3_a_rank` | integer |  |
| `fg3_pct_rank` | integer |  |
| `ftm_rank` | integer |  |
| `fta_rank` | integer |  |
| `ft_pct_rank` | integer |  |
| `oreb_rank` | integer |  |
| `dreb_rank` | integer |  |
| `reb_rank` | integer |  |
| `ast_rank` | integer |  |
| `tov_rank` | integer |  |
| `stl_rank` | integer |  |
| `blk_rank` | integer |  |
| `blka_rank` | integer |  |
| `pf_rank` | integer |  |
| `pfd_rank` | integer |  |
| `pts_rank` | integer |  |
| `plus_minus_rank` | integer |  |
| `sum_time_played` | integer |  |
| `season` | integer | Season identifier (4-digit year or 'YYYY-YY' string). |
| `league_id` | character | League identifier ('10' = WNBA). |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `per_mode` | character |  |
| `group_quantity` | integer |  |
| `measure_type` | character |  |
| `e_off_rating` | double |  |
| `off_rating` | double |  |
| `e_def_rating` | double |  |
| `def_rating` | double |  |
| `e_net_rating` | double |  |
| `net_rating` | double | Net rating (off rating - def rating). |
| `ast_pct` | double | Assist percentage. |
| `ast_to` | double |  |
| `ast_ratio` | double |  |
| `oreb_pct` | double |  |
| `dreb_pct` | double |  |
| `reb_pct` | double |  |
| `tm_tov_pct` | double |  |
| `efg_pct` | double |  |
| `ts_pct` | double | True shooting percentage (0-1). |
| `e_pace` | double |  |
| `pace` | double | Possessions per 48 minutes. |
| `pace_per40` | double | Pace per40. |
| `poss` | integer | Poss. |
| `pie` | double | Player Impact Estimate (0-1). |
| `off_rating_rank` | integer |  |
| `def_rating_rank` | integer |  |
| `net_rating_rank` | integer |  |
| `ast_pct_rank` | integer |  |
| `ast_to_rank` | integer |  |
| `ast_ratio_rank` | integer |  |
| `oreb_pct_rank` | integer |  |
| `dreb_pct_rank` | integer |  |
| `reb_pct_rank` | integer |  |
| `tm_tov_pct_rank` | integer |  |
| `efg_pct_rank` | integer |  |
| `ts_pct_rank` | integer |  |
| `pace_rank` | integer |  |
| `pie_rank` | integer |  |

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
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | double | Field goal percentage (0-1). |
| `fg3_m` | integer | Three-point field goals made. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | double | Three-point field goal percentage (0-1). |
| `ftm` | integer | Free throws made. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | double | Free throw percentage (0-1). |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `reb` | integer | Total rebounds. |
| `ast` | integer | Assists. |
| `tov` | integer | Turnovers. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `blka` | integer |  |
| `pf` | integer | Personal fouls. |
| `pfd` | integer |  |
| `pts` | integer | Points scored. |
| `plus_minus` | integer | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | double |  |
| `dd2` | integer |  |
| `td3` | integer |  |
| `wnba_fantasy_pts` | double |  |
| `gp_rank` | integer |  |
| `w_rank` | integer |  |
| `l_rank` | integer |  |
| `w_pct_rank` | integer |  |
| `min_rank` | integer |  |
| `fgm_rank` | integer |  |
| `fga_rank` | integer |  |
| `fg_pct_rank` | integer |  |
| `fg3_m_rank` | integer |  |
| `fg3_a_rank` | integer |  |
| `fg3_pct_rank` | integer |  |
| `ftm_rank` | integer |  |
| `fta_rank` | integer |  |
| `ft_pct_rank` | integer |  |
| `oreb_rank` | integer |  |
| `dreb_rank` | integer |  |
| `reb_rank` | integer |  |
| `ast_rank` | integer |  |
| `tov_rank` | integer |  |
| `stl_rank` | integer |  |
| `blk_rank` | integer |  |
| `blka_rank` | integer |  |
| `pf_rank` | integer |  |
| `pfd_rank` | integer |  |
| `pts_rank` | integer |  |
| `plus_minus_rank` | integer |  |
| `nba_fantasy_pts_rank` | integer |  |
| `dd2_rank` | integer |  |
| `td3_rank` | integer |  |
| `wnba_fantasy_pts_rank` | integer |  |
| `team_count` | integer |  |
| `season` | integer | Season identifier (4-digit year or 'YYYY-YY' string). |
| `league_id` | character | League identifier ('10' = WNBA). |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `per_mode` | character |  |
| `measure_type` | character |  |
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
| `fgm_pg` | double |  |
| `fga_pg` | double |  |
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
| `fgm_pg_rank` | integer |  |
| `fga_pg_rank` | integer |  |
| `pts_off_tov` | integer |  |
| `pts_2_nd_chance` | integer |  |
| `pts_fb` | integer |  |
| `pts_paint` | integer |  |
| `opp_pts_off_tov` | double |  |
| `opp_pts_2_nd_chance` | double |  |
| `opp_pts_fb` | double |  |
| `opp_pts_paint` | double |  |
| `pts_off_tov_rank` | integer |  |
| `pts_2_nd_chance_rank` | integer |  |
| `pts_fb_rank` | integer |  |
| `pts_paint_rank` | integer |  |
| `opp_pts_off_tov_rank` | integer |  |
| `opp_pts_2_nd_chance_rank` | integer |  |
| `opp_pts_fb_rank` | integer |  |
| `opp_pts_paint_rank` | integer |  |
| `pct_fga_2_pt` | double |  |
| `pct_fga_3_pt` | double |  |
| `pct_pts_2_pt` | double |  |
| `pct_pts_2_pt_mr` | double |  |
| `pct_pts_3_pt` | double |  |
| `pct_pts_fb` | double |  |
| `pct_pts_ft` | double |  |
| `pct_pts_off_tov` | double |  |
| `pct_pts_paint` | double |  |
| `pct_ast_2_pm` | double |  |
| `pct_uast_2_pm` | double |  |
| `pct_ast_3_pm` | double |  |
| `pct_uast_3_pm` | double |  |
| `pct_ast_fgm` | double |  |
| `pct_uast_fgm` | double |  |
| `pct_fga_2_pt_rank` | integer |  |
| `pct_fga_3_pt_rank` | integer |  |
| `pct_pts_2_pt_rank` | integer |  |
| `pct_pts_2_pt_mr_rank` | integer |  |
| `pct_pts_3_pt_rank` | integer |  |
| `pct_pts_fb_rank` | integer |  |
| `pct_pts_ft_rank` | integer |  |
| `pct_pts_off_tov_rank` | integer |  |
| `pct_pts_paint_rank` | integer |  |
| `pct_ast_2_pm_rank` | integer |  |
| `pct_uast_2_pm_rank` | integer |  |
| `pct_ast_3_pm_rank` | integer |  |
| `pct_uast_3_pm_rank` | integer |  |
| `pct_ast_fgm_rank` | integer |  |
| `pct_uast_fgm_rank` | integer |  |
| `pct_fgm` | double |  |
| `pct_fga` | double |  |
| `pct_fg3_m` | double |  |
| `pct_fg3_a` | double |  |
| `pct_ftm` | double |  |
| `pct_fta` | double |  |
| `pct_oreb` | double |  |
| `pct_dreb` | double |  |
| `pct_reb` | double |  |
| `pct_ast` | double |  |
| `pct_tov` | double |  |
| `pct_stl` | double |  |
| `pct_blk` | double |  |
| `pct_blka` | double |  |
| `pct_pf` | double |  |
| `pct_pfd` | double |  |
| `pct_pts` | double |  |
| `pct_fgm_rank` | integer |  |
| `pct_fga_rank` | integer |  |
| `pct_fg3_m_rank` | integer |  |
| `pct_fg3_a_rank` | integer |  |
| `pct_ftm_rank` | integer |  |
| `pct_fta_rank` | integer |  |
| `pct_oreb_rank` | integer |  |
| `pct_dreb_rank` | integer |  |
| `pct_reb_rank` | integer |  |
| `pct_ast_rank` | integer |  |
| `pct_tov_rank` | integer |  |
| `pct_stl_rank` | integer |  |
| `pct_blk_rank` | integer |  |
| `pct_blka_rank` | integer |  |
| `pct_pf_rank` | integer |  |
| `pct_pfd_rank` | integer |  |
| `pct_pts_rank` | integer |  |
| `def_ws` | double |  |
| `def_ws_raw` | double |  |
| `def_ws_rank` | integer |  |

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
| `league_id` | character | League identifier ('10' = WNBA). |
| `season_id` | character | Unique season identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `conference` | character | Filter players or teams by conference. |
| `conference_record` | character | Conference win-loss record. |
| `playoff_rank` | integer | League/season rank for playoff. |
| `clinch_indicator` | character | Playoff clinch indicator (e.g. 'x' clinched playoff, 'e' eliminated). |
| `division` | character | Team division. |
| `division_record` | character |  |
| `division_rank` | integer |  |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `win_pct` | double | Win percentage (0-1 decimal). |
| `league_rank` | integer |  |
| `record` | character | Record string (e.g. '12-4'). |
| `home` | character | Home. |
| `road` | character | Road. |
| `l10` | character | L10. |
| `last10_home` | character |  |
| `last10_road` | character |  |
| `ot` | character | Ot. |
| `three_pts_or_less` | character |  |
| `ten_pts_or_more` | character |  |
| `long_home_streak` | integer |  |
| `str_long_home_streak` | character |  |
| `long_road_streak` | integer |  |
| `str_long_road_streak` | character |  |
| `long_win_streak` | integer |  |
| `long_loss_streak` | integer |  |
| `current_home_streak` | integer |  |
| `str_current_home_streak` | character |  |
| `current_road_streak` | integer |  |
| `str_current_road_streak` | character |  |
| `current_streak` | integer |  |
| `str_current_streak` | character |  |
| `conference_games_back` | double |  |
| `division_games_back` | double |  |
| `clinched_conference_title` | integer |  |
| `clinched_division_title` | integer |  |
| `clinched_playoff_birth` | integer |  |
| `clinched_play_in` | integer |  |
| `eliminated_conference` | integer |  |
| `eliminated_division` | integer |  |
| `ahead_at_half` | character |  |
| `behind_at_half` | character |  |
| `tied_at_half` | character |  |
| `ahead_at_third` | character |  |
| `behind_at_third` | character |  |
| `tied_at_third` | character |  |
| `score100_pts` | character |  |
| `opp_score100_pts` | character |  |
| `opp_over500` | character |  |
| `lead_in_fgpct` | character |  |
| `lead_in_reb` | character |  |
| `fewer_turnovers` | character |  |
| `points_pg` | double | Points pg. |
| `opp_points_pg` | double | Opponent points pg. |
| `diff_points_pg` | double | Diff points pg. |
| `vs_east` | character |  |
| `vs_atlantic` | character |  |
| `vs_central` | character |  |
| `vs_southeast` | character |  |
| `vs_west` | character |  |
| `vs_northwest` | character |  |
| `vs_pacific` | character |  |
| `vs_southwest` | character |  |
| `jan` | character |  |
| `feb` | character |  |
| `mar` | character |  |
| `apr` | character |  |
| `may` | character |  |
| `jun` | character |  |
| `jul` | character |  |
| `aug` | character |  |
| `sep` | character |  |
| `oct` | character |  |
| `nov` | character |  |
| `dec` | character |  |
| `score_80_plus` | character |  |
| `opp_score_80_plus` | character |  |
| `score_below_80` | character |  |
| `opp_score_below_80` | character |  |
| `total_points` | integer |  |
| `opp_total_points` | integer |  |
| `diff_total_points` | integer |  |
| `league_games_back` | double |  |
| `playoff_seeding` | character |  |
| `clinched_post_season` | integer |  |
| `neutral` | character | Neutral. |
| `season` | integer | Season identifier (4-digit year or 'YYYY-YY' string). |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | double | Wins percentage (0-1 decimal). |
| `min` | double | Minutes played. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | double | Field goal percentage (0-1). |
| `fg3_m` | integer | Three-point field goals made. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | double | Three-point field goal percentage (0-1). |
| `ftm` | integer | Free throws made. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | double | Free throw percentage (0-1). |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `reb` | integer | Total rebounds. |
| `ast` | integer | Assists. |
| `tov` | double | Turnovers. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `blka` | integer |  |
| `pf` | integer | Personal fouls. |
| `pfd` | integer |  |
| `pts` | integer | Points scored. |
| `plus_minus` | double | Plus/minus point differential while on court. |
| `gp_rank` | integer |  |
| `w_rank` | integer |  |
| `l_rank` | integer |  |
| `w_pct_rank` | integer |  |
| `min_rank` | integer |  |
| `fgm_rank` | integer |  |
| `fga_rank` | integer |  |
| `fg_pct_rank` | integer |  |
| `fg3_m_rank` | integer |  |
| `fg3_a_rank` | integer |  |
| `fg3_pct_rank` | integer |  |
| `ftm_rank` | integer |  |
| `fta_rank` | integer |  |
| `ft_pct_rank` | integer |  |
| `oreb_rank` | integer |  |
| `dreb_rank` | integer |  |
| `reb_rank` | integer |  |
| `ast_rank` | integer |  |
| `tov_rank` | integer |  |
| `stl_rank` | integer |  |
| `blk_rank` | integer |  |
| `blka_rank` | integer |  |
| `pf_rank` | integer |  |
| `pfd_rank` | integer |  |
| `pts_rank` | integer |  |
| `plus_minus_rank` | integer |  |
| `season` | integer | Season identifier (4-digit year or 'YYYY-YY' string). |
| `league_id` | character | League identifier ('10' = WNBA). |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `per_mode` | character |  |
| `measure_type` | character |  |
| `e_off_rating` | double |  |
| `off_rating` | double |  |
| `e_def_rating` | double |  |
| `def_rating` | double |  |
| `e_net_rating` | double |  |
| `net_rating` | double | Net rating (off rating - def rating). |
| `ast_pct` | double | Assist percentage. |
| `ast_to` | double |  |
| `ast_ratio` | double |  |
| `oreb_pct` | double |  |
| `dreb_pct` | double |  |
| `reb_pct` | double |  |
| `tm_tov_pct` | double |  |
| `efg_pct` | double |  |
| `ts_pct` | double | True shooting percentage (0-1). |
| `e_pace` | double |  |
| `pace` | double | Possessions per 48 minutes. |
| `pace_per40` | double | Pace per40. |
| `poss` | integer | Poss. |
| `pie` | double | Player Impact Estimate (0-1). |
| `off_rating_rank` | integer |  |
| `def_rating_rank` | integer |  |
| `net_rating_rank` | integer |  |
| `ast_pct_rank` | integer |  |
| `ast_to_rank` | integer |  |
| `ast_ratio_rank` | integer |  |
| `oreb_pct_rank` | integer |  |
| `dreb_pct_rank` | integer |  |
| `reb_pct_rank` | integer |  |
| `tm_tov_pct_rank` | integer |  |
| `efg_pct_rank` | integer |  |
| `ts_pct_rank` | integer |  |
| `pace_rank` | integer |  |
| `pie_rank` | integer |  |
| `pts_off_tov` | double |  |
| `pts_2_nd_chance` | double |  |
| `pts_fb` | double |  |
| `pts_paint` | double |  |
| `opp_pts_off_tov` | double |  |
| `opp_pts_2_nd_chance` | double |  |
| `opp_pts_fb` | double |  |
| `opp_pts_paint` | double |  |
| `pts_off_tov_rank` | integer |  |
| `pts_2_nd_chance_rank` | integer |  |
| `pts_fb_rank` | integer |  |
| `pts_paint_rank` | integer |  |
| `opp_pts_off_tov_rank` | integer |  |
| `opp_pts_2_nd_chance_rank` | integer |  |
| `opp_pts_fb_rank` | integer |  |
| `opp_pts_paint_rank` | integer |  |
| `pct_fga_2_pt` | double |  |
| `pct_fga_3_pt` | double |  |
| `pct_pts_2_pt` | double |  |
| `pct_pts_2_pt_mr` | double |  |
| `pct_pts_3_pt` | double |  |
| `pct_pts_fb` | double |  |
| `pct_pts_ft` | double |  |
| `pct_pts_off_tov` | double |  |
| `pct_pts_paint` | double |  |
| `pct_ast_2_pm` | double |  |
| `pct_uast_2_pm` | double |  |
| `pct_ast_3_pm` | double |  |
| `pct_uast_3_pm` | double |  |
| `pct_ast_fgm` | double |  |
| `pct_uast_fgm` | double |  |
| `pct_fga_2_pt_rank` | integer |  |
| `pct_fga_3_pt_rank` | integer |  |
| `pct_pts_2_pt_rank` | integer |  |
| `pct_pts_2_pt_mr_rank` | integer |  |
| `pct_pts_3_pt_rank` | integer |  |
| `pct_pts_fb_rank` | integer |  |
| `pct_pts_ft_rank` | integer |  |
| `pct_pts_off_tov_rank` | integer |  |
| `pct_pts_paint_rank` | integer |  |
| `pct_ast_2_pm_rank` | integer |  |
| `pct_uast_2_pm_rank` | integer |  |
| `pct_ast_3_pm_rank` | integer |  |
| `pct_uast_3_pm_rank` | integer |  |
| `pct_ast_fgm_rank` | integer |  |
| `pct_uast_fgm_rank` | integer |  |
| `opp_fgm` | double |  |
| `opp_fga` | double |  |
| `opp_fg_pct` | double |  |
| `opp_fg3_m` | double |  |
| `opp_fg3_a` | double |  |
| `opp_fg3_pct` | double |  |
| `opp_ftm` | double |  |
| `opp_fta` | double |  |
| `opp_ft_pct` | double |  |
| `opp_oreb` | double |  |
| `opp_dreb` | double |  |
| `opp_reb` | double |  |
| `opp_ast` | double |  |
| `opp_tov` | double |  |
| `opp_stl` | double |  |
| `opp_blk` | double |  |
| `opp_blka` | double |  |
| `opp_pf` | double |  |
| `opp_pfd` | double |  |
| `opp_pts` | double | Opponent points. |
| `opp_fgm_rank` | integer |  |
| `opp_fga_rank` | integer |  |
| `opp_fg_pct_rank` | integer |  |
| `opp_fg3_m_rank` | integer |  |
| `opp_fg3_a_rank` | integer |  |
| `opp_fg3_pct_rank` | integer |  |
| `opp_ftm_rank` | integer |  |
| `opp_fta_rank` | integer |  |
| `opp_ft_pct_rank` | integer |  |
| `opp_oreb_rank` | integer |  |
| `opp_dreb_rank` | integer |  |
| `opp_reb_rank` | integer |  |
| `opp_ast_rank` | integer |  |
| `opp_tov_rank` | integer |  |
| `opp_stl_rank` | integer |  |
| `opp_blk_rank` | integer |  |
| `opp_blka_rank` | integer |  |
| `opp_pf_rank` | integer |  |
| `opp_pfd_rank` | integer |  |
| `opp_pts_rank` | integer |  |

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_team_season_stats
df = load_wnba_stats_team_season_stats(seasons=2026)
print(df.shape)
```
