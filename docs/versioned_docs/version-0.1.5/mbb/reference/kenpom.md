---
title: MBB — KenPom (kenpom.com, subscription)
sidebar_label: KenPom (kenpom.com, subscription)
description: "MBB — KenPom (kenpom.com, subscription) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 11
toc_max_heading_level: 2
---
# MBB — KenPom (kenpom.com, subscription)

`sportsdataverse.mbb` — 30 endpoints.

## kenpom_ratings

GET /index.php - Pomeroy season ratings (AdjEM/AdjO/AdjD/AdjT plus SOS, one row per team). Port of hoopR kp_pomeroy_ratings().

**Endpoint URL:** `GET https://kenpom.com/index.php`

**Valid URL:** [https://kenpom.com/index.php?y=2025](https://kenpom.com/index.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year (2025 = the 2024-25 season). Data begins at 2002. |

### Returns {#kenpom_ratings-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `rk` | character | Rk. |
| `team` | character | Team-side label or team identifier. |
| `conf` | character | character. |
| `w_l` | character | W l. |
| `net_rtg` | character |  |
| `o_rtg` | character | O rtg. |
| `o_rtg_rk` | character | O rtg rk. |
| `d_rtg` | character |  |
| `d_rtg_rk` | character |  |
| `adj_t` | character | Adj t. |
| `adj_t_rk` | character | Adj t rk. |
| `luck` | character | Luck. |
| `luck_rk` | character | Luck rk. |
| `strength_of_schedule_net_rtg` | character |  |
| `strength_of_schedule_net_rtg_rk` | character |  |
| `strength_of_schedule_o_rtg` | character |  |
| `strength_of_schedule_o_rtg_rk` | character |  |
| `strength_of_schedule_d_rtg` | character |  |
| `strength_of_schedule_d_rtg_rk` | character |  |
| `ncsos_net_rtg` | character |  |
| `ncsos_net_rtg_rk` | character |  |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_ratings-example}

```python
kenpom_ratings(year=2025)
```

_Last validated n/a._

## kenpom_efficiency

GET /summary.php - efficiency and tempo summary (adjusted and raw O/D/T, average possession length). Port of hoopR kp_efficiency().

**Endpoint URL:** `GET https://kenpom.com/summary.php`

**Valid URL:** [https://kenpom.com/summary.php?y=2025](https://kenpom.com/summary.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. Columns are narrower before 2010. |

### Returns {#kenpom_efficiency-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `conf` | character | character. |
| `off_rating_adjusted` | double |  |
| `off_rating_adjusted_rk` | double |  |
| `off_rating_adjusted_rk_rk` | integer |  |
| `off_rating_raw` | double |  |
| `off_rating_raw_rk` | integer |  |
| `def_rating_adjusted` | double |  |
| `def_rating_adjusted_rk` | double |  |
| `def_rating_adjusted_rk_rk` | integer |  |
| `def_rating_raw` | double |  |
| `def_rating_raw_rk` | integer |  |
| `tempo_adjusted` | double |  |
| `tempo_adjusted_rk` | double |  |
| `tempo_raw` | integer |  |
| `tempo_raw_rk` | double |  |
| `tempo_off` | integer |  |
| `avg_poss_length_off` | double | Avg poss length off. |
| `avg_poss_length_def` | integer | Avg poss length def. |
| `avg_poss_length_def_rk` | double | Avg poss length def rk. |
| `avg_poss_length_def_rk_rk` | integer |  |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_efficiency-example}

```python
kenpom_efficiency(year=2025)
```

_Last validated n/a._

## kenpom_four_factors

GET /stats.php - four-factors rankings (eFG%, TO%, OR%, FTRate on offense and defense). Port of hoopR kp_fourfactors().

**Endpoint URL:** `GET https://kenpom.com/stats.php`

**Valid URL:** [https://kenpom.com/stats.php?y=2025](https://kenpom.com/stats.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns {#kenpom_four_factors-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `conf` | character | character. |
| `adj_tempo` | double |  |
| `adj_tempo_rk` | integer |  |
| `offense_adj_oe` | double |  |
| `offense_adj_oe_rk` | integer |  |
| `offense_e_fg_pct` | double |  |
| `offense_e_fg_pct_rk` | integer |  |
| `offense_to_pct` | double |  |
| `offense_to_pct_rk` | integer |  |
| `offense_or_pct` | double |  |
| `offense_or_pct_rk` | integer |  |
| `offense_ft_rate` | double |  |
| `offense_ft_rate_rk` | integer |  |
| `defense_adj_de` | double |  |
| `defense_adj_de_rk` | integer |  |
| `defense_e_fg_pct` | double |  |
| `defense_e_fg_pct_rk` | integer |  |
| `defense_to_pct` | double |  |
| `defense_to_pct_rk` | integer |  |
| `defense_or_pct` | double |  |
| `defense_or_pct_rk` | integer |  |
| `defense_ft_rate` | double |  |
| `defense_ft_rate_rk` | integer |  |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_four_factors-example}

```python
kenpom_four_factors(year=2025)
```

_Last validated n/a._

## kenpom_point_distribution

GET /pointdist.php - share of points scored from 2s, 3s and free throws, offense and defense. Port of hoopR kp_pointdist().

**Endpoint URL:** `GET https://kenpom.com/pointdist.php`

**Valid URL:** [https://kenpom.com/pointdist.php?y=2025](https://kenpom.com/pointdist.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns {#kenpom_point_distribution-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `conf` | character | character. |
| `offense_ft` | double |  |
| `offense_ft_rk` | integer |  |
| `offense_2_pt_fg` | double |  |
| `offense_2_pt_fg_rk` | integer |  |
| `offense_3_pt_fg` | double |  |
| `offense_3_pt_fg_rk` | integer |  |
| `defense_ft` | double |  |
| `defense_ft_rk` | integer |  |
| `defense_2_pt_fg` | double |  |
| `defense_2_pt_fg_rk` | integer |  |
| `defense_3_pt_fg` | double |  |
| `defense_3_pt_fg_rk` | integer |  |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_point_distribution-example}

```python
kenpom_point_distribution(year=2025)
```

_Last validated n/a._

## kenpom_height

GET /height.php - team height, effective height, experience, bench minutes and continuity. Port of hoopR kp_height().

**Endpoint URL:** `GET https://kenpom.com/height.php`

**Valid URL:** [https://kenpom.com/height.php?y=2025](https://kenpom.com/height.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. Columns are narrower before 2008. |

### Returns {#kenpom_height-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `conf` | character | character. |
| `avg_hgt` | character | Avg hgt. |
| `avg_hgt_rk` | character | Avg hgt rk. |
| `eff_hgt` | character | Eff hgt. |
| `eff_hgt_rk` | character | Eff hgt rk. |
| `c_hgt` | character | C hgt. |
| `c_hgt_rk` | character | C hgt rk. |
| `pf_hgt` | character | Pf hgt. |
| `pf_hgt_rk` | character | Pf hgt rk. |
| `sf_hgt` | character | Sf hgt. |
| `sf_hgt_rk` | character | Sf hgt rk. |
| `sg_hgt` | character | Sg hgt. |
| `sg_hgt_rk` | character | Sg hgt rk. |
| `pg_hgt` | character | Pg hgt. |
| `pg_hgt_rk` | character | Pg hgt rk. |
| `experience` | character | Years of professional experience. |
| `experience_rk` | character | Experience rk. |
| `bench` | character | Bench. |
| `bench_rk` | character | Bench rk. |
| `continuity` | character | Continuity. |
| `continuity_rk` | character | Continuity rk. |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_height-example}

```python
kenpom_height(year=2025)
```

_Last validated n/a._

## kenpom_foul_trouble

GET /foul_trouble.php - team foul-trouble splits (minutes and efficiency with starters in foul trouble). Port of hoopR kp_foul_trouble().

**Endpoint URL:** `GET https://kenpom.com/foul_trouble.php`

**Valid URL:** [https://kenpom.com/foul_trouble.php?y=2025](https://kenpom.com/foul_trouble.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns {#kenpom_foul_trouble-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `conf` | character | character. |
| `2_fp_pct` | double |  |
| `2_fp_pct_rk` | integer |  |
| `adj2_fp` | double |  |
| `adj2_fp_rk` | integer |  |
| `2_foul_total_time` | character |  |
| `2_foul_total_time_rk` | integer |  |
| `2_foul_time_on` | character |  |
| `2_foul_time_on_rk` | integer |  |
| `bench_pct` | double | Bench percentage (0-1 decimal). |
| `bench_pct_rk` | integer | Bench pct rk. |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_foul_trouble-example}

```python
kenpom_foul_trouble(year=2025)
```

_Last validated n/a._

## kenpom_team_stats

GET /teamstats.php - team shooting and style splits; side='o' for offense, 'd' for defense. Port of hoopR kp_teamstats().

**Endpoint URL:** `GET https://kenpom.com/teamstats.php`

**Valid URL:** [https://kenpom.com/teamstats.php?y=2025&od=o](https://kenpom.com/teamstats.php?y=2025&od=o)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |
| `od` | `side` |  |  | `Y` | Side of the ball: 'o' (offense, hoopR's default) or 'd' (defense). |

### Returns {#kenpom_team_stats-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `conf` | character | character. |
| `3_p_pct` | character |  |
| `3_p_pct_rk` | character |  |
| `2_p_pct` | character |  |
| `2_p_pct_rk` | character |  |
| `ft_pct` | character | Free throw percentage (0-1). |
| `ft_pct_rk` | character | Ft pct rk. |
| `blk_pct` | character | Blocks percentage (0-1 decimal). |
| `blk_pct_rk` | character | Blk pct rk. |
| `stl_pct` | character | Steals percentage (0-1 decimal). |
| `stl_pct_rk` | character | Stl pct rk. |
| `nst_pct` | character |  |
| `nst_pct_rk` | character |  |
| `2_p_dist` | character |  |
| `2_p_dist_rk` | character |  |
| `a_pct` | character | A percentage (0-1 decimal). |
| `a_pct_rk` | character | A pct rk. |
| `3_pa_pct` | character |  |
| `3_pa_pct_rk` | character |  |
| `adj_oe` | character | Adjusted offensive efficiency. |
| `adj_oe_rk` | character | Adj oe rk. |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_team_stats-example}

```python
kenpom_team_stats(year=2025, side='o')
```

_Last validated n/a._

## kenpom_player_stats

GET /playerstats.php - national player leaderboard for one metric. Port of hoopR kp_playerstats().

**Endpoint URL:** `GET https://kenpom.com/playerstats.php`

**Valid URL:** [https://kenpom.com/playerstats.php?y=2025&s=eFG](https://kenpom.com/playerstats.php?y=2025&s=eFG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. Data begins at 2004. |
| `s` | `metric` |  | `Y` |  | Metric slug as KenPom spells it on the wire - one of ORtg, PctMin, eFG, PctPoss, PctShots, ORPct, DRPct, TORate, ARate, PctBlocks, FTRate, PctStls, TS, FCper40, FDper40, FG2Pct, FG3Pct, FTPct. (hoopR's kp_playerstats() takes the display labels - ORtg, Min, eFG, Poss, Shots, OR, DR, TO, ARate, Blk, FTRate, Stl, TS, FC40, FD40, 2P, 3P, FT - and maps them to these.) |
| `f` | `conf` |  |  | `Y` | Conference filter (KenPom abbreviation, e.g. 'ACC', 'B10'); omit for all of Division I. |
| `c` | `conf_only` |  |  | `Y` | Conference-games-only toggle: 'c' restricts the leaderboard to conference play. |

### Returns {#kenpom_player_stats-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `rk` | integer | Rk. |
| `player` | character | Player name. |
| `team` | character | Team-side label or team identifier. |
| `e_fg_pct` | double | E field goals percentage (0-1 decimal). |
| `ht` | character | Listed height. |
| `wt` | integer | Listed weight (lbs). |
| `yr` | character | Yr. |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_player_stats-example}

```python
kenpom_player_stats(year=2025, metric='eFG')
```

_Last validated n/a._

## kenpom_kpoy

GET /kpoy.php - KenPom Player of the Year standings and the game-MVP table. Port of hoopR kp_kpoy().

**Endpoint URL:** `GET https://kenpom.com/kpoy.php`

**Valid URL:** [https://kenpom.com/kpoy.php?y=2025](https://kenpom.com/kpoy.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns {#kenpom_kpoy-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**kpoy_table**

| col_name | type | description |
|---|---|---|
| `column_0` | integer |  |
| `player` | character | Player name. |
| `k_poy_rating` | double |  |

**kpoy_table_2**

| col_name | type | description |
|---|---|---|
| `column_0` | double |  |
| `player` | character | Player name. |
| `game_mvp_s` | integer |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_kpoy-example}

```python
kenpom_kpoy(year=2025)
```

_Last validated n/a._

## kenpom_team

GET /team.php - a team's full season page. Returns EVERY table on it, so one call covers hoopR's kp_team_schedule(), kp_team_players() and kp_team_lineups(), which each fetch this same page separately -- plus kp_team_depth_chart(), recovered under a "depth_chart" key from an embedded script tag rather than a table (KenPom dropped the static depth-chart table; see kp_team_depth_chart()'s R source).

**Endpoint URL:** `GET https://kenpom.com/team.php`

**Valid URL:** [https://kenpom.com/team.php?team=Duke&y=2025](https://kenpom.com/team.php?team=Duke&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | KenPom team name, spelled as the site does (e.g. 'Duke', 'Michigan St.'). |
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. Lineup tables begin at 2011. |

### Returns {#kenpom_team-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**report_table**

| col_name | type | description |
|---|---|---|
| `category` | character | Category label. |
| `offense` | character |  |
| `defense` | character |  |
| `d_i_avg` | double |  |

**schedule_table**

| col_name | type | description |
|---|---|---|
| `0` | character |  |
| `1` | integer |  |
| `2` | integer |  |
| `3` | character |  |
| `4` | character |  |
| `5` | integer |  |
| `6` | character |  |
| `7` | character |  |
| `8` | character |  |
| `9` | character |  |
| `10` | character |  |

**player_table**

| col_name | type | description |
|---|---|---|
| `column_0` | double |  |
| `column_0_rk` | character |  |
| `ht` | character | Listed height. |
| `wt` | double | Listed weight (lbs). |
| `yr` | character | Yr. |
| `g` | double | Games played. |
| `s` | double | S. |
| `pct_min` | double |  |
| `o_rtg` | double | O rtg. |
| `pct_poss` | double |  |
| `pct_shots` | double |  |
| `e_fg_pct` | double | E field goals percentage (0-1 decimal). |
| `ts_pct` | double | True shooting percentage (0-1). |
| `or_pct` | double | Or percentage (0-1 decimal). |
| `dr_pct` | double | Dr percentage (0-1 decimal). |
| `a_rate` | double | A rate. |
| `to_rate` | double | To rate. |
| `blk_pct` | double | Blocks percentage (0-1 decimal). |
| `stl_pct` | double | Steals percentage (0-1 decimal). |
| `fc_40` | double |  |
| `fd_40` | double |  |
| `ft_rate` | double | Ft rate. |
| `ftm_a` | character |  |
| `pct` | double | Win percentage. |
| `2_pm_a` | character |  |
| `pct_1` | double |  |
| `3_pm_a` | character |  |
| `pct_2` | double |  |

**depth_chart**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `pct_pg` | double |  |
| `pct_sg` | double |  |
| `pct_sf` | double |  |
| `pct_pf` | double |  |
| `pct_c` | double |  |
| `name` | character | Display name. |
| `pct_poss` | double |  |
| `fta` | integer | Free throw attempts. |
| `fg2_a` | integer |  |
| `fg3_a` | integer | Three-point field goal attempts. |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | integer | Player weight in pounds. |
| `year` | character | 4-digit year. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_team-example}

```python
kenpom_team(team='Duke', year=2025)
```

_Last validated n/a._

## kenpom_team_players_expanded

GET /player-expanded.php - a team's expanded per-player table plus the minutes matrix. Covers hoopR's kp_team_player_stats() and kp_minutes_matrix() in one fetch.

**Endpoint URL:** `GET https://kenpom.com/player-expanded.php`

**Valid URL:** [https://kenpom.com/player-expanded.php?team=Duke&y=2025](https://kenpom.com/player-expanded.php?team=Duke&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | KenPom team name, spelled as the site does. |
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. Starts ('S') are available from 2014. |

### Returns {#kenpom_team_players_expanded-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**player_table**

| col_name | type | description |
|---|---|---|
| `column_0` | double |  |
| `column_0_rk` | character |  |
| `ht` | character | Listed height. |
| `wt` | double | Listed weight (lbs). |
| `yr` | character | Yr. |
| `g` | double | Games played. |
| `pct_min` | double |  |
| `o_rtg` | character | O rtg. |
| `pct_poss` | character |  |
| `pct_shots` | character |  |
| `e_fg_pct` | character | E field goals percentage (0-1 decimal). |
| `ts_pct` | character | True shooting percentage (0-1). |
| `or_pct` | double | Or percentage (0-1 decimal). |
| `dr_pct` | character | Dr percentage (0-1 decimal). |
| `a_rate` | character | A rate. |
| `to_rate` | double | To rate. |
| `blk_pct` | character | Blocks percentage (0-1 decimal). |
| `stl_pct` | character | Steals percentage (0-1 decimal). |
| `fc_40` | double |  |
| `fd_40` | character |  |
| `ft_rate` | character | Ft rate. |
| `ftm_a` | character |  |
| `pct` | character | Win percentage. |
| `2_pm_a` | character |  |
| `pct_1` | double |  |
| `3_pm_a` | character |  |
| `pct_2` | character |  |

**player_table_2**

| col_name | type | description |
|---|---|---|
| `column_0` | double |  |
| `column_0_rk` | character |  |
| `ht` | character | Listed height. |
| `wt` | double | Listed weight (lbs). |
| `yr` | character | Yr. |
| `g` | double | Games played. |
| `pct_min` | character |  |
| `o_rtg` | character | O rtg. |
| `pct_poss` | character |  |
| `pct_shots` | character |  |
| `e_fg_pct` | character | E field goals percentage (0-1 decimal). |
| `ts_pct` | character | True shooting percentage (0-1). |
| `or_pct` | double | Or percentage (0-1 decimal). |
| `dr_pct` | character | Dr percentage (0-1 decimal). |
| `a_rate` | character | A rate. |
| `to_rate` | double | To rate. |
| `blk_pct` | character | Blocks percentage (0-1 decimal). |
| `stl_pct` | character | Steals percentage (0-1 decimal). |
| `fc_40` | double |  |
| `fd_40` | character |  |
| `ft_rate` | character | Ft rate. |
| `ftm_a` | character |  |
| `pct` | character | Win percentage. |
| `2_pm_a` | character |  |
| `pct_1` | character |  |
| `3_pm_a` | character |  |
| `pct_2` | character |  |

**minutes_table**

| col_name | type | description |
|---|---|---|
| `column_0` | character |  |
| `column_0_rk` | integer |  |
| `column_0_rk_rk` | character |  |
| `column_0_rk_rk_rk` | character |  |
| `caleb_foster` | integer |  |
| `darren_harris` | double |  |
| `isaiah_evans` | double |  |
| `mason_gillis` | integer |  |
| `sion_james` | integer |  |
| `tyrese_proctor` | integer |  |
| `kon_knueppel` | integer |  |
| `cooper_flagg` | integer |  |
| `maliq_brown` | integer |  |
| `patrick_ngongba` | double |  |
| `khaman_maluach` | integer |  |
| `starting_lineup_number` | integer |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_team_players_expanded-example}

```python
kenpom_team_players_expanded(team='Duke', year=2025)
```

_Last validated n/a._

## kenpom_game_plan

GET /gameplan.php - a team's game-plan page (per-game four factors and personnel splits). Port of hoopR kp_gameplan().

**Endpoint URL:** `GET https://kenpom.com/gameplan.php`

**Valid URL:** [https://kenpom.com/gameplan.php?team=Duke&y=2025](https://kenpom.com/gameplan.php?team=Duke&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | KenPom team name, spelled as the site does. |
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns {#kenpom_game_plan-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**schedule_table**

| col_name | type | description |
|---|---|---|
| `date` | character | Date in YYYY-MM-DD format. |
| `opponent` | integer | Opponent. |
| `opponent_rk` | character | Opponent rk. |
| `result` | character | Result. |
| `result_rk` | character |  |
| `pace` | integer | Possessions per 48 minutes. |
| `offense_eff` | double |  |
| `offense_eff_1` | integer |  |
| `offense_e_fg_pct` | double |  |
| `offense_to_pct` | double |  |
| `offense_or_pct` | double |  |
| `offense_ftr` | double |  |
| `offense_2_p_pct` | character |  |
| `offense_2_p_pct_1` | double |  |
| `offense_3_p_pct` | character |  |
| `offense_3_p_pct_1` | double |  |
| `offense_3_pa_pct` | double |  |
| `defense_eff` | double |  |
| `defense_eff_1` | integer |  |
| `defense_e_fg_pct` | double |  |
| `defense_to_pct` | double |  |
| `defense_or_pct` | double |  |
| `defense_ftr` | double |  |
| `defense_2_p_pct` | character |  |
| `defense_2_p_pct_1` | double |  |
| `defense_3_p_pct` | character |  |
| `defense_3_p_pct_1` | double |  |
| `defense_3_pa_pct` | double |  |

**table_1**

| col_name | type | description |
|---|---|---|
| `column_0` | character |  |
| `pct_points_c` | double |  |
| `pct_points_pf` | double |  |
| `pct_points_sf` | double |  |
| `pct_points_sg` | double |  |
| `pct_points_pg` | double |  |

**table_2**

| col_name | type | description |
|---|---|---|
| `column_0` | character |  |
| `pct_off_rebs_c` | double |  |
| `pct_off_rebs_pf` | double |  |
| `pct_off_rebs_sf` | double |  |
| `pct_off_rebs_sg` | double |  |
| `pct_off_rebs_pg` | double |  |

**table_3**

| col_name | type | description |
|---|---|---|
| `column_0` | character |  |
| `pct_def_rebs_c` | double |  |
| `pct_def_rebs_pf` | double |  |
| `pct_def_rebs_sf` | double |  |
| `pct_def_rebs_sg` | double |  |
| `pct_def_rebs_pg` | double |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_game_plan-example}

```python
kenpom_game_plan(team='Duke', year=2025)
```

_Last validated n/a._

## kenpom_opponent_tracker

GET /opptracker.php - opponent tracker; side='o' for offense, 'd' for defense. Port of hoopR kp_opptracker().

**Endpoint URL:** `GET https://kenpom.com/opptracker.php`

**Valid URL:** [https://kenpom.com/opptracker.php?team=Duke&y=2025&t=o](https://kenpom.com/opptracker.php?team=Duke&y=2025&t=o)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | KenPom team name, spelled as the site does. |
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. Columns are narrower before 2010. |
| `t` | `side` |  |  | `Y` | Side of the ball: 'o' (offense) or 'd' (defense). |

### Returns {#kenpom_opponent_tracker-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**conf_table**

| col_name | type | description |
|---|---|---|
| `date` | character | Date in YYYY-MM-DD format. |
| `team` | character | Team-side label or team identifier. |
| `result` | character | Result. |
| `adj_oe` | double | Adjusted offensive efficiency. |
| `adj_oe_1` | integer |  |
| `e_fg_pct` | double | E field goals percentage (0-1 decimal). |
| `e_fg_pct_1` | integer |  |
| `to_pct` | double | To percentage (0-1 decimal). |
| `to_pct_1` | integer |  |
| `or_pct` | double | Or percentage (0-1 decimal). |
| `or_pct_1` | integer |  |
| `ftr` | double | Free-throw rate (offense). |
| `ftr_1` | integer |  |
| `2_p_pct` | double |  |
| `2_p_pct_1` | integer |  |
| `3_p_pct` | double |  |
| `3_p_pct_1` | integer |  |
| `ft_pct` | double | Free throw percentage (0-1). |
| `ft_pct_1` | integer |  |
| `3_pa_pct` | double |  |
| `3_pa_pct_1` | integer |  |
| `apl` | double |  |
| `apl_1` | integer |  |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_opponent_tracker-example}

```python
kenpom_opponent_tracker(team='Duke', year=2025, side='o')
```

_Last validated n/a._

## kenpom_player_career

GET /player.php - one player's career page (season-by-season stats and game log). Port of hoopR kp_player_career().

**Endpoint URL:** `GET https://kenpom.com/player.php`

**Valid URL:** [https://kenpom.com/player.php?p=51234](https://kenpom.com/player.php?p=51234)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `p` | `player_id` |  | `Y` |  | KenPom player id - the `p=` value on a player-page URL. |

### Returns {#kenpom_player_career-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**player_table**

| col_name | type | description |
|---|---|---|
| `column_0` | character |  |
| `column_0_rk` | character |  |
| `ht` | character | Listed height. |
| `wt` | double | Listed weight (lbs). |
| `yr` | character | Yr. |
| `g` | integer | Games played. |
| `pct_min` | double |  |
| `o_rtg` | double | O rtg. |
| `pct_poss` | double |  |
| `pct_shots` | double |  |
| `e_fg_pct` | double | E field goals percentage (0-1 decimal). |
| `ts_pct` | double | True shooting percentage (0-1). |
| `or_pct` | double | Or percentage (0-1 decimal). |
| `dr_pct` | double | Dr percentage (0-1 decimal). |
| `a_rate` | double | A rate. |
| `to_rate` | double | To rate. |
| `blk_pct` | double | Blocks percentage (0-1 decimal). |
| `stl_pct` | double | Steals percentage (0-1 decimal). |
| `fc_40` | double |  |
| `fd_40` | double |  |
| `ft_rate` | double | Ft rate. |
| `ftm_a` | character |  |
| `pct` | double | Win percentage. |
| `2_pm_a` | character |  |
| `pct_1` | double |  |
| `3_pm_a` | character |  |
| `pct_2` | double |  |

**schedule_table**

| col_name | type | description |
|---|---|---|
| `column_0` | double |  |
| `column_0_rk` | character |  |
| `column_0_rk_rk` | integer |  |
| `opponent` | character | Opponent. |
| `result` | character | Result. |
| `result_rk` | double |  |
| `site` | character |  |
| `site_rk` | character |  |
| `site_rk_rk` | double |  |
| `st` | double |  |
| `mp` | integer | Minutes played. |
| `o_rtg` | character | O rtg. |
| `pct_ps` | character |  |
| `pts` | integer | Points scored. |
| `2_pt` | character |  |
| `3_pt` | character |  |
| `ft` | character |  |
| `or` | integer | Or. |
| `dr` | integer | Dr. |
| `a` | integer | A. |
| `to` | integer | To. |
| `blk` | integer | Blocks. |
| `stl` | integer | Steals. |
| `pf` | integer | Personal fouls. |
| `pf_rk` | double | Pf rk. |

**schedule_table_3**

| col_name | type | description |
|---|---|---|
| `column_0` | double |  |
| `column_0_rk` | character |  |
| `column_0_rk_rk` | integer |  |
| `opponent` | character | Opponent. |
| `result` | character | Result. |
| `result_rk` | double |  |
| `site` | character |  |
| `site_rk` | character |  |
| `site_rk_rk` | double |  |
| `st` | character |  |
| `mp` | integer | Minutes played. |
| `o_rtg` | integer | O rtg. |
| `pct_ps` | integer |  |
| `pts` | integer | Points scored. |
| `2_pt` | character |  |
| `3_pt` | character |  |
| `ft` | character |  |
| `or` | integer | Or. |
| `dr` | integer | Dr. |
| `a` | integer | A. |
| `to` | integer | To. |
| `blk` | integer | Blocks. |
| `stl` | integer | Steals. |
| `pf` | integer | Personal fouls. |
| `pf_rk` | double | Pf rk. |

**schedule_table_4**

| col_name | type | description |
|---|---|---|
| `column_0` | double |  |
| `column_0_rk` | character |  |
| `column_0_rk_rk` | integer |  |
| `opponent` | character | Opponent. |
| `result` | character | Result. |
| `result_rk` | double |  |
| `site` | character |  |
| `site_rk` | character |  |
| `site_rk_rk` | character |  |
| `st` | character |  |
| `mp` | integer | Minutes played. |
| `o_rtg` | integer | O rtg. |
| `pct_ps` | integer |  |
| `pts` | integer | Points scored. |
| `2_pt` | character |  |
| `3_pt` | character |  |
| `ft` | character |  |
| `or` | integer | Or. |
| `dr` | integer | Dr. |
| `a` | integer | A. |
| `to` | integer | To. |
| `blk` | integer | Blocks. |
| `stl` | integer | Steals. |
| `pf` | integer | Personal fouls. |
| `pf_rk` | double | Pf rk. |

**schedule_table_5**

| col_name | type | description |
|---|---|---|
| `column_0` | double |  |
| `column_0_rk` | character |  |
| `column_0_rk_rk` | integer |  |
| `opponent` | character | Opponent. |
| `result` | character | Result. |
| `result_rk` | double |  |
| `site` | character |  |
| `site_rk` | character |  |
| `site_rk_rk` | double |  |
| `st` | character |  |
| `mp` | integer | Minutes played. |
| `o_rtg` | integer | O rtg. |
| `pct_ps` | integer |  |
| `pts` | integer | Points scored. |
| `2_pt` | character |  |
| `3_pt` | character |  |
| `ft` | character |  |
| `or` | integer | Or. |
| `dr` | integer | Dr. |
| `a` | integer | A. |
| `to` | integer | To. |
| `blk` | integer | Blocks. |
| `stl` | integer | Steals. |
| `pf` | integer | Personal fouls. |
| `pf_rk` | double | Pf rk. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_player_career-example}

```python
kenpom_player_career(player_id=51234)
```

_Last validated n/a._

## kenpom_box

GET /box.php - box-score detail for one game (per-team four factors, player lines, scoring runs). Port of hoopR kp_box().

**Endpoint URL:** `GET https://kenpom.com/box.php`

**Valid URL:** [https://kenpom.com/box.php?g=1097&y=2025](https://kenpom.com/box.php?g=1097&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `g` | `game_id` |  | `Y` |  | KenPom game id - the `g=` value on a FanMatch game link. |
| `y` | `year` |  | `Y` |  | Season (4-digit ENDING year) the game belongs to. |

### Returns {#kenpom_box-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**linescore_table2**

| col_name | type | description |
|---|---|---|
| `column_0` | character |  |
| `q1` | integer | Q1. |
| `q2` | integer | Q2. |
| `q3` | integer | Q3. |
| `q4` | integer | Q4. |
| `t` | integer | T. |

**table_1**

| col_name | type | description |
|---|---|---|
| `column_0` | character |  |
| `column_0_rk` | integer |  |
| `name` | character | Display name. |
| `min` | integer | Minutes played. |
| `o_rtg` | integer | O rtg. |
| `pct_ps` | integer |  |
| `pts` | integer | Points scored. |
| `2_pm_a` | character |  |
| `3_pm_a` | character |  |
| `ftm_a` | character |  |
| `or` | integer | Or. |
| `dr` | integer | Dr. |
| `a` | integer | A. |
| `to` | integer | To. |
| `blk` | integer | Blocks. |
| `stl` | integer | Steals. |
| `pf` | integer | Personal fouls. |

**table_2**

| col_name | type | description |
|---|---|---|
| `column_0` | character |  |
| `column_0_rk` | integer |  |
| `name` | character | Display name. |
| `min` | integer | Minutes played. |
| `o_rtg` | integer | O rtg. |
| `pct_ps` | integer |  |
| `pts` | integer | Points scored. |
| `2_pm_a` | character |  |
| `3_pm_a` | character |  |
| `ftm_a` | character |  |
| `or` | integer | Or. |
| `dr` | integer | Dr. |
| `a` | integer | A. |
| `to` | integer | To. |
| `blk` | integer | Blocks. |
| `stl` | integer | Steals. |
| `pf` | integer | Personal fouls. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_box-example}

```python
kenpom_box(game_id=1097, year=2025)
```

_Last validated n/a._

## kenpom_win_probability

GET /winprob.php - in-game win-probability table for one game. Port of hoopR kp_winprob().

**Endpoint URL:** `GET https://kenpom.com/winprob.php`

**Valid URL:** [https://kenpom.com/winprob.php?g=3577&y=2025](https://kenpom.com/winprob.php?g=3577&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `g` | `game_id` |  | `Y` |  | KenPom game id. |
| `y` | `year` |  | `Y` |  | Season (4-digit ENDING year) the game belongs to. |

### Returns {#kenpom_win_probability-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

No returns table is published for this endpoint: parser: KenPom draws this page from an embedded script, and parse_kenpom_page reads HTML tables only, so the captured page parses to no frames.

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_win_probability-example}

```python
kenpom_win_probability(game_id=3577, year=2025)
```

_Last validated n/a._

## kenpom_fan_match

GET /fanmatch.php - the FanMatch slate for one date (predictions, thrill score, results). Port of hoopR kp_fanmatch().

**Endpoint URL:** `GET https://kenpom.com/fanmatch.php`

**Valid URL:** [https://kenpom.com/fanmatch.php?d=2025-02-01](https://kenpom.com/fanmatch.php?d=2025-02-01)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `d` | `date` |  | `Y` |  | Slate date as YYYY-MM-DD. |

### Returns {#kenpom_fan_match-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**fanmatch_table**

| col_name | type | description |
|---|---|---|
| `game` | character | Game. |
| `prediction` | character | Pre-game prediction (favorite, score, win %). |
| `time` | character | Time / clock value. |
| `location` | character | Location. |
| `thrill_score` | double | Thrill score. |
| `come_back` | integer |  |
| `excite_ment` | double |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_fan_match-example}

```python
kenpom_fan_match(date='2025-02-01')
```

_Last validated n/a._

## kenpom_team_history

GET /history.php?t= - a program's season-by-season history. Port of hoopR kp_team_history().

**Endpoint URL:** `GET https://kenpom.com/history.php`

**Valid URL:** [https://kenpom.com/history.php?t=Duke](https://kenpom.com/history.php?t=Duke)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `t` | `team` |  | `Y` |  | KenPom team name, spelled as the site does. |

### Returns {#kenpom_team_history-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**player_table**

| col_name | type | description |
|---|---|---|
| `column_0` | integer |  |
| `pre_rk` | character |  |
| `coach` | character | Coach. |
| `conf` | character | character. |
| `w_l` | character | W l. |
| `adj_t` | double | Adj t. |
| `adj_o` | double | Adj o. |
| `adj_d` | double | Adj d. |
| `offense_e_fg_pct` | double |  |
| `offense_to_pct` | double |  |
| `offense_or_pct` | double |  |
| `offense_ftr` | double |  |
| `offense_2_p_pct` | double |  |
| `offense_3_p_pct` | double |  |
| `offense_ft_pct` | double |  |
| `offense_3_pa_pct` | double |  |
| `offense_a_pct` | double |  |
| `offense_apl` | double |  |
| `defense_e_fg_pct` | double |  |
| `defense_to_pct` | double |  |
| `defense_or_pct` | double |  |
| `defense_ftr` | double |  |
| `defense_2_p_pct` | double |  |
| `defense_3_p_pct` | double |  |
| `defense_blk_pct` | double |  |
| `defense_3_pa_pct` | double |  |
| `defense_a_pct` | double |  |
| `defense_apl` | double |  |
| `2_fp_pct` | double |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_team_history-example}

```python
kenpom_team_history(team='Duke')
```

_Last validated n/a._

## kenpom_coach_history

GET /history.php?c= - a coach's season-by-season history. Port of hoopR kp_coach_history().

**Endpoint URL:** `GET https://kenpom.com/history.php`

**Valid URL:** [https://kenpom.com/history.php?c=Jon+Scheyer](https://kenpom.com/history.php?c=Jon+Scheyer)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `c` | `coach` |  | `Y` |  | Coach name as KenPom spells it (e.g. 'Jon Scheyer'). |

### Returns {#kenpom_coach_history-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**player_table**

| col_name | type | description |
|---|---|---|
| `column_0` | integer |  |
| `pre_rk` | character |  |
| `team` | character | Team-side label or team identifier. |
| `conf` | character | character. |
| `w_l` | character | W l. |
| `adj_t` | double | Adj t. |
| `adj_o` | double | Adj o. |
| `adj_d` | double | Adj d. |
| `offense_e_fg_pct` | double |  |
| `offense_to_pct` | double |  |
| `offense_or_pct` | double |  |
| `offense_ftr` | double |  |
| `offense_2_p_pct` | double |  |
| `offense_3_p_pct` | double |  |
| `offense_ft_pct` | double |  |
| `offense_3_pa_pct` | double |  |
| `offense_a_pct` | double |  |
| `offense_apl` | double |  |
| `defense_e_fg_pct` | double |  |
| `defense_to_pct` | double |  |
| `defense_or_pct` | double |  |
| `defense_ftr` | double |  |
| `defense_2_p_pct` | double |  |
| `defense_3_p_pct` | double |  |
| `defense_blk_pct` | double |  |
| `defense_3_pa_pct` | double |  |
| `defense_a_pct` | double |  |
| `defense_apl` | double |  |
| `2_fp_pct` | double |  |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_coach_history-example}

```python
kenpom_coach_history(coach='Jon Scheyer')
```

_Last validated n/a._

## kenpom_program_ratings

GET /programs.php - program-level ratings across the full KenPom era. Port of hoopR kp_program_ratings().

**Endpoint URL:** `GET https://kenpom.com/programs.php`

**Valid URL:** [https://kenpom.com/programs.php](https://kenpom.com/programs.php)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#kenpom_program_ratings-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `column_0` | integer |  |
| `team` | character | Team-side label or team identifier. |
| `conf` | character | character. |
| `rtg` | double | Rtg. |
| `kenpom_best` | integer |  |
| `kenpom_best_1` | integer |  |
| `kenpom_worst` | integer |  |
| `kenpom_worst_1` | integer |  |
| `kenpom_median` | integer |  |
| `kenpom_top_10` | integer |  |
| `kenpom_top_25` | integer |  |
| `kenpom_top_50` | integer |  |
| `ncaa_tourney_ch` | integer |  |
| `ncaa_tourney_f4` | integer |  |
| `ncaa_tourney_s16` | integer |  |
| `ncaa_tourney_r1` | integer |  |
| `chg` | integer | Chg. |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_program_ratings-example}

```python
kenpom_program_ratings()
```

_Last validated n/a._

## kenpom_archive_ratings

GET /archive.php - the Pomeroy ratings as they stood on a past date. Port of hoopR kp_pomeroy_archive_ratings().

**Endpoint URL:** `GET https://kenpom.com/archive.php`

**Valid URL:** [https://kenpom.com/archive.php?d=2025-02-01](https://kenpom.com/archive.php?d=2025-02-01)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `d` | `date` |  | `Y` |  | Snapshot date as YYYY-MM-DD. |

### Returns {#kenpom_archive_ratings-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `rk` | character | Rk. |
| `team` | character | Team-side label or team identifier. |
| `ratings_on_2025_02_01_conf` | character |  |
| `ratings_on_2025_02_01_net_rtg` | character |  |
| `ratings_on_2025_02_01_o_rtg` | character |  |
| `ratings_on_2025_02_01_o_rtg_rk` | character |  |
| `ratings_on_2025_02_01_d_rtg` | character |  |
| `ratings_on_2025_02_01_d_rtg_rk` | character |  |
| `ratings_on_2025_02_01_adj_t` | character |  |
| `ratings_on_2025_02_01_adj_t_rk` | character |  |
| `final_2025_ratings_rk` | character |  |
| `final_2025_ratings_net_rtg` | character |  |
| `final_2025_ratings_o_rtg` | character |  |
| `final_2025_ratings_o_rtg_rk` | character |  |
| `final_2025_ratings_d_rtg` | character |  |
| `final_2025_ratings_d_rtg_rk` | character |  |
| `final_2025_ratings_adj_t` | character |  |
| `final_2025_ratings_adj_t_rk` | character |  |
| `rk_chg` | character | Rk chg. |
| `em_chg` | character | Em chg. |
| `adj_t_chg` | character | Adj t chg. |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_archive_ratings-example}

```python
kenpom_archive_ratings(date='2025-02-01')
```

_Last validated n/a._

## kenpom_conference

GET /conf.php - one conference's season page (standings, efficiency, per-team splits). Port of hoopR kp_conf().

**Endpoint URL:** `GET https://kenpom.com/conf.php`

**Valid URL:** [https://kenpom.com/conf.php?c=ACC&y=2025](https://kenpom.com/conf.php?c=ACC&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `c` | `conf` |  | `Y` |  | KenPom conference abbreviation (e.g. 'ACC', 'B10', 'SEC'). |
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns {#kenpom_conference-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**conf_table**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `overall` | character | Overall pick number. |
| `conf` | character | character. |
| `net_rtg` | double |  |
| `net_rtg_1` | integer |  |
| `o_rtg` | double | O rtg. |
| `o_rtg_1` | integer |  |
| `d_rtg` | double |  |
| `d_rtg_1` | integer |  |
| `adj_t` | double | Adj t. |
| `adj_t_1` | integer |  |
| `conf_sos` | double | Conf sos. |
| `conf_sos_1` | integer |  |
| `conf_sos_1_rk` | character |  |
| `conf_sos_1_rk_rk` | character |  |
| `conf_sos_1_rk_rk_rk` | character |  |
| `ncaa_seed` | integer | Ncaa seed. |

**conf_table_2**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `oe` | double | Raw offensive efficiency (points/100 poss). |
| `oe_1` | integer |  |
| `e_fg_pct` | double | E field goals percentage (0-1 decimal). |
| `e_fg_pct_1` | integer |  |
| `to_pct` | double | To percentage (0-1 decimal). |
| `to_pct_1` | integer |  |
| `or_pct` | double | Or percentage (0-1 decimal). |
| `or_pct_1` | integer |  |
| `ftr` | double | Free-throw rate (offense). |
| `ftr_1` | integer |  |
| `2_p_pct` | double |  |
| `2_p_pct_1` | integer |  |
| `3_p_pct` | double |  |
| `3_p_pct_1` | integer |  |
| `ft_pct` | double | Free throw percentage (0-1). |
| `ft_pct_1` | integer |  |
| `tempo` | double | Tempo. |
| `tempo_1` | integer |  |
| `ncaa_seed` | integer | Ncaa seed. |

**conf_table_3**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `de` | double | Raw defensive efficiency (points allowed/100 poss). |
| `de_1` | integer |  |
| `e_fg_pct` | double | E field goals percentage (0-1 decimal). |
| `e_fg_pct_1` | integer |  |
| `to_pct` | double | To percentage (0-1 decimal). |
| `to_pct_1` | integer |  |
| `or_pct` | double | Or percentage (0-1 decimal). |
| `or_pct_1` | integer |  |
| `ftr` | double | Free-throw rate (offense). |
| `ftr_1` | integer |  |
| `2_p_pct` | double |  |
| `2_p_pct_1` | integer |  |
| `3_p_pct` | double |  |
| `3_p_pct_1` | integer |  |
| `blk_pct` | double | Blocks percentage (0-1 decimal). |
| `blk_pct_1` | integer |  |
| `stl_pct` | double | Steals percentage (0-1 decimal). |
| `stl_pct_1` | integer |  |
| `ncaa_seed` | integer | Ncaa seed. |

**conf_table_4**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player` | character | Player name. |

**conf_table_5**

| col_name | type | description |
|---|---|---|
| `stat` | character | Stat. |
| `value` | double | Numeric or string value field. |
| `value_1` | integer |  |

**conf_table_6**

| col_name | type | description |
|---|---|---|
| `stat` | character | Stat. |
| `stat_rk` | character |  |
| `value` | character | Numeric or string value field. |
| `value_1` | integer |  |

**conf_table_rank**

| col_name | type | description |
|---|---|---|
| `conference` | integer | Conference name. |
| `conference_1` | character |  |
| `rating` | double | Simple Rating System (SRS) value. |
| `conference_2` | double |  |
| `conference_3` | character |  |
| `rating_1` | double |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_conference-example}

```python
kenpom_conference(conf='ACC', year=2025)
```

_Last validated n/a._

## kenpom_conference_stats

GET /confstats.php - league-wide conference comparison for one season. Port of hoopR kp_confstats().

**Endpoint URL:** `GET https://kenpom.com/confstats.php`

**Valid URL:** [https://kenpom.com/confstats.php?y=2025](https://kenpom.com/confstats.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns {#kenpom_conference_stats-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**confrank_table**

| col_name | type | description |
|---|---|---|
| `conf` | character | character. |
| `eff` | double | Eff. |
| `eff_1` | integer |  |
| `tempo` | double | Tempo. |
| `tempo_1` | integer |  |
| `e_fg_pct` | double | E field goals percentage (0-1 decimal). |
| `e_fg_pct_1` | integer |  |
| `to_pct` | double | To percentage (0-1 decimal). |
| `to_pct_1` | integer |  |
| `or_pct` | double | Or percentage (0-1 decimal). |
| `or_pct_1` | integer |  |
| `ftr` | double | Free-throw rate (offense). |
| `ftr_1` | integer |  |
| `blk_pct` | double | Blocks percentage (0-1 decimal). |
| `blk_pct_1` | integer |  |
| `stl_pct` | double | Steals percentage (0-1 decimal). |
| `stl_pct_1` | integer |  |
| `2_p_pct` | double |  |
| `2_p_pct_1` | integer |  |
| `3_p_pct` | double |  |
| `3_p_pct_1` | integer |  |
| `ft_pct` | double | Free throw percentage (0-1). |
| `ft_pct_1` | integer |  |
| `3_pa_pct` | double |  |
| `3_pa_pct_1` | integer |  |
| `a_pct` | double | A percentage (0-1 decimal). |
| `a_pct_1` | integer |  |
| `home_w_l` | character | Home team's wins losses. |
| `home_w_l_1` | double |  |
| `home_w_l_2` | integer |  |
| `close` | character | Close. |
| `close_1` | integer |  |
| `blowouts` | character | Blowouts. |
| `blowouts_1` | integer |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_conference_stats-example}

```python
kenpom_conference_stats(year=2025)
```

_Last validated n/a._

## kenpom_conference_history

GET /confhistory.php - one conference's season-by-season history. Port of hoopR kp_confhistory().

**Endpoint URL:** `GET https://kenpom.com/confhistory.php`

**Valid URL:** [https://kenpom.com/confhistory.php?c=ACC](https://kenpom.com/confhistory.php?c=ACC)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `c` | `conf` |  | `Y` |  | KenPom conference abbreviation. |

### Returns {#kenpom_conference_history-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**player_table**

| col_name | type | description |
|---|---|---|
| `year` | double | 4-digit year. |
| `rank` | integer | Rank. |
| `pace` | double | Possessions per 48 minutes. |
| `o_rtg` | double | O rtg. |
| `e_fg_pct` | double | E field goals percentage (0-1 decimal). |
| `to_pct` | double | To percentage (0-1 decimal). |
| `or_pct` | double | Or percentage (0-1 decimal). |
| `ftr` | double | Free-throw rate (offense). |
| `2_p_pct` | double |  |
| `3_p_pct` | double |  |
| `ft_pct` | double | Free throw percentage (0-1). |
| `3_pa_pct` | double |  |
| `a_pct` | double | A percentage (0-1 decimal). |
| `blk_pct` | double | Blocks percentage (0-1 decimal). |
| `stl_pct` | double | Steals percentage (0-1 decimal). |
| `home_record` | character | Home win-loss record. |
| `bids` | integer | Bids. |
| `s16` | integer | S16. |
| `f4` | integer | F4. |
| `ch` | integer | Ch. |
| `reg_season_champ` | character | Reg season champ. |
| `tourney_champ` | character | Tourney champ. |
| `best_team` | character | Best team. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_conference_history-example}

```python
kenpom_conference_history(conf='ACC')
```

_Last validated n/a._

## kenpom_trends

GET /trends.php - national Division I trends by season (tempo, efficiency, shooting, fouls). Port of hoopR kp_trends().

**Endpoint URL:** `GET https://kenpom.com/trends.php`

**Valid URL:** [https://kenpom.com/trends.php](https://kenpom.com/trends.php)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#kenpom_trends-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `season` | integer | Season year. |
| `efficiency` | double | Efficiency. |
| `tempo` | double | Tempo. |
| `e_fg_pct` | double | E field goals percentage (0-1 decimal). |
| `to_pct` | double | To percentage (0-1 decimal). |
| `or_pct` | double | Or percentage (0-1 decimal). |
| `ft_rate` | double | Ft rate. |
| `2_p_pct` | double |  |
| `3_p_pct` | double |  |
| `3_pa_pct` | double |  |
| `ft_pct` | double | Free throw percentage (0-1). |
| `a_pct` | double | A percentage (0-1 decimal). |
| `blk_pct` | double | Blocks percentage (0-1 decimal). |
| `stl_pct` | double | Steals percentage (0-1 decimal). |
| `nst_pct` | double |  |
| `avg_ht` | double |  |
| `cont` | double |  |
| `home_win_pct` | double | Home win percentage (0-1 decimal). |
| `ppg` | double | Points per game. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_trends-example}

```python
kenpom_trends()
```

_Last validated n/a._

## kenpom_home_court_advantage

GET /hca.php - per-team home-court advantage estimates. Port of hoopR kp_hca().

**Endpoint URL:** `GET https://kenpom.com/hca.php`

**Valid URL:** [https://kenpom.com/hca.php](https://kenpom.com/hca.php)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#kenpom_home_court_advantage-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `conf` | character | character. |
| `hca` | character | Hca. |
| `hca_rk` | character | Hca rk. |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_pf` | character |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_pf_rk` | character |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_pts` | character |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_pts_rk` | character |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_nst` | character |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_nst_rk` | character |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_blk` | character |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_blk_rk` | character |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_elev` | character |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_elev_rk` | character |  |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_home_court_advantage-example}

```python
kenpom_home_court_advantage()
```

_Last validated n/a._

## kenpom_arenas

GET /arenas.php - arena reference (name, capacity, average attendance) by team. Port of hoopR kp_arenas().

**Endpoint URL:** `GET https://kenpom.com/arenas.php`

**Valid URL:** [https://kenpom.com/arenas.php?y=2025](https://kenpom.com/arenas.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns {#kenpom_arenas-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `column_0` | integer |  |
| `team` | character | Team-side label or team identifier. |
| `conf` | character | character. |
| `arena` | character | Arena. |
| `alternate` | double | Alternate. |
| `ncaa_seed` | integer | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_arenas-example}

```python
kenpom_arenas(year=2025)
```

_Last validated n/a._

## kenpom_officials

GET /officials.php - referee ratings for one season. Port of hoopR kp_officials().

**Endpoint URL:** `GET https://kenpom.com/officials.php`

**Valid URL:** [https://kenpom.com/officials.php?y=2025](https://kenpom.com/officials.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns {#kenpom_officials-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `column_0` | integer |  |
| `name_faa` | character |  |
| `rating` | double | Simple Rating System (SRS) value. |
| `gms` | integer | Gms. |
| `last_game` | character | Last game date or score string. |
| `last_game_rk` | character |  |
| `last_game_rk_rk` | character |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_officials-example}

```python
kenpom_officials(year=2025)
```

_Last validated n/a._

## kenpom_referee

GET /referee.php - one referee's game log and splits for a season. Port of hoopR kp_referee().

**Endpoint URL:** `GET https://kenpom.com/referee.php`

**Valid URL:** [https://kenpom.com/referee.php?r=714294&y=2025](https://kenpom.com/referee.php?r=714294&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `r` | `referee` |  | `Y` |  | KenPom referee id - the `r=` value on an officials-table link. |
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns {#kenpom_referee-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `column_0` | integer |  |
| `date` | character | Date in YYYY-MM-DD format. |
| `time_et` | character | Time et. |
| `game` | character | Game. |
| `location` | character | Location. |
| `venue` | character | Venue name. |
| `venue_rk` | character |  |
| `thrill_score` | double | Thrill score. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_referee-example}

```python
kenpom_referee(referee=714294, year=2025)
```

_Last validated n/a._

## kenpom_game_attributes

GET /game_attrs.php - season game leaderboards by attribute (thrill score, comebacks, upsets, ...). Port of hoopR kp_game_attrs().

**Endpoint URL:** `GET https://kenpom.com/game_attrs.php`

**Valid URL:** [https://kenpom.com/game_attrs.php?y=2025&s=ThrillScore](https://kenpom.com/game_attrs.php?y=2025&s=ThrillScore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |
| `s` | `attribute` |  |  | `Y` | Attribute slug, e.g. ThrillScore, Comeback, FanMatch, Upsets, Busts, MinutesPlayed, PossessionLength, LeadChanges. |

### Returns {#kenpom_game_attributes-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `column_0` | integer |  |
| `date` | character | Date in YYYY-MM-DD format. |
| `game` | character | Game. |
| `game_rk` | character |  |
| `location` | character | Location. |
| `location_rk` | character |  |
| `value` | double | Numeric or string value field. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Example {#kenpom_game_attributes-example}

```python
kenpom_game_attributes(year=2025, attribute='ThrillScore')
```

_Last validated n/a._
