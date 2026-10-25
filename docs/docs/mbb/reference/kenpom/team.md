---
title: "MBB — KenPom (kenpom.com, subscription) — Team"
sidebar_label: "Team"
sidebar_position: 1
description: "MBB — KenPom (kenpom.com, subscription) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MBB — KenPom (kenpom.com, subscription) — Team

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

### Raises {#kenpom_team_stats-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_team_stats-example}

```python
kenpom_team_stats(year=2025, side='o')
```

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
| `jersey` | character | Jersey number worn by the player. |
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

### Raises {#kenpom_team-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_team-example}

```python
kenpom_team(team='Duke', year=2025)
```

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
| `jersey` | character | Jersey number worn by the player. |
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
| `jersey` | character | Jersey number worn by the player. |
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

### Raises {#kenpom_team_players_expanded-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_team_players_expanded-example}

```python
kenpom_team_players_expanded(team='Duke', year=2025)
```

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

### Raises {#kenpom_team_history-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_team_history-example}

```python
kenpom_team_history(team='Duke')
```
