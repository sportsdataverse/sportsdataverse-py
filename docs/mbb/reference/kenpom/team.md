# Men's college basketball — KenPom (kenpom.com, subscription) — Team

> Men's college basketball — KenPom (kenpom.com, subscription) — Team — function reference in sdv-py, the SportsDataverse Python package.

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
| `team` | String | Team-side label or team identifier. |
| `conf` | String | character. |
| `3_p_pct` | String |  |
| `3_p_pct_rk` | String |  |
| `2_p_pct` | String |  |
| `2_p_pct_rk` | String |  |
| `ft_pct` | String | Free throw percentage (0-1). |
| `ft_pct_rk` | String | Ft pct rk. |
| `blk_pct` | String | Blocks percentage (0-1 decimal). |
| `blk_pct_rk` | String | Blk pct rk. |
| `stl_pct` | String | Steals percentage (0-1 decimal). |
| `stl_pct_rk` | String | Stl pct rk. |
| `nst_pct` | String |  |
| `nst_pct_rk` | String |  |
| `2_p_dist` | String |  |
| `2_p_dist_rk` | String |  |
| `a_pct` | String | A percentage (0-1 decimal). |
| `a_pct_rk` | String | A pct rk. |
| `3_pa_pct` | String |  |
| `3_pa_pct_rk` | String |  |
| `adj_oe` | String | Adjusted offensive efficiency. |
| `adj_oe_rk` | String | Adj oe rk. |
| `ncaa_seed` | Int64 | Ncaa seed. |

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
| `category` | String | Category label. |
| `offense` | String |  |
| `defense` | String |  |
| `d_i_avg` | Float64 |  |

**schedule_table**

| col_name | type | description |
|---|---|---|
| `0` | String |  |
| `1` | Int64 |  |
| `2` | Int64 |  |
| `3` | String |  |
| `4` | String |  |
| `5` | Int64 |  |
| `6` | String |  |
| `7` | String |  |
| `8` | String |  |
| `9` | String |  |
| `10` | String |  |

**player_table**

| col_name | type | description |
|---|---|---|
| `jersey` | String | Jersey number worn by the player. |
| `column_0_rk` | String |  |
| `ht` | String | Listed height. |
| `wt` | Float64 | Listed weight (lbs). |
| `yr` | String | Yr. |
| `g` | Float64 | Games played. |
| `s` | Float64 | S. |
| `pct_min` | Float64 |  |
| `o_rtg` | Float64 | O rtg. |
| `pct_poss` | Float64 |  |
| `pct_shots` | Float64 |  |
| `e_fg_pct` | Float64 | E field goals percentage (0-1 decimal). |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `or_pct` | Float64 | Or percentage (0-1 decimal). |
| `dr_pct` | Float64 | Dr percentage (0-1 decimal). |
| `a_rate` | Float64 | A rate. |
| `to_rate` | Float64 | To rate. |
| `blk_pct` | Float64 | Blocks percentage (0-1 decimal). |
| `stl_pct` | Float64 | Steals percentage (0-1 decimal). |
| `fc_40` | Float64 |  |
| `fd_40` | Float64 |  |
| `ft_rate` | Float64 | Ft rate. |
| `ftm_a` | String |  |
| `pct` | Float64 | Win percentage. |
| `2_pm_a` | String |  |
| `pct_1` | Float64 |  |
| `3_pm_a` | String |  |
| `pct_2` | Float64 |  |

**depth_chart**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `pct_pg` | Float64 |  |
| `pct_sg` | Float64 |  |
| `pct_sf` | Float64 |  |
| `pct_pf` | Float64 |  |
| `pct_c` | Float64 |  |
| `name` | String | Display name. |
| `pct_poss` | Float64 |  |
| `fta` | Int64 | Free throw attempts. |
| `fg2_a` | Int64 |  |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `height` | String | Player height (string e.g. '6-2' or inches). |
| `weight` | Int64 | Player weight in pounds. |
| `year` | String | 4-digit year. |

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
| `jersey` | String | Jersey number worn by the player. |
| `column_0_rk` | String |  |
| `ht` | String | Listed height. |
| `wt` | Float64 | Listed weight (lbs). |
| `yr` | String | Yr. |
| `g` | Float64 | Games played. |
| `pct_min` | Float64 |  |
| `o_rtg` | String | O rtg. |
| `pct_poss` | String |  |
| `pct_shots` | String |  |
| `e_fg_pct` | String | E field goals percentage (0-1 decimal). |
| `ts_pct` | String | True shooting percentage (0-1). |
| `or_pct` | Float64 | Or percentage (0-1 decimal). |
| `dr_pct` | String | Dr percentage (0-1 decimal). |
| `a_rate` | String | A rate. |
| `to_rate` | Float64 | To rate. |
| `blk_pct` | String | Blocks percentage (0-1 decimal). |
| `stl_pct` | String | Steals percentage (0-1 decimal). |
| `fc_40` | Float64 |  |
| `fd_40` | String |  |
| `ft_rate` | String | Ft rate. |
| `ftm_a` | String |  |
| `pct` | String | Win percentage. |
| `2_pm_a` | String |  |
| `pct_1` | Float64 |  |
| `3_pm_a` | String |  |
| `pct_2` | String |  |

**player_table_2**

| col_name | type | description |
|---|---|---|
| `jersey` | String | Jersey number worn by the player. |
| `column_0_rk` | String |  |
| `ht` | String | Listed height. |
| `wt` | Float64 | Listed weight (lbs). |
| `yr` | String | Yr. |
| `g` | Float64 | Games played. |
| `pct_min` | String |  |
| `o_rtg` | String | O rtg. |
| `pct_poss` | String |  |
| `pct_shots` | String |  |
| `e_fg_pct` | String | E field goals percentage (0-1 decimal). |
| `ts_pct` | String | True shooting percentage (0-1). |
| `or_pct` | Float64 | Or percentage (0-1 decimal). |
| `dr_pct` | String | Dr percentage (0-1 decimal). |
| `a_rate` | String | A rate. |
| `to_rate` | Float64 | To rate. |
| `blk_pct` | String | Blocks percentage (0-1 decimal). |
| `stl_pct` | String | Steals percentage (0-1 decimal). |
| `fc_40` | Float64 |  |
| `fd_40` | String |  |
| `ft_rate` | String | Ft rate. |
| `ftm_a` | String |  |
| `pct` | String | Win percentage. |
| `2_pm_a` | String |  |
| `pct_1` | String |  |
| `3_pm_a` | String |  |
| `pct_2` | String |  |

**minutes_table**

| col_name | type | description |
|---|---|---|
| `column_0` | String |  |
| `column_0_rk` | Int64 |  |
| `column_0_rk_rk` | String |  |
| `column_0_rk_rk_rk` | String |  |
| `caleb_foster` | Int64 |  |
| `darren_harris` | Float64 |  |
| `isaiah_evans` | Float64 |  |
| `mason_gillis` | Int64 |  |
| `sion_james` | Int64 |  |
| `tyrese_proctor` | Int64 |  |
| `kon_knueppel` | Int64 |  |
| `cooper_flagg` | Int64 |  |
| `maliq_brown` | Int64 |  |
| `patrick_ngongba` | Float64 |  |
| `khaman_maluach` | Int64 |  |
| `starting_lineup_number` | Int64 |  |

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
| `column_0` | Int64 |  |
| `pre_rk` | String |  |
| `coach` | String | Coach. |
| `conf` | String | character. |
| `w_l` | String | W l. |
| `adj_t` | Float64 | Adj t. |
| `adj_o` | Float64 | Adj o. |
| `adj_d` | Float64 | Adj d. |
| `offense_e_fg_pct` | Float64 |  |
| `offense_to_pct` | Float64 |  |
| `offense_or_pct` | Float64 |  |
| `offense_ftr` | Float64 |  |
| `offense_2_p_pct` | Float64 |  |
| `offense_3_p_pct` | Float64 |  |
| `offense_ft_pct` | Float64 |  |
| `offense_3_pa_pct` | Float64 |  |
| `offense_a_pct` | Float64 |  |
| `offense_apl` | Float64 |  |
| `defense_e_fg_pct` | Float64 |  |
| `defense_to_pct` | Float64 |  |
| `defense_or_pct` | Float64 |  |
| `defense_ftr` | Float64 |  |
| `defense_2_p_pct` | Float64 |  |
| `defense_3_p_pct` | Float64 |  |
| `defense_blk_pct` | Float64 |  |
| `defense_3_pa_pct` | Float64 |  |
| `defense_a_pct` | Float64 |  |
| `defense_apl` | Float64 |  |
| `2_fp_pct` | Float64 |  |

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
