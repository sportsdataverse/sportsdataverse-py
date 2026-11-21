# Men's college basketball — KenPom (kenpom.com, subscription) — Other

> Men's college basketball — KenPom (kenpom.com, subscription) — Other — function reference in sdv-py, the SportsDataverse Python package.

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
| `rk` | String | Rk. |
| `team` | String | Team-side label or team identifier. |
| `conf` | String | character. |
| `w_l` | String | W l. |
| `net_rtg` | String |  |
| `o_rtg` | String | O rtg. |
| `o_rtg_rk` | String | O rtg rk. |
| `d_rtg` | String |  |
| `d_rtg_rk` | String |  |
| `adj_t` | String | Adj t. |
| `adj_t_rk` | String | Adj t rk. |
| `luck` | String | Luck. |
| `luck_rk` | String | Luck rk. |
| `strength_of_schedule_net_rtg` | String |  |
| `strength_of_schedule_net_rtg_rk` | String |  |
| `strength_of_schedule_o_rtg` | String |  |
| `strength_of_schedule_o_rtg_rk` | String |  |
| `strength_of_schedule_d_rtg` | String |  |
| `strength_of_schedule_d_rtg_rk` | String |  |
| `ncsos_net_rtg` | String |  |
| `ncsos_net_rtg_rk` | String |  |
| `ncaa_seed` | Int64 | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_ratings-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_ratings-example}

```python
kenpom_ratings(year=2025)
```

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
| `team` | String | Team-side label or team identifier. |
| `conf` | String | character. |
| `off_rating_adjusted` | Float64 |  |
| `off_rating_adjusted_rk` | Float64 |  |
| `off_rating_adjusted_rk_rk` | Int64 |  |
| `off_rating_raw` | Float64 |  |
| `off_rating_raw_rk` | Int64 |  |
| `def_rating_adjusted` | Float64 |  |
| `def_rating_adjusted_rk` | Float64 |  |
| `def_rating_adjusted_rk_rk` | Int64 |  |
| `def_rating_raw` | Float64 |  |
| `def_rating_raw_rk` | Int64 |  |
| `tempo_adjusted` | Float64 |  |
| `tempo_adjusted_rk` | Float64 |  |
| `tempo_raw` | Int64 |  |
| `tempo_raw_rk` | Float64 |  |
| `tempo_off` | Int64 |  |
| `avg_poss_length_off` | Float64 | Avg poss length off. |
| `avg_poss_length_def` | Int64 | Avg poss length def. |
| `avg_poss_length_def_rk` | Float64 | Avg poss length def rk. |
| `avg_poss_length_def_rk_rk` | Int64 |  |
| `ncaa_seed` | Int64 | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_efficiency-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_efficiency-example}

```python
kenpom_efficiency(year=2025)
```

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
| `team` | String | Team-side label or team identifier. |
| `conf` | String | character. |
| `adj_tempo` | Float64 |  |
| `adj_tempo_rk` | Int64 |  |
| `offense_adj_oe` | Float64 |  |
| `offense_adj_oe_rk` | Int64 |  |
| `offense_e_fg_pct` | Float64 |  |
| `offense_e_fg_pct_rk` | Int64 |  |
| `offense_to_pct` | Float64 |  |
| `offense_to_pct_rk` | Int64 |  |
| `offense_or_pct` | Float64 |  |
| `offense_or_pct_rk` | Int64 |  |
| `offense_ft_rate` | Float64 |  |
| `offense_ft_rate_rk` | Int64 |  |
| `defense_adj_de` | Float64 |  |
| `defense_adj_de_rk` | Int64 |  |
| `defense_e_fg_pct` | Float64 |  |
| `defense_e_fg_pct_rk` | Int64 |  |
| `defense_to_pct` | Float64 |  |
| `defense_to_pct_rk` | Int64 |  |
| `defense_or_pct` | Float64 |  |
| `defense_or_pct_rk` | Int64 |  |
| `defense_ft_rate` | Float64 |  |
| `defense_ft_rate_rk` | Int64 |  |
| `ncaa_seed` | Int64 | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_four_factors-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_four_factors-example}

```python
kenpom_four_factors(year=2025)
```

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
| `team` | String | Team-side label or team identifier. |
| `conf` | String | character. |
| `offense_ft` | Float64 |  |
| `offense_ft_rk` | Int64 |  |
| `offense_2_pt_fg` | Float64 |  |
| `offense_2_pt_fg_rk` | Int64 |  |
| `offense_3_pt_fg` | Float64 |  |
| `offense_3_pt_fg_rk` | Int64 |  |
| `defense_ft` | Float64 |  |
| `defense_ft_rk` | Int64 |  |
| `defense_2_pt_fg` | Float64 |  |
| `defense_2_pt_fg_rk` | Int64 |  |
| `defense_3_pt_fg` | Float64 |  |
| `defense_3_pt_fg_rk` | Int64 |  |
| `ncaa_seed` | Int64 | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_point_distribution-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_point_distribution-example}

```python
kenpom_point_distribution(year=2025)
```

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
| `team` | String | Team-side label or team identifier. |
| `conf` | String | character. |
| `avg_hgt` | String | Avg hgt. |
| `avg_hgt_rk` | String | Avg hgt rk. |
| `eff_hgt` | String | Eff hgt. |
| `eff_hgt_rk` | String | Eff hgt rk. |
| `c_hgt` | String | C hgt. |
| `c_hgt_rk` | String | C hgt rk. |
| `pf_hgt` | String | Pf hgt. |
| `pf_hgt_rk` | String | Pf hgt rk. |
| `sf_hgt` | String | Sf hgt. |
| `sf_hgt_rk` | String | Sf hgt rk. |
| `sg_hgt` | String | Sg hgt. |
| `sg_hgt_rk` | String | Sg hgt rk. |
| `pg_hgt` | String | Pg hgt. |
| `pg_hgt_rk` | String | Pg hgt rk. |
| `experience` | String | Years of professional experience. |
| `experience_rk` | String | Experience rk. |
| `bench` | String | Bench. |
| `bench_rk` | String | Bench rk. |
| `continuity` | String | Continuity. |
| `continuity_rk` | String | Continuity rk. |
| `ncaa_seed` | Int64 | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_height-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_height-example}

```python
kenpom_height(year=2025)
```

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
| `team` | String | Team-side label or team identifier. |
| `conf` | String | character. |
| `2_fp_pct` | Float64 |  |
| `2_fp_pct_rk` | Int64 |  |
| `adj2_fp` | Float64 |  |
| `adj2_fp_rk` | Int64 |  |
| `2_foul_total_time` | String |  |
| `2_foul_total_time_rk` | Int64 |  |
| `2_foul_time_on` | String |  |
| `2_foul_time_on_rk` | Int64 |  |
| `bench_pct` | Float64 | Bench percentage (0-1 decimal). |
| `bench_pct_rk` | Int64 | Bench pct rk. |
| `ncaa_seed` | Int64 | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_foul_trouble-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_foul_trouble-example}

```python
kenpom_foul_trouble(year=2025)
```

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
| `rk` | Int64 | Rk. |
| `player` | String | Player name. |
| `team` | String | Team-side label or team identifier. |
| `e_fg_pct` | Float64 | E field goals percentage (0-1 decimal). |
| `ht` | String | Listed height. |
| `wt` | Int64 | Listed weight (lbs). |
| `yr` | String | Yr. |
| `ncaa_seed` | Int64 | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_player_stats-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_player_stats-example}

```python
kenpom_player_stats(year=2025, metric='eFG')
```

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
| `column_0` | Int64 |  |
| `player` | String | Player name. |
| `k_poy_rating` | Float64 |  |

**kpoy_table_2**

| col_name | type | description |
|---|---|---|
| `column_0` | Float64 |  |
| `player` | String | Player name. |
| `game_mvp_s` | Int64 |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_kpoy-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_kpoy-example}

```python
kenpom_kpoy(year=2025)
```

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
| `date` | String | Date in YYYY-MM-DD format. |
| `opponent` | Int64 | Opponent. |
| `opponent_rk` | String | Opponent rk. |
| `result` | String | Result. |
| `result_rk` | String |  |
| `pace` | Int64 | Possessions per 48 minutes. |
| `offense_eff` | Float64 |  |
| `offense_eff_1` | Int64 |  |
| `offense_e_fg_pct` | Float64 |  |
| `offense_to_pct` | Float64 |  |
| `offense_or_pct` | Float64 |  |
| `offense_ftr` | Float64 |  |
| `offense_2_p_pct` | String |  |
| `offense_2_p_pct_1` | Float64 |  |
| `offense_3_p_pct` | String |  |
| `offense_3_p_pct_1` | Float64 |  |
| `offense_3_pa_pct` | Float64 |  |
| `defense_eff` | Float64 |  |
| `defense_eff_1` | Int64 |  |
| `defense_e_fg_pct` | Float64 |  |
| `defense_to_pct` | Float64 |  |
| `defense_or_pct` | Float64 |  |
| `defense_ftr` | Float64 |  |
| `defense_2_p_pct` | String |  |
| `defense_2_p_pct_1` | Float64 |  |
| `defense_3_p_pct` | String |  |
| `defense_3_p_pct_1` | Float64 |  |
| `defense_3_pa_pct` | Float64 |  |

**table_1**

| col_name | type | description |
|---|---|---|
| `column_0` | String |  |
| `pct_points_c` | Float64 |  |
| `pct_points_pf` | Float64 |  |
| `pct_points_sf` | Float64 |  |
| `pct_points_sg` | Float64 |  |
| `pct_points_pg` | Float64 |  |

**table_2**

| col_name | type | description |
|---|---|---|
| `column_0` | String |  |
| `pct_off_rebs_c` | Float64 |  |
| `pct_off_rebs_pf` | Float64 |  |
| `pct_off_rebs_sf` | Float64 |  |
| `pct_off_rebs_sg` | Float64 |  |
| `pct_off_rebs_pg` | Float64 |  |

**table_3**

| col_name | type | description |
|---|---|---|
| `column_0` | String |  |
| `pct_def_rebs_c` | Float64 |  |
| `pct_def_rebs_pf` | Float64 |  |
| `pct_def_rebs_sf` | Float64 |  |
| `pct_def_rebs_sg` | Float64 |  |
| `pct_def_rebs_pg` | Float64 |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_game_plan-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_game_plan-example}

```python
kenpom_game_plan(team='Duke', year=2025)
```

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
| `date` | String | Date in YYYY-MM-DD format. |
| `team` | String | Team-side label or team identifier. |
| `result` | String | Result. |
| `adj_oe` | Float64 | Adjusted offensive efficiency. |
| `adj_oe_1` | Int64 |  |
| `e_fg_pct` | Float64 | E field goals percentage (0-1 decimal). |
| `e_fg_pct_1` | Int64 |  |
| `to_pct` | Float64 | To percentage (0-1 decimal). |
| `to_pct_1` | Int64 |  |
| `or_pct` | Float64 | Or percentage (0-1 decimal). |
| `or_pct_1` | Int64 |  |
| `ftr` | Float64 | Free-throw rate (offense). |
| `ftr_1` | Int64 |  |
| `2_p_pct` | Float64 |  |
| `2_p_pct_1` | Int64 |  |
| `3_p_pct` | Float64 |  |
| `3_p_pct_1` | Int64 |  |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `ft_pct_1` | Int64 |  |
| `3_pa_pct` | Float64 |  |
| `3_pa_pct_1` | Int64 |  |
| `apl` | Float64 |  |
| `apl_1` | Int64 |  |
| `ncaa_seed` | Int64 | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_opponent_tracker-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_opponent_tracker-example}

```python
kenpom_opponent_tracker(team='Duke', year=2025, side='o')
```

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
| `column_0` | String |  |
| `column_0_rk` | String |  |
| `ht` | String | Listed height. |
| `wt` | Float64 | Listed weight (lbs). |
| `yr` | String | Yr. |
| `g` | Int64 | Games played. |
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

**schedule_table**

| col_name | type | description |
|---|---|---|
| `column_0` | Float64 |  |
| `column_0_rk` | String |  |
| `column_0_rk_rk` | Int64 |  |
| `opponent` | String | Opponent. |
| `result` | String | Result. |
| `result_rk` | Float64 |  |
| `site` | String |  |
| `site_rk` | String |  |
| `site_rk_rk` | Float64 |  |
| `st` | Float64 |  |
| `mp` | Int64 | Minutes played. |
| `o_rtg` | String | O rtg. |
| `pct_ps` | String |  |
| `pts` | Int64 | Points scored. |
| `2_pt` | String |  |
| `3_pt` | String |  |
| `ft` | String |  |
| `or` | Int64 | Or. |
| `dr` | Int64 | Dr. |
| `a` | Int64 | A. |
| `to` | Int64 | To. |
| `blk` | Int64 | Blocks. |
| `stl` | Int64 | Steals. |
| `pf` | Int64 | Personal fouls. |
| `pf_rk` | Float64 | Pf rk. |

**schedule_table_3**

| col_name | type | description |
|---|---|---|
| `column_0` | Float64 |  |
| `column_0_rk` | String |  |
| `column_0_rk_rk` | Int64 |  |
| `opponent` | String | Opponent. |
| `result` | String | Result. |
| `result_rk` | Float64 |  |
| `site` | String |  |
| `site_rk` | String |  |
| `site_rk_rk` | Float64 |  |
| `st` | String |  |
| `mp` | Int64 | Minutes played. |
| `o_rtg` | Int64 | O rtg. |
| `pct_ps` | Int64 |  |
| `pts` | Int64 | Points scored. |
| `2_pt` | String |  |
| `3_pt` | String |  |
| `ft` | String |  |
| `or` | Int64 | Or. |
| `dr` | Int64 | Dr. |
| `a` | Int64 | A. |
| `to` | Int64 | To. |
| `blk` | Int64 | Blocks. |
| `stl` | Int64 | Steals. |
| `pf` | Int64 | Personal fouls. |
| `pf_rk` | Float64 | Pf rk. |

**schedule_table_4**

| col_name | type | description |
|---|---|---|
| `column_0` | Float64 |  |
| `column_0_rk` | String |  |
| `column_0_rk_rk` | Int64 |  |
| `opponent` | String | Opponent. |
| `result` | String | Result. |
| `result_rk` | Float64 |  |
| `site` | String |  |
| `site_rk` | String |  |
| `site_rk_rk` | String |  |
| `st` | String |  |
| `mp` | Int64 | Minutes played. |
| `o_rtg` | Int64 | O rtg. |
| `pct_ps` | Int64 |  |
| `pts` | Int64 | Points scored. |
| `2_pt` | String |  |
| `3_pt` | String |  |
| `ft` | String |  |
| `or` | Int64 | Or. |
| `dr` | Int64 | Dr. |
| `a` | Int64 | A. |
| `to` | Int64 | To. |
| `blk` | Int64 | Blocks. |
| `stl` | Int64 | Steals. |
| `pf` | Int64 | Personal fouls. |
| `pf_rk` | Float64 | Pf rk. |

**schedule_table_5**

| col_name | type | description |
|---|---|---|
| `column_0` | Float64 |  |
| `column_0_rk` | String |  |
| `column_0_rk_rk` | Int64 |  |
| `opponent` | String | Opponent. |
| `result` | String | Result. |
| `result_rk` | Float64 |  |
| `site` | String |  |
| `site_rk` | String |  |
| `site_rk_rk` | Float64 |  |
| `st` | String |  |
| `mp` | Int64 | Minutes played. |
| `o_rtg` | Int64 | O rtg. |
| `pct_ps` | Int64 |  |
| `pts` | Int64 | Points scored. |
| `2_pt` | String |  |
| `3_pt` | String |  |
| `ft` | String |  |
| `or` | Int64 | Or. |
| `dr` | Int64 | Dr. |
| `a` | Int64 | A. |
| `to` | Int64 | To. |
| `blk` | Int64 | Blocks. |
| `stl` | Int64 | Steals. |
| `pf` | Int64 | Personal fouls. |
| `pf_rk` | Float64 | Pf rk. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_player_career-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_player_career-example}

```python
kenpom_player_career(player_id=51234)
```

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
| `column_0` | String |  |
| `q1` | Int64 | Q1. |
| `q2` | Int64 | Q2. |
| `q3` | Int64 | Q3. |
| `q4` | Int64 | Q4. |
| `t` | Int64 | T. |

**table_1**

| col_name | type | description |
|---|---|---|
| `column_0` | String |  |
| `jersey` | String | Jersey number worn by the player. |
| `name` | String | Display name. |
| `min` | Int64 | Minutes played. |
| `o_rtg` | Int64 | O rtg. |
| `pct_ps` | Int64 |  |
| `pts` | Int64 | Points scored. |
| `2_pm_a` | String |  |
| `3_pm_a` | String |  |
| `ftm_a` | String |  |
| `or` | Int64 | Or. |
| `dr` | Int64 | Dr. |
| `a` | Int64 | A. |
| `to` | Int64 | To. |
| `blk` | Int64 | Blocks. |
| `stl` | Int64 | Steals. |
| `pf` | Int64 | Personal fouls. |

**table_2**

| col_name | type | description |
|---|---|---|
| `column_0` | String |  |
| `jersey` | String | Jersey number worn by the player. |
| `name` | String | Display name. |
| `min` | Int64 | Minutes played. |
| `o_rtg` | Int64 | O rtg. |
| `pct_ps` | Int64 |  |
| `pts` | Int64 | Points scored. |
| `2_pm_a` | String |  |
| `3_pm_a` | String |  |
| `ftm_a` | String |  |
| `or` | Int64 | Or. |
| `dr` | Int64 | Dr. |
| `a` | Int64 | A. |
| `to` | Int64 | To. |
| `blk` | Int64 | Blocks. |
| `stl` | Int64 | Steals. |
| `pf` | Int64 | Personal fouls. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_box-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_box-example}

```python
kenpom_box(game_id=1097, year=2025)
```

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

### Raises {#kenpom_win_probability-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_win_probability-example}

```python
kenpom_win_probability(game_id=3577, year=2025)
```

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
| `game` | String | Game. |
| `prediction` | String | Pre-game prediction (favorite, score, win %). |
| `time` | String | Time / clock value. |
| `location` | String | Location. |
| `thrill_score` | Float64 | Thrill score. |
| `come_back` | Int64 |  |
| `excite_ment` | Float64 |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_fan_match-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_fan_match-example}

```python
kenpom_fan_match(date='2025-02-01')
```

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
| `column_0` | Int64 |  |
| `pre_rk` | String |  |
| `team` | String | Team-side label or team identifier. |
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
| `ncaa_seed` | Int64 | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_coach_history-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_coach_history-example}

```python
kenpom_coach_history(coach='Jon Scheyer')
```

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
| `column_0` | Int64 |  |
| `team` | String | Team-side label or team identifier. |
| `conf` | String | character. |
| `rtg` | Float64 | Rtg. |
| `kenpom_best` | Int64 |  |
| `kenpom_best_1` | Int64 |  |
| `kenpom_worst` | Int64 |  |
| `kenpom_worst_1` | Int64 |  |
| `kenpom_median` | Int64 |  |
| `kenpom_top_10` | Int64 |  |
| `kenpom_top_25` | Int64 |  |
| `kenpom_top_50` | Int64 |  |
| `ncaa_tourney_ch` | Int64 |  |
| `ncaa_tourney_f4` | Int64 |  |
| `ncaa_tourney_s16` | Int64 |  |
| `ncaa_tourney_r1` | Int64 |  |
| `chg` | Int64 | Chg. |
| `ncaa_seed` | Int64 | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_program_ratings-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_program_ratings-example}

```python
kenpom_program_ratings()
```

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
| `rk` | String | Rk. |
| `team` | String | Team-side label or team identifier. |
| `ratings_on_2025_02_01_conf` | String |  |
| `ratings_on_2025_02_01_net_rtg` | String |  |
| `ratings_on_2025_02_01_o_rtg` | String |  |
| `ratings_on_2025_02_01_o_rtg_rk` | String |  |
| `ratings_on_2025_02_01_d_rtg` | String |  |
| `ratings_on_2025_02_01_d_rtg_rk` | String |  |
| `ratings_on_2025_02_01_adj_t` | String |  |
| `ratings_on_2025_02_01_adj_t_rk` | String |  |
| `final_2025_ratings_rk` | String |  |
| `final_2025_ratings_net_rtg` | String |  |
| `final_2025_ratings_o_rtg` | String |  |
| `final_2025_ratings_o_rtg_rk` | String |  |
| `final_2025_ratings_d_rtg` | String |  |
| `final_2025_ratings_d_rtg_rk` | String |  |
| `final_2025_ratings_adj_t` | String |  |
| `final_2025_ratings_adj_t_rk` | String |  |
| `rk_chg` | String | Rk chg. |
| `em_chg` | String | Em chg. |
| `adj_t_chg` | String | Adj t chg. |
| `ncaa_seed` | Int64 | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_archive_ratings-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_archive_ratings-example}

```python
kenpom_archive_ratings(date='2025-02-01')
```

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
| `team` | String | Team-side label or team identifier. |
| `overall` | String | Overall pick number. |
| `conf` | String | character. |
| `net_rtg` | Float64 |  |
| `net_rtg_1` | Int64 |  |
| `o_rtg` | Float64 | O rtg. |
| `o_rtg_1` | Int64 |  |
| `d_rtg` | Float64 |  |
| `d_rtg_1` | Int64 |  |
| `adj_t` | Float64 | Adj t. |
| `adj_t_1` | Int64 |  |
| `conf_sos` | Float64 | Conf sos. |
| `conf_sos_1` | Int64 |  |
| `conf_sos_1_rk` | String |  |
| `conf_sos_1_rk_rk` | String |  |
| `conf_sos_1_rk_rk_rk` | String |  |
| `ncaa_seed` | Int64 | Ncaa seed. |

**conf_table_2**

| col_name | type | description |
|---|---|---|
| `team` | String | Team-side label or team identifier. |
| `oe` | Float64 | Raw offensive efficiency (points/100 poss). |
| `oe_1` | Int64 |  |
| `e_fg_pct` | Float64 | E field goals percentage (0-1 decimal). |
| `e_fg_pct_1` | Int64 |  |
| `to_pct` | Float64 | To percentage (0-1 decimal). |
| `to_pct_1` | Int64 |  |
| `or_pct` | Float64 | Or percentage (0-1 decimal). |
| `or_pct_1` | Int64 |  |
| `ftr` | Float64 | Free-throw rate (offense). |
| `ftr_1` | Int64 |  |
| `2_p_pct` | Float64 |  |
| `2_p_pct_1` | Int64 |  |
| `3_p_pct` | Float64 |  |
| `3_p_pct_1` | Int64 |  |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `ft_pct_1` | Int64 |  |
| `tempo` | Float64 | Tempo. |
| `tempo_1` | Int64 |  |
| `ncaa_seed` | Int64 | Ncaa seed. |

**conf_table_3**

| col_name | type | description |
|---|---|---|
| `team` | String | Team-side label or team identifier. |
| `de` | Float64 | Raw defensive efficiency (points allowed/100 poss). |
| `de_1` | Int64 |  |
| `e_fg_pct` | Float64 | E field goals percentage (0-1 decimal). |
| `e_fg_pct_1` | Int64 |  |
| `to_pct` | Float64 | To percentage (0-1 decimal). |
| `to_pct_1` | Int64 |  |
| `or_pct` | Float64 | Or percentage (0-1 decimal). |
| `or_pct_1` | Int64 |  |
| `ftr` | Float64 | Free-throw rate (offense). |
| `ftr_1` | Int64 |  |
| `2_p_pct` | Float64 |  |
| `2_p_pct_1` | Int64 |  |
| `3_p_pct` | Float64 |  |
| `3_p_pct_1` | Int64 |  |
| `blk_pct` | Float64 | Blocks percentage (0-1 decimal). |
| `blk_pct_1` | Int64 |  |
| `stl_pct` | Float64 | Steals percentage (0-1 decimal). |
| `stl_pct_1` | Int64 |  |
| `ncaa_seed` | Int64 | Ncaa seed. |

**conf_table_4**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank. |
| `player` | String | Player name. |

**conf_table_5**

| col_name | type | description |
|---|---|---|
| `stat` | String | Stat. |
| `value` | Float64 | Numeric or string value field. |
| `value_1` | Int64 |  |

**conf_table_6**

| col_name | type | description |
|---|---|---|
| `stat` | String | Stat. |
| `stat_rk` | String |  |
| `value` | String | Numeric or string value field. |
| `value_1` | Int64 |  |

**conf_table_rank**

| col_name | type | description |
|---|---|---|
| `conference` | Int64 | Conference name. |
| `conference_1` | String |  |
| `rating` | Float64 | Simple Rating System (SRS) value. |
| `conference_2` | Float64 |  |
| `conference_3` | String |  |
| `rating_1` | Float64 |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_conference-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_conference-example}

```python
kenpom_conference(conf='ACC', year=2025)
```

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
| `conf` | String | character. |
| `eff` | Float64 | Eff. |
| `eff_1` | Int64 |  |
| `tempo` | Float64 | Tempo. |
| `tempo_1` | Int64 |  |
| `e_fg_pct` | Float64 | E field goals percentage (0-1 decimal). |
| `e_fg_pct_1` | Int64 |  |
| `to_pct` | Float64 | To percentage (0-1 decimal). |
| `to_pct_1` | Int64 |  |
| `or_pct` | Float64 | Or percentage (0-1 decimal). |
| `or_pct_1` | Int64 |  |
| `ftr` | Float64 | Free-throw rate (offense). |
| `ftr_1` | Int64 |  |
| `blk_pct` | Float64 | Blocks percentage (0-1 decimal). |
| `blk_pct_1` | Int64 |  |
| `stl_pct` | Float64 | Steals percentage (0-1 decimal). |
| `stl_pct_1` | Int64 |  |
| `2_p_pct` | Float64 |  |
| `2_p_pct_1` | Int64 |  |
| `3_p_pct` | Float64 |  |
| `3_p_pct_1` | Int64 |  |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `ft_pct_1` | Int64 |  |
| `3_pa_pct` | Float64 |  |
| `3_pa_pct_1` | Int64 |  |
| `a_pct` | Float64 | A percentage (0-1 decimal). |
| `a_pct_1` | Int64 |  |
| `home_w_l` | String | Home team's wins losses. |
| `home_w_l_1` | Float64 |  |
| `home_w_l_2` | Int64 |  |
| `close` | String | Close. |
| `close_1` | Int64 |  |
| `blowouts` | String | Blowouts. |
| `blowouts_1` | Int64 |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_conference_stats-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_conference_stats-example}

```python
kenpom_conference_stats(year=2025)
```

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
| `year` | Float64 | 4-digit year. |
| `rank` | Int64 | Rank. |
| `pace` | Float64 | Possessions per 48 minutes. |
| `o_rtg` | Float64 | O rtg. |
| `e_fg_pct` | Float64 | E field goals percentage (0-1 decimal). |
| `to_pct` | Float64 | To percentage (0-1 decimal). |
| `or_pct` | Float64 | Or percentage (0-1 decimal). |
| `ftr` | Float64 | Free-throw rate (offense). |
| `2_p_pct` | Float64 |  |
| `3_p_pct` | Float64 |  |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `3_pa_pct` | Float64 |  |
| `a_pct` | Float64 | A percentage (0-1 decimal). |
| `blk_pct` | Float64 | Blocks percentage (0-1 decimal). |
| `stl_pct` | Float64 | Steals percentage (0-1 decimal). |
| `home_record` | String | Home win-loss record. |
| `bids` | Int64 | Bids. |
| `s16` | Int64 | S16. |
| `f4` | Int64 | F4. |
| `ch` | Int64 | Ch. |
| `reg_season_champ` | String | Reg season champ. |
| `tourney_champ` | String | Tourney champ. |
| `best_team` | String | Best team. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_conference_history-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_conference_history-example}

```python
kenpom_conference_history(conf='ACC')
```

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
| `season` | Int64 | Season year. |
| `efficiency` | Float64 | Efficiency. |
| `tempo` | Float64 | Tempo. |
| `e_fg_pct` | Float64 | E field goals percentage (0-1 decimal). |
| `to_pct` | Float64 | To percentage (0-1 decimal). |
| `or_pct` | Float64 | Or percentage (0-1 decimal). |
| `ft_rate` | Float64 | Ft rate. |
| `2_p_pct` | Float64 |  |
| `3_p_pct` | Float64 |  |
| `3_pa_pct` | Float64 |  |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `a_pct` | Float64 | A percentage (0-1 decimal). |
| `blk_pct` | Float64 | Blocks percentage (0-1 decimal). |
| `stl_pct` | Float64 | Steals percentage (0-1 decimal). |
| `nst_pct` | Float64 |  |
| `avg_ht` | Float64 |  |
| `cont` | Float64 |  |
| `home_win_pct` | Float64 | Home win percentage (0-1 decimal). |
| `ppg` | Float64 | Points per game. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_trends-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_trends-example}

```python
kenpom_trends()
```

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
| `team` | String | Team-side label or team identifier. |
| `conf` | String | character. |
| `hca` | String | Hca. |
| `hca_rk` | String | Hca rk. |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_pf` | String |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_pf_rk` | String |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_pts` | String |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_pts_rk` | String |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_nst` | String |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_nst_rk` | String |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_blk` | String |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_blk_rk` | String |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_elev` | String |  |
| `model_inputs_based_on_last_60_home_and_road_conf_games_values_are_per_game_differences_between_home_and_road_margin_elev_rk` | String |  |
| `ncaa_seed` | Int64 | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_home_court_advantage-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_home_court_advantage-example}

```python
kenpom_home_court_advantage()
```

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
| `column_0` | Int64 |  |
| `team` | String | Team-side label or team identifier. |
| `conf` | String | character. |
| `arena` | String | Arena. |
| `alternate` | Float64 | Alternate. |
| `ncaa_seed` | Int64 | Ncaa seed. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_arenas-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_arenas-example}

```python
kenpom_arenas(year=2025)
```

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
| `column_0` | Int64 |  |
| `name_faa` | String |  |
| `rating` | Float64 | Simple Rating System (SRS) value. |
| `gms` | Int64 | Gms. |
| `last_game` | String | Last game date or score string. |
| `last_game_rk` | String |  |
| `last_game_rk_rk` | String |  |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_officials-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_officials-example}

```python
kenpom_officials(year=2025)
```

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
| `column_0` | Int64 |  |
| `date` | String | Date in YYYY-MM-DD format. |
| `time_et` | String | Time et. |
| `game` | String | Game. |
| `location` | String | Location. |
| `venue` | String | Venue name. |
| `venue_rk` | String |  |
| `thrill_score` | Float64 | Thrill score. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_referee-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_referee-example}

```python
kenpom_referee(referee=714294, year=2025)
```

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
| `column_0` | Int64 |  |
| `date` | String | Date in YYYY-MM-DD format. |
| `game` | String | Game. |
| `game_rk` | String |  |
| `location` | String | Location. |
| `location_rk` | String |  |
| `value` | Float64 | Numeric or string value field. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_game_attributes-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_game_attributes-example}

```python
kenpom_game_attributes(year=2025, attribute='ThrillScore')
```
