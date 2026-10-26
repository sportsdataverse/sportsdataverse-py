---
title: "MLB — MLB Statcast (Baseball Savant) — Leaderboard: catcher–park"
sidebar_label: "Leaderboard: catcher–park"
sidebar_position: 2
description: "MLB — MLB Statcast (Baseball Savant) — Leaderboard: catcher–park — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Statcast (Baseball Savant) — Leaderboard: catcher–park

## mlb_statcast_leaderboard_catcher_throwing

GET /leaderboard/catcher-throwing — catcher throwing leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/catcher-throwing`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/catcher-throwing?csv=true](https://baseballsavant.mlb.com/leaderboard/catcher-throwing?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_catcher_throwing-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | MLBAM player id. |
| `player_name` | String | Player name. |
| `team_name` | String | Team name. |
| `start_year` | Int64 | First season in the range. |
| `end_year` | Int64 | Last season in the range. |
| `sb_attempts` | Int64 | Sb attempts. |
| `catcher_stealing_runs` | Float64 | Catcher stealing runs. |
| `caught_stealing_above_average` | Float64 | Caught-stealing above average. |
| `n_cs` | Int64 | Caught stealing (count). |
| `rate_cs` | Float64 | Rate cs. |
| `est_cs_pct` | Float64 | Expected caught stealing rate. |
| `cs_aa_per_throw` | Float64 | Cs aa per throw. |
| `seasonal_runner_speed` | Float64 | Seasonal runner speed. |
| `runner_distance_from_second` | Float64 | Runner distance from second. |
| `pop_time` | Float64 | Pop time. |
| `exchange_time` | Float64 | Exchange time. |
| `arm_strength` | Float64 | Arm strength (mph, top throws). |
| `n_xcs_with_flight_over_xcs` | Float64 | Number of xcs with flight over xcs. |
| `n_xcs_with_exchange_over_xcs` | Float64 | Number of xcs with exchange over xcs. |
| `n_xcs_with_accuracy_over_xcs` | Float64 | Number of xcs with accuracy over xcs. |
| `n_xcs_with_ground_other_over_xcs` | Float64 | Number of xcs with ground other over xcs. |
| `n_xcs_with_onfly_other_over_xcs` | Float64 | Number of xcs with onfly other over xcs. |
| `n_xcs_with_untracked_other_over_xcs` | Int64 | Number of xcs with untracked other over xcs. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_catcher_throwing-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_catcher_throwing-example}

```python
mlb_statcast_leaderboard_catcher_throwing()
```

## mlb_statcast_leaderboard_catcher_stance

GET /leaderboard/catcher-stance — catcher stance leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/catcher-stance`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/catcher-stance?csv=true](https://baseballsavant.mlb.com/leaderboard/catcher-stance?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_catcher_stance-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | MLBAM player id. |
| `name` | String | Player (or entity) name. |
| `year` | Int64 | Season year. |
| `pitches` | Int64 | Pitches. |
| `knee_down_pct` | Float64 | Share of pitches received in a knee-down stance. |
| `l_down_r_up_pct` | Float64 | L down r up rate. |
| `r_down_l_up_pct` | Float64 | R down l up rate. |
| `both_down_pct` | Float64 | Both down rate. |
| `both_up_pct` | Float64 | Both up rate. |
| `extended_leg_pct` | Float64 | Extended leg rate. |
| `inside_down_pct` | Float64 | Inside down rate. |
| `outside_down_pct` | Float64 | Outside down rate. |
| `one_knee_framing_rv` | Float64 | One knee framing rv. |
| `other_framing_rv` | Int64 | Other framing rv. |
| `one_knee_calledstr_pct` | Float64 | One knee calledstr rate. |
| `other_calledstr_pct` | String | Other calledstr rate. |
| `one_knee_blocking_rv` | Float64 | One knee blocking rv. |
| `other_blocking_rv` | Int64 | Other blocking rv. |
| `one_knee_pbwp100` | Float64 | One knee pbwp100. |
| `other_pbwp100` | String | Other pbwp100. |
| `one_knee_throwing_rv` | Int64 | One knee throwing rv. |
| `other_throwing_rv` | Int64 | Other throwing rv. |
| `one_knee_csaa100` | Int64 | One knee csaa100. |
| `other_csaa100` | String | Other csaa100. |
| `catching_rv` | Float64 | Catching rv. |
| `one_knee_pitching_rv` | Float64 | One knee pitching rv. |
| `other_pitching_rv` | Float64 | Other pitching rv. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_catcher_stance-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_catcher_stance-example}

```python
mlb_statcast_leaderboard_catcher_stance()
```

## mlb_statcast_leaderboard_basestealing_run_value

GET /leaderboard/basestealing-run-value — basestealing run-value leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/basestealing-run-value`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/basestealing-run-value?csv=true](https://baseballsavant.mlb.com/leaderboard/basestealing-run-value?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_basestealing_run_value-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | MLBAM player id. |
| `player_name` | String | Player name. |
| `team_name` | String | Team name. |
| `start_year` | Int64 | First season in the range. |
| `end_year` | Int64 | Last season in the range. |
| `key_target_base` | String | Key target base. |
| `runs_stolen_on_running_act` | Float64 | Runs stolen on running act. |
| `n_init` | Int64 | Number of init. |
| `rate_sbx` | Int64 | Rate sbx. |
| `n_sb` | Int64 | Stolen bases allowed (count). |
| `n_cs` | Int64 | Caught stealing (count). |
| `n_pk` | Int64 | Number of pk. |
| `n_bk` | Int64 | Number of bk. |
| `n_fb` | Int64 | Number of fb. |
| `n_plus` | Int64 | Number of plus. |
| `n_minus` | Int64 | Number of minus. |
| `net_act_plus` | Float64 | Net act plus. |
| `net_act_minus` | Float64 | Net act minus. |
| `r_primary_lead` | Float64 | Average primary lead distance (ft). |
| `r_secondary_lead` | Float64 | Average secondary lead (ft). |
| `r_sec_minus_prim_lead` | Float64 | R sec minus prim lead. |
| `r_primary_lead_sbx` | String | R primary lead sbx. |
| `r_secondary_lead_sbx` | String | R secondary lead sbx. |
| `r_sec_minus_prim_lead_sbx` | String | R sec minus prim lead sbx. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_basestealing_run_value-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_basestealing_run_value-example}

```python
mlb_statcast_leaderboard_basestealing_run_value()
```

## mlb_statcast_leaderboard_baserunning_run_value

GET /leaderboard/baserunning-run-value — baserunning run-value leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/baserunning-run-value`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/baserunning-run-value?csv=true](https://baseballsavant.mlb.com/leaderboard/baserunning-run-value?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_baserunning_run_value-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | MLBAM player id. |
| `entity_name` | String | Player (or team) entity name. |
| `team_name` | String | Team name. |
| `start_year` | Int64 | First season in the range. |
| `end_year` | Int64 | Last season in the range. |
| `runner_runs_tot` | Float64 | Runner runs tot. |
| `runner_runs_xb` | Float64 | Runner runs xb. |
| `runner_runs_sbx` | Float64 | Runner runs sbx. |
| `n_runner_moved` | Int64 | Number of runner moved. |
| `runner_runs_xb_swipe` | Float64 | Runner runs xb swipe. |
| `runner_runs_xb_snipe` | Int64 | Runner runs xb snipe. |
| `runner_runs_xb_freeze` | Float64 | Runner runs xb freeze. |
| `n_runner_moved_xb` | Int64 | Number of runner moved xb. |
| `runner_runs_sb2` | Float64 | Runner runs sb2. |
| `runner_runs_sb3` | Float64 | Runner runs sb3. |
| `simple_stolen_on_running_act_sb2` | Float64 | Simple stolen on running act sb2. |
| `simple_stolen_on_running_act_sb3` | Float64 | Simple stolen on running act sb3. |
| `n_runner_moved_sbx` | Int64 | Number of runner moved sbx. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_baserunning_run_value-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_baserunning_run_value-example}

```python
mlb_statcast_leaderboard_baserunning_run_value()
```

## mlb_statcast_leaderboard_baserunning

GET /leaderboard/baserunning — extra-bases-taken run-value leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/baserunning`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/baserunning?csv=true](https://baseballsavant.mlb.com/leaderboard/baserunning?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_baserunning-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `entity_name` | String | Player (or team) entity name. |
| `entity_id` | Int64 | MLBAM id of the player/team entity. |
| `team_name` | String | Team name. |
| `year` | Int64 | Season year. |
| `runner_runs` | Float64 | Baserunning run value as a runner. |
| `fielder_runs` | Float64 | Run value from the defense's perspective. |
| `runner_runs_advances` | Float64 | Runner runs advances. |
| `runner_runs_thrown_out` | Int64 | Runner runs thrown out. |
| `runner_runs_hold` | Float64 | Runner runs hold. |
| `fielder_runs_advances` | Float64 | Fielder runs advances. |
| `fielder_runs_thrown_out` | Int64 | Fielder runs thrown out. |
| `fielder_runs_hold` | Float64 | Fielder runs hold. |
| `n_opp_xb` | Int64 | Number of opp xb. |
| `n_att_xb` | Int64 | Number of att xb. |
| `rate_att_xb` | Float64 | Rate att xb. |
| `est_rate_att_generic_runner` | Float64 | Expected rate att generic runner. |
| `est_rate_att_generic_fielder` | Float64 | Expected rate att generic fielder. |
| `n_out` | Int64 | Number of out. |
| `n_safe` | Int64 | Number of safe. |
| `rate_safe` | Float64 | Rate safe. |
| `rate_safe_per_attempt` | Int64 | Rate safe per attempt. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_baserunning-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_baserunning-example}

```python
mlb_statcast_leaderboard_baserunning()
```

## mlb_statcast_leaderboard_year_to_year

GET /leaderboard/statcast-year-to-year — year-to-year metric change leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/statcast-year-to-year`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/statcast-year-to-year?csv=true](https://baseballsavant.mlb.com/leaderboard/statcast-year-to-year?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_year_to_year-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `name` | String | Player (or entity) name. |
| `entity_id` | Int64 | MLBAM id of the player/team entity. |
| `2015` | String | 2015. |
| `2016` | String | 2016. |
| `delta_2015_2016` | String | Delta 2015 2016. |
| `2017` | String | 2017. |
| `delta_2016_2017` | String | Delta 2016 2017. |
| `2018` | String | 2018. |
| `delta_2017_2018` | String | Delta 2017 2018. |
| `2019` | String | 2019. |
| `delta_2018_2019` | String | Delta 2018 2019. |
| `2020` | String | 2020. |
| `delta_2019_2020` | String | Delta 2019 2020. |
| `2021` | String | 2021. |
| `delta_2020_2021` | String | Delta 2020 2021. |
| `2022` | String | 2022. |
| `delta_2021_2022` | String | Delta 2021 2022. |
| `2023` | String | 2023. |
| `delta_2022_2023` | String | Delta 2022 2023. |
| `2024` | String | 2024. |
| `delta_2023_2024` | String | Delta 2023 2024. |
| `2025` | String | 2025. |
| `delta_2024_2025` | String | Delta 2024 2025. |
| `2026` | String | 2026. |
| `delta_2025_2026` | String | Delta 2025 2026. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_year_to_year-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_year_to_year-example}

```python
mlb_statcast_leaderboard_year_to_year()
```

## mlb_statcast_leaderboard_timer_infractions

GET /leaderboard/pitch-timer-infractions — pitch-timer infractions leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitch-timer-infractions`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitch-timer-infractions?csv=true](https://baseballsavant.mlb.com/leaderboard/pitch-timer-infractions?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_timer_infractions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `entity_id` | String | MLBAM id of the player/team entity. |
| `entity_name` | String | Player (or team) entity name. |
| `year` | String | Season year. |
| `pitches` | String | Pitches. |
| `all_violations` | String | Pitch-timer violations (total). |
| `pitcher_timer` | String | Pitcher timer. |
| `batter_timer` | String | Batter timer. |
| `batter_timeout` | String | Batter timeout. |
| `catcher_timer` | String | Catcher timer. |
| `defensive_shift` | String | Defensive shift. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_timer_infractions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_timer_infractions-example}

```python
mlb_statcast_leaderboard_timer_infractions()
```

## mlb_statcast_leaderboard_custom

GET /leaderboard/custom — build-your-own metric leaderboard (comma-separated selections).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/custom`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/custom?csv=true](https://baseballsavant.mlb.com/leaderboard/custom?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `selections` | `selections` |  |  | `Y` | selections query parameter. |
| `filter` | `filter` |  |  | `Y` | filter query parameter. |
| `min` | `min` |  |  | `Y` | min query parameter. |
| `sort` | `sort` |  |  | `Y` | sort query parameter. |
| `sortDir` | `sort_dir` |  |  | `Y` | sortDir query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_custom-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_name, first_name` | String | Last name, first name. |
| `player_id` | Int64 | MLBAM player id. |
| `year` | Int64 | Season year. |
| `xba` | Float64 | Expected batting average. |
| `xslg` | Float64 | Expected slugging. |
| `xwoba` | Float64 | Expected wOBA. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_custom-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_custom-example}

```python
mlb_statcast_leaderboard_custom()
```

## mlb_statcast_leaderboard_fielding_run_value

GET /leaderboard/fielding-run-value — fielding run-value leaderboard (HTML-embedded JSON).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/fielding-run-value`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/fielding-run-value](https://baseballsavant.mlb.com/leaderboard/fielding-run-value)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |

### Returns {#mlb_statcast_leaderboard_fielding_run_value-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `total_runs` | Float64 | Total runs. |
| `inf_of_runs` | String | Inf of runs. |
| `range_runs` | String | Range runs. |
| `arm_runs` | String | Arm runs. |
| `dp_runs` | String | Dp runs. |
| `catching_runs` | Float64 | Catching runs. |
| `framing_runs` | Float64 | Framing runs. |
| `throwing_runs` | Float64 | Throwing runs. |
| `blocking_runs` | Float64 | Blocking runs. |
| `outs_total` | Int64 | Outs total. |
| `tot_pa` | Int64 | Tot pa. |
| `outs_2` | Int64 | Outs 2. |
| `outs_3` | Int64 | Outs 3. |
| `outs_4` | Int64 | Outs 4. |
| `outs_5` | Int64 | Outs 5. |
| `outs_6` | Int64 | Outs 6. |
| `outs_7` | Int64 | Outs 7. |
| `outs_8` | Int64 | Outs 8. |
| `outs_9` | Int64 | Outs 9. |
| `id` | Int64 | MLBAM player id. |
| `name` | String | Player (or entity) name. |
| `team_id` | Int64 | MLBAM team id. |
| `n_teams` | Int64 | Number of teams. |
| `team_name` | String | Team name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_fielding_run_value-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_fielding_run_value-example}

```python
mlb_statcast_leaderboard_fielding_run_value()
```

## mlb_statcast_leaderboard_park_factors

GET /leaderboard/statcast-park-factors — Statcast park-factors leaderboard (HTML-embedded JSON).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/statcast-park-factors`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/statcast-park-factors](https://baseballsavant.mlb.com/leaderboard/statcast-park-factors)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |

### Returns {#mlb_statcast_leaderboard_park_factors-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `grouping_venue_conditions` | String | Grouping venue conditions. |
| `key_is_year_rolling` | Int64 | Key is year rolling. |
| `key_num_years_rolling` | Int64 | Key num years rolling. |
| `key_year` | Int64 | Key year. |
| `key_bat_side` | String | Key bat side. |
| `venue_id` | Int64 | Venue id. |
| `venue_name` | String | Ballpark name. |
| `main_team_id` | Int64 | Main team id. |
| `name_display_club` | String | Club name. |
| `n_pa` | Int64 | Number of plate appearances. |
| `index_runs` | Int64 | Index runs. |
| `index_hardhit` | Int64 | Index hardhit. |
| `index_woba` | Int64 | Park factor index for wOBA (100 = neutral). |
| `index_wobatto` | Int64 | Index wobatto. |
| `index_wobacon` | Int64 | Index wobacon. |
| `index_xwobacon` | Int64 | Index xwobacon. |
| `index_xbacon` | Int64 | Index xbacon. |
| `index_obp` | Int64 | Index obp. |
| `index_so` | Int64 | Index so. |
| `index_bb` | Int64 | Index bb. |
| `index_bacon` | Int64 | Index bacon. |
| `index_hits` | Int64 | Index hits. |
| `index_1b` | Int64 | Index 1b. |
| `index_2b` | Int64 | Index 2b. |
| `index_3b` | Int64 | Index 3b. |
| `index_hr` | Int64 | Index hr. |
| `year_range` | String | Year range. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_park_factors-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_park_factors-example}

```python
mlb_statcast_leaderboard_park_factors()
```
