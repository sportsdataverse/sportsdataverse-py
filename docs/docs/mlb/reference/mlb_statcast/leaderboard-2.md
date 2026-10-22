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
| `player_id` | integer | MLBAM player id. |
| `player_name` | character | Player name. |
| `team_name` | character | Team name. |
| `start_year` | integer | First season in the range. |
| `end_year` | integer | Last season in the range. |
| `sb_attempts` | integer | Sb attempts. |
| `catcher_stealing_runs` | numeric | Catcher stealing runs. |
| `caught_stealing_above_average` | numeric | Caught-stealing above average. |
| `n_cs` | integer | Caught stealing (count). |
| `rate_cs` | numeric | Rate cs. |
| `est_cs_pct` | numeric | Expected caught stealing rate. |
| `cs_aa_per_throw` | numeric | Cs aa per throw. |
| `seasonal_runner_speed` | numeric | Seasonal runner speed. |
| `runner_distance_from_second` | numeric | Runner distance from second. |
| `pop_time` | numeric | Pop time. |
| `exchange_time` | numeric | Exchange time. |
| `arm_strength` | numeric | Arm strength (mph, top throws). |
| `n_xcs_with_flight_over_xcs` | numeric | Number of xcs with flight over xcs. |
| `n_xcs_with_exchange_over_xcs` | numeric | Number of xcs with exchange over xcs. |
| `n_xcs_with_accuracy_over_xcs` | numeric | Number of xcs with accuracy over xcs. |
| `n_xcs_with_ground_other_over_xcs` | numeric | Number of xcs with ground other over xcs. |
| `n_xcs_with_onfly_other_over_xcs` | numeric | Number of xcs with onfly other over xcs. |
| `n_xcs_with_untracked_other_over_xcs` | integer | Number of xcs with untracked other over xcs. |

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
| `id` | integer | MLBAM player id. |
| `name` | character | Player (or entity) name. |
| `year` | integer | Season year. |
| `pitches` | integer | Pitches. |
| `knee_down_pct` | numeric | Share of pitches received in a knee-down stance. |
| `l_down_r_up_pct` | numeric | L down r up rate. |
| `r_down_l_up_pct` | numeric | R down l up rate. |
| `both_down_pct` | numeric | Both down rate. |
| `both_up_pct` | numeric | Both up rate. |
| `extended_leg_pct` | numeric | Extended leg rate. |
| `inside_down_pct` | numeric | Inside down rate. |
| `outside_down_pct` | numeric | Outside down rate. |
| `one_knee_framing_rv` | numeric | One knee framing rv. |
| `other_framing_rv` | integer | Other framing rv. |
| `one_knee_calledstr_pct` | numeric | One knee calledstr rate. |
| `other_calledstr_pct` | character | Other calledstr rate. |
| `one_knee_blocking_rv` | numeric | One knee blocking rv. |
| `other_blocking_rv` | integer | Other blocking rv. |
| `one_knee_pbwp100` | numeric | One knee pbwp100. |
| `other_pbwp100` | character | Other pbwp100. |
| `one_knee_throwing_rv` | integer | One knee throwing rv. |
| `other_throwing_rv` | integer | Other throwing rv. |
| `one_knee_csaa100` | integer | One knee csaa100. |
| `other_csaa100` | character | Other csaa100. |
| `catching_rv` | numeric | Catching rv. |
| `one_knee_pitching_rv` | numeric | One knee pitching rv. |
| `other_pitching_rv` | numeric | Other pitching rv. |

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
| `player_id` | integer | MLBAM player id. |
| `player_name` | character | Player name. |
| `team_name` | character | Team name. |
| `start_year` | integer | First season in the range. |
| `end_year` | integer | Last season in the range. |
| `key_target_base` | character | Key target base. |
| `runs_stolen_on_running_act` | numeric | Runs stolen on running act. |
| `n_init` | integer | Number of init. |
| `rate_sbx` | integer | Rate sbx. |
| `n_sb` | integer | Stolen bases allowed (count). |
| `n_cs` | integer | Caught stealing (count). |
| `n_pk` | integer | Number of pk. |
| `n_bk` | integer | Number of bk. |
| `n_fb` | integer | Number of fb. |
| `n_plus` | integer | Number of plus. |
| `n_minus` | integer | Number of minus. |
| `net_act_plus` | numeric | Net act plus. |
| `net_act_minus` | numeric | Net act minus. |
| `r_primary_lead` | numeric | Average primary lead distance (ft). |
| `r_secondary_lead` | numeric | Average secondary lead (ft). |
| `r_sec_minus_prim_lead` | numeric | R sec minus prim lead. |
| `r_primary_lead_sbx` | character | R primary lead sbx. |
| `r_secondary_lead_sbx` | character | R secondary lead sbx. |
| `r_sec_minus_prim_lead_sbx` | character | R sec minus prim lead sbx. |

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
| `player_id` | integer | MLBAM player id. |
| `entity_name` | character | Player (or team) entity name. |
| `team_name` | character | Team name. |
| `start_year` | integer | First season in the range. |
| `end_year` | integer | Last season in the range. |
| `runner_runs_tot` | numeric | Runner runs tot. |
| `runner_runs_xb` | numeric | Runner runs xb. |
| `runner_runs_sbx` | numeric | Runner runs sbx. |
| `n_runner_moved` | integer | Number of runner moved. |
| `runner_runs_xb_swipe` | numeric | Runner runs xb swipe. |
| `runner_runs_xb_snipe` | integer | Runner runs xb snipe. |
| `runner_runs_xb_freeze` | numeric | Runner runs xb freeze. |
| `n_runner_moved_xb` | integer | Number of runner moved xb. |
| `runner_runs_sb2` | numeric | Runner runs sb2. |
| `runner_runs_sb3` | numeric | Runner runs sb3. |
| `simple_stolen_on_running_act_sb2` | numeric | Simple stolen on running act sb2. |
| `simple_stolen_on_running_act_sb3` | numeric | Simple stolen on running act sb3. |
| `n_runner_moved_sbx` | integer | Number of runner moved sbx. |

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
| `entity_name` | character | Player (or team) entity name. |
| `entity_id` | integer | MLBAM id of the player/team entity. |
| `team_name` | character | Team name. |
| `year` | integer | Season year. |
| `runner_runs` | numeric | Baserunning run value as a runner. |
| `fielder_runs` | numeric | Run value from the defense's perspective. |
| `runner_runs_advances` | numeric | Runner runs advances. |
| `runner_runs_thrown_out` | integer | Runner runs thrown out. |
| `runner_runs_hold` | numeric | Runner runs hold. |
| `fielder_runs_advances` | numeric | Fielder runs advances. |
| `fielder_runs_thrown_out` | integer | Fielder runs thrown out. |
| `fielder_runs_hold` | numeric | Fielder runs hold. |
| `n_opp_xb` | integer | Number of opp xb. |
| `n_att_xb` | integer | Number of att xb. |
| `rate_att_xb` | numeric | Rate att xb. |
| `est_rate_att_generic_runner` | numeric | Expected rate att generic runner. |
| `est_rate_att_generic_fielder` | numeric | Expected rate att generic fielder. |
| `n_out` | integer | Number of out. |
| `n_safe` | integer | Number of safe. |
| `rate_safe` | numeric | Rate safe. |
| `rate_safe_per_attempt` | integer | Rate safe per attempt. |

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
| `name` | character | Player (or entity) name. |
| `entity_id` | integer | MLBAM id of the player/team entity. |
| `2015` | character | 2015. |
| `2016` | character | 2016. |
| `delta_2015_2016` | character | Delta 2015 2016. |
| `2017` | character | 2017. |
| `delta_2016_2017` | character | Delta 2016 2017. |
| `2018` | character | 2018. |
| `delta_2017_2018` | character | Delta 2017 2018. |
| `2019` | character | 2019. |
| `delta_2018_2019` | character | Delta 2018 2019. |
| `2020` | character | 2020. |
| `delta_2019_2020` | character | Delta 2019 2020. |
| `2021` | character | 2021. |
| `delta_2020_2021` | character | Delta 2020 2021. |
| `2022` | character | 2022. |
| `delta_2021_2022` | character | Delta 2021 2022. |
| `2023` | character | 2023. |
| `delta_2022_2023` | character | Delta 2022 2023. |
| `2024` | character | 2024. |
| `delta_2023_2024` | character | Delta 2023 2024. |
| `2025` | character | 2025. |
| `delta_2024_2025` | character | Delta 2024 2025. |
| `2026` | character | 2026. |
| `delta_2025_2026` | character | Delta 2025 2026. |

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
| `entity_id` | character | MLBAM id of the player/team entity. |
| `entity_name` | character | Player (or team) entity name. |
| `year` | character | Season year. |
| `pitches` | character | Pitches. |
| `all_violations` | character | Pitch-timer violations (total). |
| `pitcher_timer` | character | Pitcher timer. |
| `batter_timer` | character | Batter timer. |
| `batter_timeout` | character | Batter timeout. |
| `catcher_timer` | character | Catcher timer. |
| `defensive_shift` | character | Defensive shift. |

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
| `last_name, first_name` | character | Last name, first name. |
| `player_id` | integer | MLBAM player id. |
| `year` | integer | Season year. |
| `xba` | numeric | Expected batting average. |
| `xslg` | numeric | Expected slugging. |
| `xwoba` | numeric | Expected wOBA. |

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
| `total_runs` | numeric | Total runs. |
| `inf_of_runs` | character | Inf of runs. |
| `range_runs` | character | Range runs. |
| `arm_runs` | character | Arm runs. |
| `dp_runs` | character | Dp runs. |
| `catching_runs` | numeric | Catching runs. |
| `framing_runs` | numeric | Framing runs. |
| `throwing_runs` | numeric | Throwing runs. |
| `blocking_runs` | numeric | Blocking runs. |
| `outs_total` | integer | Outs total. |
| `tot_pa` | integer | Tot pa. |
| `outs_2` | integer | Outs 2. |
| `outs_3` | integer | Outs 3. |
| `outs_4` | integer | Outs 4. |
| `outs_5` | integer | Outs 5. |
| `outs_6` | integer | Outs 6. |
| `outs_7` | integer | Outs 7. |
| `outs_8` | integer | Outs 8. |
| `outs_9` | integer | Outs 9. |
| `id` | integer | MLBAM player id. |
| `name` | character | Player (or entity) name. |
| `team_id` | integer | MLBAM team id. |
| `n_teams` | integer | Number of teams. |
| `team_name` | character | Team name. |

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
| `grouping_venue_conditions` | character | Grouping venue conditions. |
| `key_is_year_rolling` | integer | Key is year rolling. |
| `key_num_years_rolling` | integer | Key num years rolling. |
| `key_year` | integer | Key year. |
| `key_bat_side` | character | Key bat side. |
| `venue_id` | integer | Venue id. |
| `venue_name` | character | Ballpark name. |
| `main_team_id` | integer | Main team id. |
| `name_display_club` | character | Club name. |
| `n_pa` | integer | Number of plate appearances. |
| `index_runs` | integer | Index runs. |
| `index_hardhit` | integer | Index hardhit. |
| `index_woba` | integer | Park factor index for wOBA (100 = neutral). |
| `index_wobatto` | integer | Index wobatto. |
| `index_wobacon` | integer | Index wobacon. |
| `index_xwobacon` | integer | Index xwobacon. |
| `index_xbacon` | integer | Index xbacon. |
| `index_obp` | integer | Index obp. |
| `index_so` | integer | Index so. |
| `index_bb` | integer | Index bb. |
| `index_bacon` | integer | Index bacon. |
| `index_hits` | integer | Index hits. |
| `index_1b` | integer | Index 1b. |
| `index_2b` | integer | Index 2b. |
| `index_3b` | integer | Index 3b. |
| `index_hr` | integer | Index hr. |
| `year_range` | character | Year range. |

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
