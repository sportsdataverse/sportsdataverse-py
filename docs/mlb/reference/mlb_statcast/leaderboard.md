# MLB — MLB Statcast (Baseball Savant) — Leaderboard: expected–catcher

> MLB — MLB Statcast (Baseball Savant) — Leaderboard: expected–catcher — function reference in sdv-py, the SportsDataverse Python package.

## mlb_statcast_leaderboard_expected_stats

GET /leaderboard/expected_statistics — xBA/xSLG/xwOBA/xISO expected-statistics leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/expected_statistics`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/expected_statistics?csv=true](https://baseballsavant.mlb.com/leaderboard/expected_statistics?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_expected_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_name, first_name` | String | Last name, first name. |
| `player_id` | Int64 | MLBAM player id. |
| `year` | Int64 | Season year. |
| `pa` | Int64 | Plate appearances. |
| `bip` | Int64 | Balls in play. |
| `ba` | Float64 | Batting average. |
| `est_ba` | Float64 | Expected batting average (xBA). |
| `est_ba_minus_ba_diff` | Float64 | xBA minus actual BA (over/under-performance). |
| `slg` | Float64 | Slugging percentage. |
| `est_slg` | Float64 | Expected slugging (xSLG). |
| `est_slg_minus_slg_diff` | Float64 | xSLG minus actual SLG. |
| `woba` | Float64 | Weighted on-base average. |
| `est_woba` | Float64 | Expected wOBA (xwOBA). |
| `est_woba_minus_woba_diff` | Float64 | xwOBA minus actual wOBA. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_expected_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_expected_stats-example}

```python
mlb_statcast_leaderboard_expected_stats()
```

## mlb_statcast_leaderboard_percentile_rankings

GET /leaderboard/percentile-rankings — player percentile-ranking sliders (xwOBA/xBA/xSLG/…).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/percentile-rankings`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/percentile-rankings?csv=true](https://baseballsavant.mlb.com/leaderboard/percentile-rankings?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_percentile_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_name` | String | Player name. |
| `player_id` | Int64 | MLBAM player id. |
| `year` | Int64 | Season year. |
| `xwoba` | String | Expected wOBA. |
| `xba` | String | Expected batting average. |
| `xslg` | String | Expected slugging. |
| `xiso` | String | Expected isolated power. |
| `xobp` | String | Expected on-base percentage. |
| `brl` | String | Barrels. |
| `brl_percent` | String | Barrel rate (% of batted balls). |
| `exit_velocity` | String | Exit velocity (mph). |
| `max_ev` | Int64 | Max ev. |
| `hard_hit_percent` | String | Hard-hit rate (95+ mph EV). |
| `k_percent` | String | Strikeout rate. |
| `bb_percent` | String | Walk rate. |
| `whiff_percent` | String | Whiff rate (swings and misses / swings). |
| `chase_percent` | String | Chase rate. |
| `arm_strength` | Int64 | Arm strength (mph, top throws). |
| `sprint_speed` | Int64 | Sprint speed (ft/sec, top 50% of competitive runs). |
| `oaa` | Int64 | Outs Above Average. |
| `bat_speed` | String | Bat speed (mph). |
| `squared_up_rate` | String | Squared up rate. |
| `swing_length` | String | Swing length (ft, head travel). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_percentile_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_percentile_rankings-example}

```python
mlb_statcast_leaderboard_percentile_rankings()
```

## mlb_statcast_leaderboard_sprint_speed

GET /leaderboard/sprint_speed — sprint-speed (ft/sec) leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/sprint_speed`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/sprint_speed?csv=true](https://baseballsavant.mlb.com/leaderboard/sprint_speed?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_sprint_speed-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_name, first_name` | String | Last name, first name. |
| `player_id` | Int64 | MLBAM player id. |
| `team_id` | Int64 | MLBAM team id. |
| `team` | String | Team abbreviation. |
| `position` | String | Position. |
| `age` | Int64 | Player age. |
| `competitive_runs` | Int64 | Competitive runs (qualifying sprint-speed runs). |
| `bolts` | Int64 | Bolts. |
| `hp_to_1b` | Float64 | Home-to-first time (s). |
| `sprint_speed` | Float64 | Sprint speed (ft/sec, top 50% of competitive runs). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_sprint_speed-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_sprint_speed-example}

```python
mlb_statcast_leaderboard_sprint_speed()
```

## mlb_statcast_leaderboard_running_splits

GET /leaderboard/running_splits — 90-foot running splits leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/running_splits`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/running_splits?csv=true](https://baseballsavant.mlb.com/leaderboard/running_splits?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_running_splits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_name, first_name` | String | Last name, first name. |
| `player_id` | Int64 | MLBAM player id. |
| `name_abbrev` | String | Team/name abbreviation. |
| `team_id` | Int64 | MLBAM team id. |
| `position_name` | String | Position name. |
| `age` | Int64 | Player age. |
| `bat_side` | String | Batter side (R/L/S). |
| `seconds_since_hit_000` | Float64 | Seconds since hit 000. |
| `seconds_since_hit_005` | Float64 | Seconds since hit 005. |
| `seconds_since_hit_010` | Float64 | Seconds since hit 010. |
| `seconds_since_hit_015` | Float64 | Seconds since hit 015. |
| `seconds_since_hit_020` | Float64 | Seconds since hit 020. |
| `seconds_since_hit_025` | Float64 | Seconds since hit 025. |
| `seconds_since_hit_030` | Float64 | Seconds since hit 030. |
| `seconds_since_hit_035` | Float64 | Seconds since hit 035. |
| `seconds_since_hit_040` | Float64 | Seconds since hit 040. |
| `seconds_since_hit_045` | Float64 | Seconds since hit 045. |
| `seconds_since_hit_050` | Float64 | Seconds since hit 050. |
| `seconds_since_hit_055` | Float64 | Seconds since hit 055. |
| `seconds_since_hit_060` | Float64 | Seconds since hit 060. |
| `seconds_since_hit_065` | Float64 | Seconds since hit 065. |
| `seconds_since_hit_070` | Float64 | Seconds since hit 070. |
| `seconds_since_hit_075` | Float64 | Seconds since hit 075. |
| `seconds_since_hit_080` | Float64 | Seconds since hit 080. |
| `seconds_since_hit_085` | Float64 | Seconds since hit 085. |
| `seconds_since_hit_090` | Float64 | Seconds since hit 090. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_running_splits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_running_splits-example}

```python
mlb_statcast_leaderboard_running_splits()
```

## mlb_statcast_leaderboard_bat_tracking

GET /leaderboard/bat-tracking — bat-tracking (swing speed / squared-up) leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/bat-tracking`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/bat-tracking?csv=true](https://baseballsavant.mlb.com/leaderboard/bat-tracking?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_bat_tracking-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | MLBAM player id. |
| `name` | String | Player (or entity) name. |
| `swings_competitive` | Int64 | Competitive swings. |
| `percent_swings_competitive` | Float64 | Share of swings that are competitive. |
| `contact` | Int64 | Contact. |
| `avg_bat_speed` | Float64 | Average bat speed (mph). |
| `hard_swing_rate` | Float64 | Hard swing rate. |
| `squared_up_per_bat_contact` | Float64 | Squared up per bat contact. |
| `squared_up_per_swing` | Float64 | Squared-up rate per swing. |
| `blast_per_bat_contact` | Float64 | Blast per bat contact. |
| `blast_per_swing` | Float64 | Blasts per swing. |
| `swing_length` | Float64 | Swing length (ft, head travel). |
| `swords` | Int64 | Swords. |
| `batter_run_value` | Float64 | Batter run value. |
| `whiffs` | String | Whiffs. |
| `whiff_per_swing` | String | Whiff per swing. |
| `batted_ball_events` | Int64 | Batted ball events. |
| `batted_ball_event_per_swing` | Float64 | Batted ball event per swing. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_bat_tracking-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_bat_tracking-example}

```python
mlb_statcast_leaderboard_bat_tracking()
```

## mlb_statcast_leaderboard_swing_path

GET /leaderboard/bat-tracking/swing-path-attack-angle — swing path & attack-angle leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/bat-tracking/swing-path-attack-angle`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/bat-tracking/swing-path-attack-angle?csv=true](https://baseballsavant.mlb.com/leaderboard/bat-tracking/swing-path-attack-angle?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_swing_path-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | MLBAM player id. |
| `name` | String | Player (or entity) name. |
| `side` | String | Side. |
| `avg_bat_speed` | Float64 | Average bat speed (mph). |
| `swing_tilt` | Float64 | Swing tilt (deg). |
| `attack_angle` | Float64 | Attack angle (deg, bat path at contact). |
| `attack_direction` | Float64 | Attack direction (deg, pull/oppo). |
| `ideal_attack_angle_rate` | Float64 | Rate of swings in the ideal attack-angle window. |
| `avg_intercept_y_vs_plate` | Float64 | Avg intercept y vs plate. |
| `avg_intercept_y_vs_batter` | Float64 | Avg intercept y vs batter. |
| `avg_batter_y_position` | Float64 | Avg batter y position. |
| `avg_batter_x_position` | Float64 | Avg batter x position. |
| `competitive_swings` | Int64 | Competitive swings. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_swing_path-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_swing_path-example}

```python
mlb_statcast_leaderboard_swing_path()
```

## mlb_statcast_leaderboard_swing_timing

GET /leaderboard/bat-tracking/swing-timing-miss-distance — swing timing & miss-distance leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/bat-tracking/swing-timing-miss-distance`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/bat-tracking/swing-timing-miss-distance?csv=true](https://baseballsavant.mlb.com/leaderboard/bat-tracking/swing-timing-miss-distance?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_swing_timing-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | MLBAM player id. |
| `name` | String | Player (or entity) name. |
| `year` | Int64 | Season year. |
| `team_name` | String | Team name. |
| `bat_side_formatted` | String | Batter side (formatted). |
| `miss_distance` | Float64 | Average miss distance (in) on swings. |
| `flawed_percent` | Float64 | Flawed rate. |
| `perfect_percent` | Float64 | Perfect rate. |
| `tied_up_percent` | Float64 | Tied up rate. |
| `avg_x_tied_up` | Float64 | Avg x tied up. |
| `centered_percent` | Float64 | Centered rate. |
| `flailed_percent` | Float64 | Flailed rate. |
| `avg_x_flail` | Float64 | Avg x flail. |
| `early_percent` | Float64 | Early rate. |
| `avg_y_early` | Float64 | Avg y early. |
| `on_time_percent` | Float64 | On time rate. |
| `late_percent` | Float64 | Late rate. |
| `avg_y_late` | Float64 | Avg y late. |
| `n_swings` | Int64 | Number of swings. |
| `whiff_rate` | Float64 | Whiff rate. |
| `competitive_percent` | Float64 | Competitive rate. |
| `over_percent` | Float64 | Over rate. |
| `avg_z_over` | Float64 | Avg z over. |
| `lined_up_percent` | Float64 | Lined up rate. |
| `under_percent` | Float64 | Under rate. |
| `avg_z_under` | Float64 | Avg z under. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_swing_timing-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_swing_timing-example}

```python
mlb_statcast_leaderboard_swing_timing()
```

## mlb_statcast_leaderboard_swing_take

GET /leaderboard/swing-take — swing/take run-value leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/swing-take`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/swing-take?csv=true](https://baseballsavant.mlb.com/leaderboard/swing-take?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_swing_take-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `year` | String | Season year. |
| `last_name, first_name` | String | Last name, first name. |
| `player_id` | String | MLBAM player id. |
| `team_id` | String | MLBAM team id. |
| `pa` | String | Plate appearances. |
| `pitches` | String | Pitches. |
| `runs_all` | String | Runs all. |
| `runs_heart` | String | Runs heart. |
| `runs_shadow` | String | Runs shadow. |
| `runs_chase` | String | Runs chase. |
| `runs_waste` | String | Runs waste. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_swing_take-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_swing_take-example}

```python
mlb_statcast_leaderboard_swing_take()
```

## mlb_statcast_leaderboard_exit_velocity_barrels

GET /leaderboard/statcast — exit velocity & barrels leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/statcast`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/statcast?csv=true](https://baseballsavant.mlb.com/leaderboard/statcast?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_exit_velocity_barrels-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_name, first_name` | String | Last name, first name. |
| `player_id` | Int64 | MLBAM player id. |
| `attempts` | Int64 | Opportunities/attempts. |
| `avg_hit_angle` | Float64 | Average launch angle (deg). |
| `anglesweetspotpercent` | Float64 | Anglesweetspotpercent. |
| `max_hit_speed` | Float64 | Max exit velocity (mph). |
| `avg_hit_speed` | Float64 | Average exit velocity (mph). |
| `ev50` | Float64 | Ev50. |
| `fbld` | Float64 | Fbld. |
| `gb` | Float64 | Gb. |
| `max_distance` | Int64 | Max distance. |
| `avg_distance` | Int64 | Avg distance. |
| `avg_hr_distance` | Int64 | Avg hr distance. |
| `ev95plus` | Int64 | Ev95plus. |
| `ev95percent` | Float64 | Ev95percent. |
| `barrels` | Int64 | Barrels. |
| `brl_percent` | Float64 | Barrel rate (% of batted balls). |
| `brl_pa` | Float64 | Barrels per plate appearance. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_exit_velocity_barrels-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_exit_velocity_barrels-example}

```python
mlb_statcast_leaderboard_exit_velocity_barrels()
```

## mlb_statcast_leaderboard_batted_ball

GET /leaderboard/batted-ball — batted-ball profile leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/batted-ball`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/batted-ball?csv=true](https://baseballsavant.mlb.com/leaderboard/batted-ball?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_batted_ball-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | MLBAM player id. |
| `name` | String | Player (or entity) name. |
| `year` | Int64 | Season year. |
| `bbe` | Int64 | Batted-ball events. |
| `gb_rate` | Float64 | Gb rate. |
| `air_rate` | Float64 | Air rate. |
| `fb_rate` | Float64 | Fb rate. |
| `ld_rate` | Float64 | Ld rate. |
| `pu_rate` | Float64 | Pu rate. |
| `pull_rate` | Float64 | Pull rate. |
| `straight_rate` | Float64 | Straight rate. |
| `oppo_rate` | Float64 | Oppo rate. |
| `pull_gb_rate` | Float64 | Pull gb rate. |
| `straight_gb_rate` | Float64 | Straight gb rate. |
| `oppo_gb_rate` | Float64 | Oppo gb rate. |
| `pull_air_rate` | Float64 | Pull air rate. |
| `straight_air_rate` | Float64 | Straight air rate. |
| `oppo_air_rate` | Float64 | Oppo air rate. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_batted_ball-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_batted_ball-example}

```python
mlb_statcast_leaderboard_batted_ball()
```

## mlb_statcast_leaderboard_home_runs

GET /leaderboard/home-runs — Statcast home-runs leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/home-runs`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/home-runs?csv=true](https://baseballsavant.mlb.com/leaderboard/home-runs?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_home_runs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player` | String | Player. |
| `player_id` | Int64 | MLBAM player id. |
| `team_abbrev` | String | Team abbreviation. |
| `year` | Int64 | Season year. |
| `type` | String | Record/pitch type. |
| `avg_hr_trot` | Float64 | Avg hr trot. |
| `doubters` | Int64 | Doubters. |
| `mostly_gone` | Int64 | Mostly gone. |
| `no_doubters` | Int64 | No doubters. |
| `no_doubter_per` | Float64 | No doubter per. |
| `hr_total` | Int64 | Hr total. |
| `xhr` | Float64 | Xhr. |
| `xhr_diff` | Float64 | Xhr diff. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_home_runs-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_home_runs-example}

```python
mlb_statcast_leaderboard_home_runs()
```

## mlb_statcast_leaderboard_pitch_arsenals

GET /leaderboard/pitch-arsenals — pitch arsenals (velo/spin/movement) leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitch-arsenals`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitch-arsenals?csv=true](https://baseballsavant.mlb.com/leaderboard/pitch-arsenals?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_pitch_arsenals-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_name, first_name` | String | Last name, first name. |
| `pitcher` | Int64 | MLBAM id of the pitcher. |
| `ff_batter` | String | Ff batter. |
| `si_batter` | String | Si batter. |
| `fc_batter` | String | Fc batter. |
| `sl_batter` | String | Sl batter. |
| `ch_batter` | String | Ch batter. |
| `cu_batter` | String | Cu batter. |
| `fs_batter` | String | Fs batter. |
| `kn_batter` | String | Kn batter. |
| `st_batter` | String | St batter. |
| `sv_batter` | String | Sv batter. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_pitch_arsenals-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_pitch_arsenals-example}

```python
mlb_statcast_leaderboard_pitch_arsenals()
```

## mlb_statcast_leaderboard_pitch_arsenal_stats

GET /leaderboard/pitch-arsenal-stats — per-pitch-type outcome stats leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats?csv=true](https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_pitch_arsenal_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_name, first_name` | String | Last name, first name. |
| `player_id` | Int64 | MLBAM player id. |
| `team_name_alt` | String | Team name (alternate form). |
| `pitch_type` | String | Pitch type code. |
| `pitch_name` | String | Pitch type name. |
| `run_value_per_100` | Float64 | Run value per 100 pitches. |
| `run_value` | Int64 | Run value (runs). |
| `pitches` | Int64 | Pitches. |
| `pitch_usage` | Float64 | Pitch usage. |
| `pa` | Int64 | Plate appearances. |
| `ba` | Float64 | Batting average. |
| `slg` | Float64 | Slugging percentage. |
| `woba` | Float64 | Weighted on-base average. |
| `whiff_percent` | Float64 | Whiff rate (swings and misses / swings). |
| `k_percent` | Float64 | Strikeout rate. |
| `put_away` | Float64 | Put away. |
| `est_ba` | Float64 | Expected batting average (xBA). |
| `est_slg` | Float64 | Expected slugging (xSLG). |
| `est_woba` | Float64 | Expected wOBA (xwOBA). |
| `hard_hit_percent` | Float64 | Hard-hit rate (95+ mph EV). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_pitch_arsenal_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_pitch_arsenal_stats-example}

```python
mlb_statcast_leaderboard_pitch_arsenal_stats()
```

## mlb_statcast_leaderboard_pitch_movement

GET /leaderboard/pitch-movement — pitch-movement leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitch-movement`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitch-movement?csv=true](https://baseballsavant.mlb.com/leaderboard/pitch-movement?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_pitch_movement-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `year` | Int64 | Season year. |
| `last_name, first_name` | String | Last name, first name. |
| `pitcher_id` | Int64 | MLBAM id of the pitcher. |
| `team_name` | String | Team name. |
| `team_name_abbrev` | String | Team name abbrev. |
| `pitch_hand` | String | Pitcher handedness (R/L). |
| `avg_speed` | Int64 | Average pitch velocity (mph). |
| `pitches_thrown` | Int64 | Pitches thrown. |
| `total_pitches` | Int64 | Total pitches. |
| `pitches_per_game` | Float64 | Pitches per game. |
| `pitch_per` | Float64 | Pitch per. |
| `pitch_type` | String | Pitch type code. |
| `pitch_type_name` | String | Pitch type name. |
| `pitcher_break_z` | Float64 | Pitcher break z. |
| `league_break_z` | Float64 | League break z. |
| `diff_z` | Float64 | Diff z. |
| `rise` | Int64 | Rise. |
| `pitcher_break_z_induced` | Float64 | Pitcher break z induced. |
| `pitcher_break_x` | Float64 | Pitcher break x. |
| `league_break_x` | Float64 | League break x. |
| `diff_x` | Float64 | Diff x. |
| `tail` | Int64 | Tail. |
| `percent_rank_diff_z` | Float64 | Percent rank diff z. |
| `percent_rank_diff_x` | Float64 | Percent rank diff x. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_pitch_movement-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_pitch_movement-example}

```python
mlb_statcast_leaderboard_pitch_movement()
```

## mlb_statcast_leaderboard_pitch_tempo

GET /leaderboard/pitch-tempo — pitch-tempo leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitch-tempo`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitch-tempo?csv=true](https://baseballsavant.mlb.com/leaderboard/pitch-tempo?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_pitch_tempo-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `entity_id` | Int64 | MLBAM id of the player/team entity. |
| `entity_name` | String | Player (or team) entity name. |
| `entity_code` | String | Entity code. |
| `team_id` | Int64 | MLBAM team id. |
| `total_pitches` | Int64 | Total pitches. |
| `total_pitches_empty` | Int64 | Total pitches empty. |
| `median_seconds_empty` | Float64 | Median tempo (s) with bases empty. |
| `total_pitches_onbase` | Int64 | Total pitches onbase. |
| `freq_hot` | Float64 | Freq hot. |
| `freq_warm` | Float64 | Freq warm. |
| `freq_cold` | Float64 | Freq cold. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_pitch_tempo-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_pitch_tempo-example}

```python
mlb_statcast_leaderboard_pitch_tempo()
```

## mlb_statcast_leaderboard_active_spin

GET /leaderboard/active-spin — active-spin leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/active-spin`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/active-spin?csv=true](https://baseballsavant.mlb.com/leaderboard/active-spin?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_active_spin-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `entity_name` | String | Player (or team) entity name. |
| `entity_id` | Int64 | MLBAM id of the player/team entity. |
| `pitch_hand` | String | Pitcher handedness (R/L). |
| `active_spin_fourseam` | String | Active spin fourseam. |
| `active_spin_sinker` | Float64 | Active spin sinker. |
| `active_spin_cutter` | Float64 | Active spin cutter. |
| `active_spin_changeup` | Float64 | Active spin changeup. |
| `active_spin_splitter` | String | Active spin splitter. |
| `active_spin_curve` | String | Active spin curve. |
| `active_spin_slider` | Float64 | Active spin slider. |
| `active_spin_sweeper` | Float64 | Active spin sweeper. |
| `active_spin_slurve` | String | Active spin slurve. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_active_spin-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_active_spin-example}

```python
mlb_statcast_leaderboard_active_spin()
```

## mlb_statcast_leaderboard_spin_direction

GET /leaderboard/spin-direction-pitches — spin-direction (per-pitch) leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/spin-direction-pitches`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/spin-direction-pitches?csv=true](https://baseballsavant.mlb.com/leaderboard/spin-direction-pitches?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_spin_direction-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `year` | Int64 | Season year. |
| `last_name, first_name` | String | Last name, first name. |
| `player_id` | Int64 | MLBAM player id. |
| `pitch_hand` | String | Pitcher handedness (R/L). |
| `api_pitch_type` | String | Pitch type (API code). |
| `n_pitches` | Int64 | Number of pitches. |
| `release_speed` | Float64 | Release speed. |
| `spin_rate` | Int64 | Spin rate (rpm). |
| `movement_inches` | Float64 | Movement inches. |
| `alan_active_spin_pct` | Float64 | Alan active spin rate. |
| `active_spin` | Float64 | Active (useful) spin (%). |
| `hawkeye_measured` | Float64 | Hawkeye measured. |
| `movement_inferred` | Float64 | Movement inferred. |
| `api_pitch_name` | String | Api pitch name. |
| `active_spin_formatted` | Int64 | Active spin (formatted, %). |
| `hawkeye_measured_clock_minutes` | Int64 | Hawkeye measured clock minutes. |
| `movement_inferred_clock_minutes` | Int64 | Movement inferred clock minutes. |
| `diff_measured_inferred` | Float64 | Diff measured inferred. |
| `diff2` | Float64 | Diff2. |
| `diff_measured_inferred_minutes` | Int64 | Diff measured inferred minutes. |
| `hawkeye_measured_clock_hh` | Int64 | Hawkeye measured clock hh. |
| `hawkeye_measured_clock_mm` | Int64 | Hawkeye measured clock mm. |
| `movement_inferred_clock_hh` | Int64 | Movement inferred clock hh. |
| `movement_inferred_clock_mm` | Int64 | Movement inferred clock mm. |
| `diff_clock_hh` | Int64 | Diff clock hh. |
| `diff_clock_mm` | Int64 | Diff clock mm. |
| `hawkeye_measured_clock_label` | String | Hawkeye measured clock label. |
| `movement_inferred_clock_label` | String | Movement inferred clock label. |
| `diff_clock_label` | String | Diff clock label. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_spin_direction-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_spin_direction-example}

```python
mlb_statcast_leaderboard_spin_direction()
```

## mlb_statcast_leaderboard_arm_angles

GET /leaderboard/pitcher-arm-angles — pitcher arm-angle leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitcher-arm-angles`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitcher-arm-angles?csv=true](https://baseballsavant.mlb.com/leaderboard/pitcher-arm-angles?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_arm_angles-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | MLBAM id of the pitcher. |
| `pitcher_name` | String | Pitcher name. |
| `pitch_hand` | String | Pitcher handedness (R/L). |
| `n_pitches` | Int64 | Number of pitches. |
| `team_id` | Int64 | MLBAM team id. |
| `ball_angle` | Float64 | Arm slot angle (deg). |
| `relative_release_ball_x` | Float64 | Relative release ball x. |
| `release_ball_z` | Float64 | Release ball z. |
| `relative_shoulder_x` | Float64 | Relative shoulder x. |
| `shoulder_z` | Float64 | Shoulder z. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_arm_angles-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_arm_angles-example}

```python
mlb_statcast_leaderboard_arm_angles()
```

## mlb_statcast_leaderboard_pitcher_running_game

GET /leaderboard/pitcher-running-game — pitcher running-game (holding runners) leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitcher-running-game`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitcher-running-game?csv=true](https://baseballsavant.mlb.com/leaderboard/pitcher-running-game?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_pitcher_running_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | MLBAM player id. |
| `player_name` | String | Player name. |
| `team_name` | String | Team name. |
| `start_year` | Int64 | First season in the range. |
| `end_year` | Int64 | Last season in the range. |
| `key_target_base` | String | Key target base. |
| `runs_prevented_on_running_attr` | Float64 | Runs prevented on running attr. |
| `n_pitcher_cs_aa` | Float64 | Number of pitcher cs aa. |
| `n_init` | Int64 | Number of init. |
| `rate_sbx` | Float64 | Rate sbx. |
| `n_sb` | Int64 | Stolen bases allowed (count). |
| `n_cs` | Int64 | Caught stealing (count). |
| `n_pk` | Int64 | Number of pk. |
| `n_bk` | Int64 | Number of bk. |
| `n_fb` | Int64 | Number of fb. |
| `n_plus` | Int64 | Number of plus. |
| `n_minus` | Int64 | Number of minus. |
| `net_attr_plus` | Float64 | Net attr plus. |
| `net_attr_minus` | Float64 | Net attr minus. |
| `r_primary_lead` | Float64 | Average primary lead distance (ft). |
| `r_secondary_lead` | Float64 | Average secondary lead (ft). |
| `r_sec_minus_prim_lead` | Float64 | R sec minus prim lead. |
| `r_primary_lead_sbx` | Float64 | R primary lead sbx. |
| `r_secondary_lead_sbx` | Float64 | R secondary lead sbx. |
| `r_sec_minus_prim_lead_sbx` | Float64 | R sec minus prim lead sbx. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_pitcher_running_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_pitcher_running_game-example}

```python
mlb_statcast_leaderboard_pitcher_running_game()
```

## mlb_statcast_leaderboard_outs_above_average

GET /leaderboard/outs_above_average — Outs Above Average (OAA) fielding leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/outs_above_average`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/outs_above_average?csv=true](https://baseballsavant.mlb.com/leaderboard/outs_above_average?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_outs_above_average-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_name, first_name` | String | Last name, first name. |
| `player_id` | String | MLBAM player id. |
| `display_team_name` | String | Team display name. |
| `year` | String | Season year. |
| `primary_pos_formatted` | String | Primary position (formatted). |
| `fielding_runs_prevented` | String | Fielding Run Value (runs). |
| `outs_above_average` | String | Outs Above Average. |
| `outs_above_average_infront` | String | Outs above average infront. |
| `outs_above_average_lateral_toward3bline` | String | Outs above average lateral toward3bline. |
| `outs_above_average_lateral_toward1bline` | String | Outs above average lateral toward1bline. |
| `outs_above_average_behind` | String | Outs above average behind. |
| `outs_above_average_rhh` | String | Outs above average rhh. |
| `outs_above_average_lhh` | String | Outs above average lhh. |
| `actual_success_rate_formatted` | String | Actual success rate formatted. |
| `adj_estimated_success_rate_formatted` | String | Adj estimated success rate formatted. |
| `diff_success_rate_formatted` | String | Diff success rate formatted. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_outs_above_average-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_outs_above_average-example}

```python
mlb_statcast_leaderboard_outs_above_average()
```

## mlb_statcast_leaderboard_outfield_directional_oaa

GET /leaderboard/outfield_directional_outs_above_average — outfield directional OAA leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/outfield_directional_outs_above_average`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/outfield_directional_outs_above_average?csv=true](https://baseballsavant.mlb.com/leaderboard/outfield_directional_outs_above_average?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_outfield_directional_oaa-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_name, first_name` | String | Last name, first name. |
| `player_id` | Int64 | MLBAM player id. |
| `attempts` | Int64 | Opportunities/attempts. |
| `n_outs_above_average` | Int64 | Outs Above Average (count). |
| `n_oaa_slice_back_left` | Int64 | Number of oaa slice back left. |
| `n_oaa_slice_back` | Int64 | Number of oaa slice back. |
| `n_oaa_slice_back_right` | Int64 | Number of oaa slice back right. |
| `n_oaa_slice_back_all` | Int64 | Number of oaa slice back all. |
| `n_oaa_slice_in_left` | Int64 | Number of oaa slice in left. |
| `n_oaa_slice_in` | Int64 | Number of oaa slice in. |
| `n_oaa_slice_in_right` | Int64 | Number of oaa slice in right. |
| `n_oaa_slice_in_all` | Int64 | Number of oaa slice in all. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_outfield_directional_oaa-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_outfield_directional_oaa-example}

```python
mlb_statcast_leaderboard_outfield_directional_oaa()
```

## mlb_statcast_leaderboard_outfield_jump

GET /leaderboard/outfield_jump — outfielder jump leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/outfield_jump`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/outfield_jump?csv=true](https://baseballsavant.mlb.com/leaderboard/outfield_jump?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_outfield_jump-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_name, first_name` | String | Last name, first name. |
| `resp_fielder_id` | Int64 | MLBAM id of the responsible fielder. |
| `year` | Int64 | Season year. |
| `outs_above_average` | Int64 | Outs Above Average. |
| `outs_per_play` | Float64 | Outs per play. |
| `rel_league_burst_distance` | Int64 | Rel league burst distance. |
| `rel_league_reaction_distance` | Float64 | Rel league reaction distance. |
| `rel_league_routing_distance` | Float64 | Rel league routing distance. |
| `rel_league_bootup_distance` | Float64 | Rel league bootup distance. |
| `f_bootup_distance` | Float64 | F bootup distance. |
| `n` | Int64 | Sample count (pitches/events). |
| `n_outs` | Int64 | Number of outs. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_outfield_jump-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_outfield_jump-example}

```python
mlb_statcast_leaderboard_outfield_jump()
```

## mlb_statcast_leaderboard_catch_probability

GET /leaderboard/catch_probability — outfielder catch-probability leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/catch_probability`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/catch_probability?csv=true](https://baseballsavant.mlb.com/leaderboard/catch_probability?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_catch_probability-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_name, first_name` | String | Last name, first name. |
| `player_id` | String | MLBAM player id. |
| `oaa` | String | Outs Above Average. |
| `n_fieldout_5stars` | String | 5-star (hardest) plays made. |
| `n_opp_5stars` | String | 5-star play opportunities. |
| `n_5star_percent` | String | Number of 5star rate. |
| `n_fieldout_4stars` | String | Number of fieldout 4stars. |
| `n_opp_4stars` | String | Number of opp 4stars. |
| `n_4star_percent` | String | Number of 4star rate. |
| `n_fieldout_3stars` | String | Number of fieldout 3stars. |
| `n_opp_3stars` | String | Number of opp 3stars. |
| `n_3star_percent` | String | Number of 3star rate. |
| `n_fieldout_2stars` | String | Number of fieldout 2stars. |
| `n_opp_2stars` | String | Number of opp 2stars. |
| `n_2star_percent` | String | Number of 2star rate. |
| `n_fieldout_1stars` | String | Number of fieldout 1stars. |
| `n_opp_1stars` | String | Number of opp 1stars. |
| `n_1star_percent` | String | Number of 1star rate. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_catch_probability-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_catch_probability-example}

```python
mlb_statcast_leaderboard_catch_probability()
```

## mlb_statcast_leaderboard_arm_strength

GET /leaderboard/arm-strength — fielder arm-strength leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/arm-strength`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/arm-strength?csv=true](https://baseballsavant.mlb.com/leaderboard/arm-strength?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_arm_strength-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `fielder_name` | String | Fielder name. |
| `player_id` | Int64 | MLBAM player id. |
| `team_name` | String | Team name. |
| `primary_position` | Int64 | Primary fielding position. |
| `primary_position_name` | String | Primary position name. |
| `total_throws` | Int64 | Total throws. |
| `total_throws_1b` | Int64 | Total throws 1b. |
| `total_throws_2b` | Int64 | Total throws 2b. |
| `total_throws_3b` | Int64 | Total throws 3b. |
| `total_throws_ss` | Int64 | Total throws ss. |
| `total_throws_lf` | Int64 | Total throws lf. |
| `total_throws_cf` | Int64 | Total throws cf. |
| `total_throws_rf` | Int64 | Total throws rf. |
| `total_throws_inf` | Int64 | Total throws inf. |
| `total_throws_of` | Int64 | Total throws of. |
| `max_arm_strength` | Float64 | Max arm strength (mph). |
| `arm_1b` | Float64 | Arm 1b. |
| `arm_2b` | String | Arm 2b. |
| `arm_3b` | String | Arm 3b. |
| `arm_ss` | String | Arm ss. |
| `arm_lf` | String | Arm lf. |
| `arm_cf` | String | Arm cf. |
| `arm_rf` | String | Arm rf. |
| `arm_inf` | String | Arm inf. |
| `arm_of` | String | Arm of. |
| `arm_overall` | Float64 | Arm overall. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_arm_strength-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_arm_strength-example}

```python
mlb_statcast_leaderboard_arm_strength()
```

## mlb_statcast_leaderboard_poptime

GET /leaderboard/poptime — catcher pop-time leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/poptime`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/poptime?csv=true](https://baseballsavant.mlb.com/leaderboard/poptime?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_poptime-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `entity_name` | String | Player (or team) entity name. |
| `entity_id` | Int64 | MLBAM id of the player/team entity. |
| `team_id` | Int64 | MLBAM team id. |
| `age` | Int64 | Player age. |
| `maxeff_arm_2b_3b_sba` | Float64 | Max-effort arm velo to 2B/3B (mph). |
| `exchange_2b_3b_sba` | Float64 | Transfer/exchange time (s). |
| `pop_2b_sba_count` | Int64 | Pop-time sample (throws to 2B). |
| `pop_2b_sba` | Float64 | Pop time to 2B on stolen-base attempts (s). |
| `pop_2b_cs` | Float64 | Pop 2b cs. |
| `pop_2b_sb` | Float64 | Pop 2b sb. |
| `pop_3b_sba_count` | Int64 | Pop 3b sba count. |
| `pop_3b_sba` | Float64 | Pop 3b sba. |
| `pop_3b_cs` | Float64 | Pop 3b cs. |
| `pop_3b_sb` | Float64 | Pop 3b sb. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_poptime-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_poptime-example}

```python
mlb_statcast_leaderboard_poptime()
```

## mlb_statcast_leaderboard_catcher_framing

GET /leaderboard/catcher-framing — catcher framing leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/catcher-framing`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/catcher-framing?csv=true](https://baseballsavant.mlb.com/leaderboard/catcher-framing?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_catcher_framing-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | MLBAM player id. |
| `name` | String | Player (or entity) name. |
| `pitches` | Int64 | Pitches. |
| `rv_tot` | Float64 | Total framing run value. |
| `pct_tot` | Float64 | Total called-strike rate. |
| `rv_11` | Int64 | Rv 11. |
| `pct_11` | Float64 | Pct 11. |
| `rv_12` | Int64 | Rv 12. |
| `pct_12` | Float64 | Pct 12. |
| `rv_13` | Int64 | Rv 13. |
| `pct_13` | Int64 | Pct 13. |
| `rv_14` | Int64 | Rv 14. |
| `pct_14` | Float64 | Pct 14. |
| `rv_16` | Int64 | Rv 16. |
| `pct_16` | Float64 | Pct 16. |
| `rv_17` | Int64 | Rv 17. |
| `pct_17` | Float64 | Pct 17. |
| `rv_18` | Int64 | Rv 18. |
| `pct_18` | Float64 | Pct 18. |
| `rv_19` | Int64 | Rv 19. |
| `pct_19` | Float64 | Pct 19. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_catcher_framing-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_catcher_framing-example}

```python
mlb_statcast_leaderboard_catcher_framing()
```

## mlb_statcast_leaderboard_catcher_blocking

GET /leaderboard/catcher-blocking — catcher blocking leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/catcher-blocking`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/catcher-blocking?csv=true](https://baseballsavant.mlb.com/leaderboard/catcher-blocking?csv=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns {#mlb_statcast_leaderboard_catcher_blocking-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | MLBAM player id. |
| `player_name` | String | Player name. |
| `team_name` | String | Team name. |
| `start_year` | Int64 | First season in the range. |
| `end_year` | String | Last season in the range. |
| `pitches` | Int64 | Pitches. |
| `catcher_blocking_runs` | Int64 | Catcher blocking runs. |
| `blocks_above_average` | Int64 | Blocks above average. |
| `n_pbwp` | Int64 | Number of pbwp. |
| `x_pbwp` | Float64 | X pbwp. |
| `blocks_above_average_per_game` | Float64 | Blocks above average per game. |
| `freq_pbwp_easy` | Float64 | Freq pbwp easy. |
| `freq_pbwp_medium` | Float64 | Freq pbwp medium. |
| `freq_pbwp_tough` | Float64 | Freq pbwp tough. |
| `diff_pbwp_easy` | Float64 | Diff pbwp easy. |
| `diff_pbwp_medium` | Float64 | Diff pbwp medium. |
| `diff_pbwp_tough` | Float64 | Diff pbwp tough. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_leaderboard_catcher_blocking-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_leaderboard_catcher_blocking-example}

```python
mlb_statcast_leaderboard_catcher_blocking()
```
