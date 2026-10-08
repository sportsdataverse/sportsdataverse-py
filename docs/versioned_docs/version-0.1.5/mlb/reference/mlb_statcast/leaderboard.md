---
title: "MLB — MLB Statcast (Baseball Savant) — Leaderboard: expected–baserunning"
sidebar_label: "Leaderboard: expected–baserunning"
sidebar_position: 1
description: "MLB — MLB Statcast (Baseball Savant) — Leaderboard: expected–baserunning — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Statcast (Baseball Savant) — Leaderboard: expected–baserunning

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
| `last_name, first_name` | character | Last name, first name. |
| `player_id` | integer | MLBAM player id. |
| `year` | integer | Season year. |
| `pa` | integer | Plate appearances. |
| `bip` | integer | Balls in play. |
| `ba` | numeric | Batting average. |
| `est_ba` | numeric | Expected batting average (xBA). |
| `est_ba_minus_ba_diff` | numeric | xBA minus actual BA (over/under-performance). |
| `slg` | numeric | Slugging percentage. |
| `est_slg` | numeric | Expected slugging (xSLG). |
| `est_slg_minus_slg_diff` | numeric | xSLG minus actual SLG. |
| `woba` | numeric | Weighted on-base average. |
| `est_woba` | numeric | Expected wOBA (xwOBA). |
| `est_woba_minus_woba_diff` | numeric | xwOBA minus actual wOBA. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_expected_stats-example}

```python
mlb_statcast_leaderboard_expected_stats()
```

_Last validated n/a._

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
| `player_name` | character | Player name. |
| `player_id` | integer | MLBAM player id. |
| `year` | integer | Season year. |
| `xwoba` | character | Expected wOBA. |
| `xba` | character | Expected batting average. |
| `xslg` | character | Expected slugging. |
| `xiso` | character | Expected isolated power. |
| `xobp` | character | Expected on-base percentage. |
| `brl` | character | Barrels. |
| `brl_percent` | character | Barrel rate (% of batted balls). |
| `exit_velocity` | character | Exit velocity (mph). |
| `max_ev` | integer | Max ev. |
| `hard_hit_percent` | character | Hard-hit rate (95+ mph EV). |
| `k_percent` | character | Strikeout rate. |
| `bb_percent` | character | Walk rate. |
| `whiff_percent` | character | Whiff rate (swings and misses / swings). |
| `chase_percent` | character | Chase rate. |
| `arm_strength` | integer | Arm strength (mph, top throws). |
| `sprint_speed` | integer | Sprint speed (ft/sec, top 50% of competitive runs). |
| `oaa` | integer | Outs Above Average. |
| `bat_speed` | character | Bat speed (mph). |
| `squared_up_rate` | character | Squared up rate. |
| `swing_length` | character | Swing length (ft, head travel). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_percentile_rankings-example}

```python
mlb_statcast_leaderboard_percentile_rankings()
```

_Last validated n/a._

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
| `last_name, first_name` | character | Last name, first name. |
| `player_id` | integer | MLBAM player id. |
| `team_id` | integer | MLBAM team id. |
| `team` | character | Team abbreviation. |
| `position` | character | Position. |
| `age` | integer | Player age. |
| `competitive_runs` | integer | Competitive runs (qualifying sprint-speed runs). |
| `bolts` | integer | Bolts. |
| `hp_to_1b` | numeric | Home-to-first time (s). |
| `sprint_speed` | numeric | Sprint speed (ft/sec, top 50% of competitive runs). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_sprint_speed-example}

```python
mlb_statcast_leaderboard_sprint_speed()
```

_Last validated n/a._

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
| `last_name, first_name` | character | Last name, first name. |
| `player_id` | integer | MLBAM player id. |
| `name_abbrev` | character | Team/name abbreviation. |
| `team_id` | integer | MLBAM team id. |
| `position_name` | character | Position name. |
| `age` | integer | Player age. |
| `bat_side` | character | Batter side (R/L/S). |
| `seconds_since_hit_000` | numeric | Seconds since hit 000. |
| `seconds_since_hit_005` | numeric | Seconds since hit 005. |
| `seconds_since_hit_010` | numeric | Seconds since hit 010. |
| `seconds_since_hit_015` | numeric | Seconds since hit 015. |
| `seconds_since_hit_020` | numeric | Seconds since hit 020. |
| `seconds_since_hit_025` | numeric | Seconds since hit 025. |
| `seconds_since_hit_030` | numeric | Seconds since hit 030. |
| `seconds_since_hit_035` | numeric | Seconds since hit 035. |
| `seconds_since_hit_040` | numeric | Seconds since hit 040. |
| `seconds_since_hit_045` | numeric | Seconds since hit 045. |
| `seconds_since_hit_050` | numeric | Seconds since hit 050. |
| `seconds_since_hit_055` | numeric | Seconds since hit 055. |
| `seconds_since_hit_060` | numeric | Seconds since hit 060. |
| `seconds_since_hit_065` | numeric | Seconds since hit 065. |
| `seconds_since_hit_070` | numeric | Seconds since hit 070. |
| `seconds_since_hit_075` | numeric | Seconds since hit 075. |
| `seconds_since_hit_080` | numeric | Seconds since hit 080. |
| `seconds_since_hit_085` | numeric | Seconds since hit 085. |
| `seconds_since_hit_090` | numeric | Seconds since hit 090. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_running_splits-example}

```python
mlb_statcast_leaderboard_running_splits()
```

_Last validated n/a._

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
| `id` | integer | MLBAM player id. |
| `name` | character | Player (or entity) name. |
| `swings_competitive` | integer | Competitive swings. |
| `percent_swings_competitive` | numeric | Share of swings that are competitive. |
| `contact` | integer | Contact. |
| `avg_bat_speed` | numeric | Average bat speed (mph). |
| `hard_swing_rate` | numeric | Hard swing rate. |
| `squared_up_per_bat_contact` | numeric | Squared up per bat contact. |
| `squared_up_per_swing` | numeric | Squared-up rate per swing. |
| `blast_per_bat_contact` | numeric | Blast per bat contact. |
| `blast_per_swing` | numeric | Blasts per swing. |
| `swing_length` | numeric | Swing length (ft, head travel). |
| `swords` | integer | Swords. |
| `batter_run_value` | numeric | Batter run value. |
| `whiffs` | character | Whiffs. |
| `whiff_per_swing` | character | Whiff per swing. |
| `batted_ball_events` | integer | Batted ball events. |
| `batted_ball_event_per_swing` | numeric | Batted ball event per swing. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_bat_tracking-example}

```python
mlb_statcast_leaderboard_bat_tracking()
```

_Last validated n/a._

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
| `id` | integer | MLBAM player id. |
| `name` | character | Player (or entity) name. |
| `side` | character | Side. |
| `avg_bat_speed` | numeric | Average bat speed (mph). |
| `swing_tilt` | numeric | Swing tilt (deg). |
| `attack_angle` | numeric | Attack angle (deg, bat path at contact). |
| `attack_direction` | numeric | Attack direction (deg, pull/oppo). |
| `ideal_attack_angle_rate` | numeric | Rate of swings in the ideal attack-angle window. |
| `avg_intercept_y_vs_plate` | numeric | Avg intercept y vs plate. |
| `avg_intercept_y_vs_batter` | numeric | Avg intercept y vs batter. |
| `avg_batter_y_position` | numeric | Avg batter y position. |
| `avg_batter_x_position` | numeric | Avg batter x position. |
| `competitive_swings` | integer | Competitive swings. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_swing_path-example}

```python
mlb_statcast_leaderboard_swing_path()
```

_Last validated n/a._

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
| `id` | integer | MLBAM player id. |
| `name` | character | Player (or entity) name. |
| `year` | integer | Season year. |
| `team_name` | character | Team name. |
| `bat_side_formatted` | character | Batter side (formatted). |
| `miss_distance` | numeric | Average miss distance (in) on swings. |
| `flawed_percent` | numeric | Flawed rate. |
| `perfect_percent` | numeric | Perfect rate. |
| `tied_up_percent` | numeric | Tied up rate. |
| `avg_x_tied_up` | numeric | Avg x tied up. |
| `centered_percent` | numeric | Centered rate. |
| `flailed_percent` | numeric | Flailed rate. |
| `avg_x_flail` | numeric | Avg x flail. |
| `early_percent` | numeric | Early rate. |
| `avg_y_early` | numeric | Avg y early. |
| `on_time_percent` | numeric | On time rate. |
| `late_percent` | numeric | Late rate. |
| `avg_y_late` | numeric | Avg y late. |
| `n_swings` | integer | Number of swings. |
| `whiff_rate` | numeric | Whiff rate. |
| `competitive_percent` | numeric | Competitive rate. |
| `over_percent` | numeric | Over rate. |
| `avg_z_over` | numeric | Avg z over. |
| `lined_up_percent` | numeric | Lined up rate. |
| `under_percent` | numeric | Under rate. |
| `avg_z_under` | numeric | Avg z under. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_swing_timing-example}

```python
mlb_statcast_leaderboard_swing_timing()
```

_Last validated n/a._

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
| `year` | character | Season year. |
| `last_name, first_name` | character | Last name, first name. |
| `player_id` | character | MLBAM player id. |
| `team_id` | character | MLBAM team id. |
| `pa` | character | Plate appearances. |
| `pitches` | character | Pitches. |
| `runs_all` | character | Runs all. |
| `runs_heart` | character | Runs heart. |
| `runs_shadow` | character | Runs shadow. |
| `runs_chase` | character | Runs chase. |
| `runs_waste` | character | Runs waste. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_swing_take-example}

```python
mlb_statcast_leaderboard_swing_take()
```

_Last validated n/a._

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
| `last_name, first_name` | character | Last name, first name. |
| `player_id` | integer | MLBAM player id. |
| `attempts` | integer | Opportunities/attempts. |
| `avg_hit_angle` | numeric | Average launch angle (deg). |
| `anglesweetspotpercent` | numeric | Anglesweetspotpercent. |
| `max_hit_speed` | numeric | Max exit velocity (mph). |
| `avg_hit_speed` | numeric | Average exit velocity (mph). |
| `ev50` | numeric | Ev50. |
| `fbld` | numeric | Fbld. |
| `gb` | numeric | Gb. |
| `max_distance` | integer | Max distance. |
| `avg_distance` | integer | Avg distance. |
| `avg_hr_distance` | integer | Avg hr distance. |
| `ev95plus` | integer | Ev95plus. |
| `ev95percent` | numeric | Ev95percent. |
| `barrels` | integer | Barrels. |
| `brl_percent` | numeric | Barrel rate (% of batted balls). |
| `brl_pa` | numeric | Barrels per plate appearance. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_exit_velocity_barrels-example}

```python
mlb_statcast_leaderboard_exit_velocity_barrels()
```

_Last validated n/a._

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
| `id` | integer | MLBAM player id. |
| `name` | character | Player (or entity) name. |
| `year` | integer | Season year. |
| `bbe` | integer | Batted-ball events. |
| `gb_rate` | numeric | Gb rate. |
| `air_rate` | numeric | Air rate. |
| `fb_rate` | numeric | Fb rate. |
| `ld_rate` | numeric | Ld rate. |
| `pu_rate` | numeric | Pu rate. |
| `pull_rate` | numeric | Pull rate. |
| `straight_rate` | numeric | Straight rate. |
| `oppo_rate` | numeric | Oppo rate. |
| `pull_gb_rate` | numeric | Pull gb rate. |
| `straight_gb_rate` | numeric | Straight gb rate. |
| `oppo_gb_rate` | numeric | Oppo gb rate. |
| `pull_air_rate` | numeric | Pull air rate. |
| `straight_air_rate` | numeric | Straight air rate. |
| `oppo_air_rate` | numeric | Oppo air rate. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_batted_ball-example}

```python
mlb_statcast_leaderboard_batted_ball()
```

_Last validated n/a._

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
| `player` | character | Player. |
| `player_id` | integer | MLBAM player id. |
| `team_abbrev` | character | Team abbreviation. |
| `year` | integer | Season year. |
| `type` | character | Record/pitch type. |
| `avg_hr_trot` | numeric | Avg hr trot. |
| `doubters` | integer | Doubters. |
| `mostly_gone` | integer | Mostly gone. |
| `no_doubters` | integer | No doubters. |
| `no_doubter_per` | numeric | No doubter per. |
| `hr_total` | integer | Hr total. |
| `xhr` | numeric | Xhr. |
| `xhr_diff` | numeric | Xhr diff. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_home_runs-example}

```python
mlb_statcast_leaderboard_home_runs()
```

_Last validated n/a._

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
| `last_name, first_name` | character | Last name, first name. |
| `pitcher` | integer | MLBAM id of the pitcher. |
| `ff_batter` | character | Ff batter. |
| `si_batter` | character | Si batter. |
| `fc_batter` | character | Fc batter. |
| `sl_batter` | character | Sl batter. |
| `ch_batter` | character | Ch batter. |
| `cu_batter` | character | Cu batter. |
| `fs_batter` | character | Fs batter. |
| `kn_batter` | character | Kn batter. |
| `st_batter` | character | St batter. |
| `sv_batter` | character | Sv batter. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_pitch_arsenals-example}

```python
mlb_statcast_leaderboard_pitch_arsenals()
```

_Last validated n/a._

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
| `last_name, first_name` | character | Last name, first name. |
| `player_id` | integer | MLBAM player id. |
| `team_name_alt` | character | Team name (alternate form). |
| `pitch_type` | character | Pitch type code. |
| `pitch_name` | character | Pitch type name. |
| `run_value_per_100` | numeric | Run value per 100 pitches. |
| `run_value` | integer | Run value (runs). |
| `pitches` | integer | Pitches. |
| `pitch_usage` | numeric | Pitch usage. |
| `pa` | integer | Plate appearances. |
| `ba` | numeric | Batting average. |
| `slg` | numeric | Slugging percentage. |
| `woba` | numeric | Weighted on-base average. |
| `whiff_percent` | numeric | Whiff rate (swings and misses / swings). |
| `k_percent` | numeric | Strikeout rate. |
| `put_away` | numeric | Put away. |
| `est_ba` | numeric | Expected batting average (xBA). |
| `est_slg` | numeric | Expected slugging (xSLG). |
| `est_woba` | numeric | Expected wOBA (xwOBA). |
| `hard_hit_percent` | numeric | Hard-hit rate (95+ mph EV). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_pitch_arsenal_stats-example}

```python
mlb_statcast_leaderboard_pitch_arsenal_stats()
```

_Last validated n/a._

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
| `year` | integer | Season year. |
| `last_name, first_name` | character | Last name, first name. |
| `pitcher_id` | integer | MLBAM id of the pitcher. |
| `team_name` | character | Team name. |
| `team_name_abbrev` | character | Team name abbrev. |
| `pitch_hand` | character | Pitcher handedness (R/L). |
| `avg_speed` | integer | Average pitch velocity (mph). |
| `pitches_thrown` | integer | Pitches thrown. |
| `total_pitches` | integer | Total pitches. |
| `pitches_per_game` | numeric | Pitches per game. |
| `pitch_per` | numeric | Pitch per. |
| `pitch_type` | character | Pitch type code. |
| `pitch_type_name` | character | Pitch type name. |
| `pitcher_break_z` | numeric | Pitcher break z. |
| `league_break_z` | numeric | League break z. |
| `diff_z` | numeric | Diff z. |
| `rise` | integer | Rise. |
| `pitcher_break_z_induced` | numeric | Pitcher break z induced. |
| `pitcher_break_x` | numeric | Pitcher break x. |
| `league_break_x` | numeric | League break x. |
| `diff_x` | numeric | Diff x. |
| `tail` | integer | Tail. |
| `percent_rank_diff_z` | numeric | Percent rank diff z. |
| `percent_rank_diff_x` | numeric | Percent rank diff x. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_pitch_movement-example}

```python
mlb_statcast_leaderboard_pitch_movement()
```

_Last validated n/a._

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
| `entity_id` | integer | MLBAM id of the player/team entity. |
| `entity_name` | character | Player (or team) entity name. |
| `entity_code` | character | Entity code. |
| `team_id` | integer | MLBAM team id. |
| `total_pitches` | integer | Total pitches. |
| `total_pitches_empty` | integer | Total pitches empty. |
| `median_seconds_empty` | numeric | Median tempo (s) with bases empty. |
| `total_pitches_onbase` | integer | Total pitches onbase. |
| `freq_hot` | numeric | Freq hot. |
| `freq_warm` | numeric | Freq warm. |
| `freq_cold` | numeric | Freq cold. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_pitch_tempo-example}

```python
mlb_statcast_leaderboard_pitch_tempo()
```

_Last validated n/a._

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
| `entity_name` | character | Player (or team) entity name. |
| `entity_id` | integer | MLBAM id of the player/team entity. |
| `pitch_hand` | character | Pitcher handedness (R/L). |
| `active_spin_fourseam` | character | Active spin fourseam. |
| `active_spin_sinker` | numeric | Active spin sinker. |
| `active_spin_cutter` | numeric | Active spin cutter. |
| `active_spin_changeup` | numeric | Active spin changeup. |
| `active_spin_splitter` | character | Active spin splitter. |
| `active_spin_curve` | character | Active spin curve. |
| `active_spin_slider` | numeric | Active spin slider. |
| `active_spin_sweeper` | numeric | Active spin sweeper. |
| `active_spin_slurve` | character | Active spin slurve. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_active_spin-example}

```python
mlb_statcast_leaderboard_active_spin()
```

_Last validated n/a._

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
| `year` | integer | Season year. |
| `last_name, first_name` | character | Last name, first name. |
| `player_id` | integer | MLBAM player id. |
| `pitch_hand` | character | Pitcher handedness (R/L). |
| `api_pitch_type` | character | Pitch type (API code). |
| `n_pitches` | integer | Number of pitches. |
| `release_speed` | numeric | Release speed. |
| `spin_rate` | integer | Spin rate (rpm). |
| `movement_inches` | numeric | Movement inches. |
| `alan_active_spin_pct` | numeric | Alan active spin rate. |
| `active_spin` | numeric | Active (useful) spin (%). |
| `hawkeye_measured` | numeric | Hawkeye measured. |
| `movement_inferred` | numeric | Movement inferred. |
| `api_pitch_name` | character | Api pitch name. |
| `active_spin_formatted` | integer | Active spin (formatted, %). |
| `hawkeye_measured_clock_minutes` | integer | Hawkeye measured clock minutes. |
| `movement_inferred_clock_minutes` | integer | Movement inferred clock minutes. |
| `diff_measured_inferred` | numeric | Diff measured inferred. |
| `diff2` | numeric | Diff2. |
| `diff_measured_inferred_minutes` | integer | Diff measured inferred minutes. |
| `hawkeye_measured_clock_hh` | integer | Hawkeye measured clock hh. |
| `hawkeye_measured_clock_mm` | integer | Hawkeye measured clock mm. |
| `movement_inferred_clock_hh` | integer | Movement inferred clock hh. |
| `movement_inferred_clock_mm` | integer | Movement inferred clock mm. |
| `diff_clock_hh` | integer | Diff clock hh. |
| `diff_clock_mm` | integer | Diff clock mm. |
| `hawkeye_measured_clock_label` | character | Hawkeye measured clock label. |
| `movement_inferred_clock_label` | character | Movement inferred clock label. |
| `diff_clock_label` | character | Diff clock label. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_spin_direction-example}

```python
mlb_statcast_leaderboard_spin_direction()
```

_Last validated n/a._

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
| `pitcher` | integer | MLBAM id of the pitcher. |
| `pitcher_name` | character | Pitcher name. |
| `pitch_hand` | character | Pitcher handedness (R/L). |
| `n_pitches` | integer | Number of pitches. |
| `team_id` | integer | MLBAM team id. |
| `ball_angle` | numeric | Arm slot angle (deg). |
| `relative_release_ball_x` | numeric | Relative release ball x. |
| `release_ball_z` | numeric | Release ball z. |
| `relative_shoulder_x` | numeric | Relative shoulder x. |
| `shoulder_z` | numeric | Shoulder z. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_arm_angles-example}

```python
mlb_statcast_leaderboard_arm_angles()
```

_Last validated n/a._

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
| `player_id` | integer | MLBAM player id. |
| `player_name` | character | Player name. |
| `team_name` | character | Team name. |
| `start_year` | integer | First season in the range. |
| `end_year` | integer | Last season in the range. |
| `key_target_base` | character | Key target base. |
| `runs_prevented_on_running_attr` | numeric | Runs prevented on running attr. |
| `n_pitcher_cs_aa` | numeric | Number of pitcher cs aa. |
| `n_init` | integer | Number of init. |
| `rate_sbx` | numeric | Rate sbx. |
| `n_sb` | integer | Stolen bases allowed (count). |
| `n_cs` | integer | Caught stealing (count). |
| `n_pk` | integer | Number of pk. |
| `n_bk` | integer | Number of bk. |
| `n_fb` | integer | Number of fb. |
| `n_plus` | integer | Number of plus. |
| `n_minus` | integer | Number of minus. |
| `net_attr_plus` | numeric | Net attr plus. |
| `net_attr_minus` | numeric | Net attr minus. |
| `r_primary_lead` | numeric | Average primary lead distance (ft). |
| `r_secondary_lead` | numeric | Average secondary lead (ft). |
| `r_sec_minus_prim_lead` | numeric | R sec minus prim lead. |
| `r_primary_lead_sbx` | numeric | R primary lead sbx. |
| `r_secondary_lead_sbx` | numeric | R secondary lead sbx. |
| `r_sec_minus_prim_lead_sbx` | numeric | R sec minus prim lead sbx. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_pitcher_running_game-example}

```python
mlb_statcast_leaderboard_pitcher_running_game()
```

_Last validated n/a._

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
| `last_name, first_name` | character | Last name, first name. |
| `player_id` | character | MLBAM player id. |
| `display_team_name` | character | Team display name. |
| `year` | character | Season year. |
| `primary_pos_formatted` | character | Primary position (formatted). |
| `fielding_runs_prevented` | character | Fielding Run Value (runs). |
| `outs_above_average` | character | Outs Above Average. |
| `outs_above_average_infront` | character | Outs above average infront. |
| `outs_above_average_lateral_toward3bline` | character | Outs above average lateral toward3bline. |
| `outs_above_average_lateral_toward1bline` | character | Outs above average lateral toward1bline. |
| `outs_above_average_behind` | character | Outs above average behind. |
| `outs_above_average_rhh` | character | Outs above average rhh. |
| `outs_above_average_lhh` | character | Outs above average lhh. |
| `actual_success_rate_formatted` | character | Actual success rate formatted. |
| `adj_estimated_success_rate_formatted` | character | Adj estimated success rate formatted. |
| `diff_success_rate_formatted` | character | Diff success rate formatted. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_outs_above_average-example}

```python
mlb_statcast_leaderboard_outs_above_average()
```

_Last validated n/a._

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
| `last_name, first_name` | character | Last name, first name. |
| `player_id` | integer | MLBAM player id. |
| `attempts` | integer | Opportunities/attempts. |
| `n_outs_above_average` | integer | Outs Above Average (count). |
| `n_oaa_slice_back_left` | integer | Number of oaa slice back left. |
| `n_oaa_slice_back` | integer | Number of oaa slice back. |
| `n_oaa_slice_back_right` | integer | Number of oaa slice back right. |
| `n_oaa_slice_back_all` | integer | Number of oaa slice back all. |
| `n_oaa_slice_in_left` | integer | Number of oaa slice in left. |
| `n_oaa_slice_in` | integer | Number of oaa slice in. |
| `n_oaa_slice_in_right` | integer | Number of oaa slice in right. |
| `n_oaa_slice_in_all` | integer | Number of oaa slice in all. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_outfield_directional_oaa-example}

```python
mlb_statcast_leaderboard_outfield_directional_oaa()
```

_Last validated n/a._

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
| `last_name, first_name` | character | Last name, first name. |
| `resp_fielder_id` | integer | MLBAM id of the responsible fielder. |
| `year` | integer | Season year. |
| `outs_above_average` | integer | Outs Above Average. |
| `outs_per_play` | numeric | Outs per play. |
| `rel_league_burst_distance` | integer | Rel league burst distance. |
| `rel_league_reaction_distance` | numeric | Rel league reaction distance. |
| `rel_league_routing_distance` | numeric | Rel league routing distance. |
| `rel_league_bootup_distance` | numeric | Rel league bootup distance. |
| `f_bootup_distance` | numeric | F bootup distance. |
| `n` | integer | Sample count (pitches/events). |
| `n_outs` | integer | Number of outs. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_outfield_jump-example}

```python
mlb_statcast_leaderboard_outfield_jump()
```

_Last validated n/a._

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
| `last_name, first_name` | character | Last name, first name. |
| `player_id` | character | MLBAM player id. |
| `oaa` | character | Outs Above Average. |
| `n_fieldout_5stars` | character | 5-star (hardest) plays made. |
| `n_opp_5stars` | character | 5-star play opportunities. |
| `n_5star_percent` | character | Number of 5star rate. |
| `n_fieldout_4stars` | character | Number of fieldout 4stars. |
| `n_opp_4stars` | character | Number of opp 4stars. |
| `n_4star_percent` | character | Number of 4star rate. |
| `n_fieldout_3stars` | character | Number of fieldout 3stars. |
| `n_opp_3stars` | character | Number of opp 3stars. |
| `n_3star_percent` | character | Number of 3star rate. |
| `n_fieldout_2stars` | character | Number of fieldout 2stars. |
| `n_opp_2stars` | character | Number of opp 2stars. |
| `n_2star_percent` | character | Number of 2star rate. |
| `n_fieldout_1stars` | character | Number of fieldout 1stars. |
| `n_opp_1stars` | character | Number of opp 1stars. |
| `n_1star_percent` | character | Number of 1star rate. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_catch_probability-example}

```python
mlb_statcast_leaderboard_catch_probability()
```

_Last validated n/a._

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
| `fielder_name` | character | Fielder name. |
| `player_id` | integer | MLBAM player id. |
| `team_name` | character | Team name. |
| `primary_position` | integer | Primary fielding position. |
| `primary_position_name` | character | Primary position name. |
| `total_throws` | integer | Total throws. |
| `total_throws_1b` | integer | Total throws 1b. |
| `total_throws_2b` | integer | Total throws 2b. |
| `total_throws_3b` | integer | Total throws 3b. |
| `total_throws_ss` | integer | Total throws ss. |
| `total_throws_lf` | integer | Total throws lf. |
| `total_throws_cf` | integer | Total throws cf. |
| `total_throws_rf` | integer | Total throws rf. |
| `total_throws_inf` | integer | Total throws inf. |
| `total_throws_of` | integer | Total throws of. |
| `max_arm_strength` | numeric | Max arm strength (mph). |
| `arm_1b` | numeric | Arm 1b. |
| `arm_2b` | character | Arm 2b. |
| `arm_3b` | character | Arm 3b. |
| `arm_ss` | character | Arm ss. |
| `arm_lf` | character | Arm lf. |
| `arm_cf` | character | Arm cf. |
| `arm_rf` | character | Arm rf. |
| `arm_inf` | character | Arm inf. |
| `arm_of` | character | Arm of. |
| `arm_overall` | numeric | Arm overall. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_arm_strength-example}

```python
mlb_statcast_leaderboard_arm_strength()
```

_Last validated n/a._

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
| `entity_name` | character | Player (or team) entity name. |
| `entity_id` | integer | MLBAM id of the player/team entity. |
| `team_id` | integer | MLBAM team id. |
| `age` | integer | Player age. |
| `maxeff_arm_2b_3b_sba` | numeric | Max-effort arm velo to 2B/3B (mph). |
| `exchange_2b_3b_sba` | numeric | Transfer/exchange time (s). |
| `pop_2b_sba_count` | integer | Pop-time sample (throws to 2B). |
| `pop_2b_sba` | numeric | Pop time to 2B on stolen-base attempts (s). |
| `pop_2b_cs` | numeric | Pop 2b cs. |
| `pop_2b_sb` | numeric | Pop 2b sb. |
| `pop_3b_sba_count` | integer | Pop 3b sba count. |
| `pop_3b_sba` | numeric | Pop 3b sba. |
| `pop_3b_cs` | numeric | Pop 3b cs. |
| `pop_3b_sb` | numeric | Pop 3b sb. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_poptime-example}

```python
mlb_statcast_leaderboard_poptime()
```

_Last validated n/a._

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
| `id` | integer | MLBAM player id. |
| `name` | character | Player (or entity) name. |
| `pitches` | integer | Pitches. |
| `rv_tot` | numeric | Total framing run value. |
| `pct_tot` | numeric | Total called-strike rate. |
| `rv_11` | integer | Rv 11. |
| `pct_11` | numeric | Pct 11. |
| `rv_12` | integer | Rv 12. |
| `pct_12` | numeric | Pct 12. |
| `rv_13` | integer | Rv 13. |
| `pct_13` | integer | Pct 13. |
| `rv_14` | integer | Rv 14. |
| `pct_14` | numeric | Pct 14. |
| `rv_16` | integer | Rv 16. |
| `pct_16` | numeric | Pct 16. |
| `rv_17` | integer | Rv 17. |
| `pct_17` | numeric | Pct 17. |
| `rv_18` | integer | Rv 18. |
| `pct_18` | numeric | Pct 18. |
| `rv_19` | integer | Rv 19. |
| `pct_19` | numeric | Pct 19. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_catcher_framing-example}

```python
mlb_statcast_leaderboard_catcher_framing()
```

_Last validated n/a._

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
| `player_id` | integer | MLBAM player id. |
| `player_name` | character | Player name. |
| `team_name` | character | Team name. |
| `start_year` | integer | First season in the range. |
| `end_year` | character | Last season in the range. |
| `pitches` | integer | Pitches. |
| `catcher_blocking_runs` | integer | Catcher blocking runs. |
| `blocks_above_average` | integer | Blocks above average. |
| `n_pbwp` | integer | Number of pbwp. |
| `x_pbwp` | numeric | X pbwp. |
| `blocks_above_average_per_game` | numeric | Blocks above average per game. |
| `freq_pbwp_easy` | numeric | Freq pbwp easy. |
| `freq_pbwp_medium` | numeric | Freq pbwp medium. |
| `freq_pbwp_tough` | numeric | Freq pbwp tough. |
| `diff_pbwp_easy` | numeric | Diff pbwp easy. |
| `diff_pbwp_medium` | numeric | Diff pbwp medium. |
| `diff_pbwp_tough` | numeric | Diff pbwp tough. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_leaderboard_catcher_blocking-example}

```python
mlb_statcast_leaderboard_catcher_blocking()
```

_Last validated n/a._

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

### Example {#mlb_statcast_leaderboard_catcher_throwing-example}

```python
mlb_statcast_leaderboard_catcher_throwing()
```

_Last validated n/a._

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

### Example {#mlb_statcast_leaderboard_catcher_stance-example}

```python
mlb_statcast_leaderboard_catcher_stance()
```

_Last validated n/a._

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

### Example {#mlb_statcast_leaderboard_basestealing_run_value-example}

```python
mlb_statcast_leaderboard_basestealing_run_value()
```

_Last validated n/a._

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

### Example {#mlb_statcast_leaderboard_baserunning_run_value-example}

```python
mlb_statcast_leaderboard_baserunning_run_value()
```

_Last validated n/a._

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

### Example {#mlb_statcast_leaderboard_baserunning-example}

```python
mlb_statcast_leaderboard_baserunning()
```

_Last validated n/a._
