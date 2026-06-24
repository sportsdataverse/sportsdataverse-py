---
title: MLB — MLB Statcast (Baseball Savant)
sidebar_label: MLB Statcast (Baseball Savant)
sidebar_position: 11
---
# MLB — MLB Statcast (Baseball Savant)

`sportsdataverse.mlb` — 39 endpoints.

## `mlb_statcast_leaderboard_expected_stats`

GET /leaderboard/expected_statistics — xBA/xSLG/xwOBA/xISO expected-statistics leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/expected_statistics`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/expected_statistics](https://baseballsavant.mlb.com/leaderboard/expected_statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_expected_stats()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_percentile_rankings`

GET /leaderboard/percentile-rankings — player percentile-ranking sliders (xwOBA/xBA/xSLG/…).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/percentile-rankings`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/percentile-rankings](https://baseballsavant.mlb.com/leaderboard/percentile-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_percentile_rankings()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_sprint_speed`

GET /leaderboard/sprint_speed — sprint-speed (ft/sec) leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/sprint_speed`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/sprint_speed](https://baseballsavant.mlb.com/leaderboard/sprint_speed)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_sprint_speed()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_running_splits`

GET /leaderboard/running_splits — 90-foot running splits leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/running_splits`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/running_splits](https://baseballsavant.mlb.com/leaderboard/running_splits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_running_splits()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_bat_tracking`

GET /leaderboard/bat-tracking — bat-tracking (swing speed / squared-up) leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/bat-tracking`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/bat-tracking](https://baseballsavant.mlb.com/leaderboard/bat-tracking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_bat_tracking()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_swing_path`

GET /leaderboard/bat-tracking/swing-path-attack-angle — swing path & attack-angle leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/bat-tracking/swing-path-attack-angle`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/bat-tracking/swing-path-attack-angle](https://baseballsavant.mlb.com/leaderboard/bat-tracking/swing-path-attack-angle)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_swing_path()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_swing_timing`

GET /leaderboard/bat-tracking/swing-timing-miss-distance — swing timing & miss-distance leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/bat-tracking/swing-timing-miss-distance`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/bat-tracking/swing-timing-miss-distance](https://baseballsavant.mlb.com/leaderboard/bat-tracking/swing-timing-miss-distance)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_swing_timing()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_swing_take`

GET /leaderboard/swing-take — swing/take run-value leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/swing-take`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/swing-take](https://baseballsavant.mlb.com/leaderboard/swing-take)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_swing_take()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_exit_velocity_barrels`

GET /leaderboard/statcast — exit velocity & barrels leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/statcast`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/statcast](https://baseballsavant.mlb.com/leaderboard/statcast)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_exit_velocity_barrels()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_batted_ball`

GET /leaderboard/batted-ball — batted-ball profile leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/batted-ball`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/batted-ball](https://baseballsavant.mlb.com/leaderboard/batted-ball)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_batted_ball()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_home_runs`

GET /leaderboard/home-runs — Statcast home-runs leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/home-runs`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/home-runs](https://baseballsavant.mlb.com/leaderboard/home-runs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_home_runs()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_pitch_arsenals`

GET /leaderboard/pitch-arsenals — pitch arsenals (velo/spin/movement) leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitch-arsenals`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitch-arsenals](https://baseballsavant.mlb.com/leaderboard/pitch-arsenals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_pitch_arsenals()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_pitch_arsenal_stats`

GET /leaderboard/pitch-arsenal-stats — per-pitch-type outcome stats leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats](https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_pitch_arsenal_stats()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_pitch_movement`

GET /leaderboard/pitch-movement — pitch-movement leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitch-movement`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitch-movement](https://baseballsavant.mlb.com/leaderboard/pitch-movement)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_pitch_movement()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_pitch_tempo`

GET /leaderboard/pitch-tempo — pitch-tempo leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitch-tempo`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitch-tempo](https://baseballsavant.mlb.com/leaderboard/pitch-tempo)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_pitch_tempo()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_active_spin`

GET /leaderboard/active-spin — active-spin leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/active-spin`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/active-spin](https://baseballsavant.mlb.com/leaderboard/active-spin)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_active_spin()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_spin_direction`

GET /leaderboard/spin-direction-pitches — spin-direction (per-pitch) leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/spin-direction-pitches`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/spin-direction-pitches](https://baseballsavant.mlb.com/leaderboard/spin-direction-pitches)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_spin_direction()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_arm_angles`

GET /leaderboard/pitcher-arm-angles — pitcher arm-angle leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitcher-arm-angles`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitcher-arm-angles](https://baseballsavant.mlb.com/leaderboard/pitcher-arm-angles)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_arm_angles()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_pitcher_running_game`

GET /leaderboard/pitcher-running-game — pitcher running-game (holding runners) leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitcher-running-game`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitcher-running-game](https://baseballsavant.mlb.com/leaderboard/pitcher-running-game)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_pitcher_running_game()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_outs_above_average`

GET /leaderboard/outs_above_average — Outs Above Average (OAA) fielding leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/outs_above_average`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/outs_above_average](https://baseballsavant.mlb.com/leaderboard/outs_above_average)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_outs_above_average()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_outfield_directional_oaa`

GET /leaderboard/outfield_directional_outs_above_average — outfield directional OAA leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/outfield_directional_outs_above_average`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/outfield_directional_outs_above_average](https://baseballsavant.mlb.com/leaderboard/outfield_directional_outs_above_average)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_outfield_directional_oaa()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_outfield_jump`

GET /leaderboard/outfield_jump — outfielder jump leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/outfield_jump`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/outfield_jump](https://baseballsavant.mlb.com/leaderboard/outfield_jump)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_outfield_jump()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_catch_probability`

GET /leaderboard/catch_probability — outfielder catch-probability leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/catch_probability`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/catch_probability](https://baseballsavant.mlb.com/leaderboard/catch_probability)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_catch_probability()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_arm_strength`

GET /leaderboard/arm-strength — fielder arm-strength leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/arm-strength`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/arm-strength](https://baseballsavant.mlb.com/leaderboard/arm-strength)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_arm_strength()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_poptime`

GET /leaderboard/poptime — catcher pop-time leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/poptime`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/poptime](https://baseballsavant.mlb.com/leaderboard/poptime)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_poptime()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_catcher_framing`

GET /leaderboard/catcher-framing — catcher framing leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/catcher-framing`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/catcher-framing](https://baseballsavant.mlb.com/leaderboard/catcher-framing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_catcher_framing()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_catcher_blocking`

GET /leaderboard/catcher-blocking — catcher blocking leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/catcher-blocking`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/catcher-blocking](https://baseballsavant.mlb.com/leaderboard/catcher-blocking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_catcher_blocking()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_catcher_throwing`

GET /leaderboard/catcher-throwing — catcher throwing leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/catcher-throwing`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/catcher-throwing](https://baseballsavant.mlb.com/leaderboard/catcher-throwing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_catcher_throwing()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_catcher_stance`

GET /leaderboard/catcher-stance — catcher stance leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/catcher-stance`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/catcher-stance](https://baseballsavant.mlb.com/leaderboard/catcher-stance)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_catcher_stance()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_basestealing_run_value`

GET /leaderboard/basestealing-run-value — basestealing run-value leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/basestealing-run-value`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/basestealing-run-value](https://baseballsavant.mlb.com/leaderboard/basestealing-run-value)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_basestealing_run_value()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_baserunning_run_value`

GET /leaderboard/baserunning-run-value — baserunning run-value leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/baserunning-run-value`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/baserunning-run-value](https://baseballsavant.mlb.com/leaderboard/baserunning-run-value)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_baserunning_run_value()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_baserunning`

GET /leaderboard/baserunning — extra-bases-taken run-value leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/baserunning`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/baserunning](https://baseballsavant.mlb.com/leaderboard/baserunning)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_baserunning()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_year_to_year`

GET /leaderboard/statcast-year-to-year — year-to-year metric change leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/statcast-year-to-year`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/statcast-year-to-year](https://baseballsavant.mlb.com/leaderboard/statcast-year-to-year)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_year_to_year()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_timer_infractions`

GET /leaderboard/pitch-timer-infractions — pitch-timer infractions leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitch-timer-infractions`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitch-timer-infractions](https://baseballsavant.mlb.com/leaderboard/pitch-timer-infractions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_timer_infractions()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_custom`

GET /leaderboard/custom — build-your-own metric leaderboard (comma-separated selections).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/custom`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/custom](https://baseballsavant.mlb.com/leaderboard/custom)

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

### Returns

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

### Example

```python
mlb_statcast_leaderboard_custom()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_fielding_run_value`

GET /leaderboard/fielding-run-value — fielding run-value leaderboard (HTML-embedded JSON).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/fielding-run-value`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/fielding-run-value](https://baseballsavant.mlb.com/leaderboard/fielding-run-value)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_fielding_run_value()
```

_Last validated n/a._

## `mlb_statcast_leaderboard_park_factors`

GET /leaderboard/statcast-park-factors — Statcast park-factors leaderboard (HTML-embedded JSON).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/statcast-park-factors`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/statcast-park-factors](https://baseballsavant.mlb.com/leaderboard/statcast-park-factors)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `team` | `team` |  |  | `Y` | team query parameter. |

### Returns

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

### Example

```python
mlb_statcast_leaderboard_park_factors()
```

_Last validated n/a._

## `mlb_statcast_gamefeed`

GET /gf — Savant per-game JSON feed (pitch-by-pitch tracking).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/gf`

**Valid URL:** [https://baseballsavant.mlb.com/gf](https://baseballsavant.mlb.com/gf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  |  | `Y` | game_pk query parameter. |
| `at_bat_number` | `at_bat_number` |  |  | `Y` | at_bat_number query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `type` | character | Record/pitch type. |
| `year` | character | Season year. |
| `sport_id` | character | Sport id. |
| `play_id` | character | Statcast play UUID. |
| `inning` | character | Inning. |
| `half_inning` | character | Half inning. |
| `ab_number` | character | Ab number. |
| `cap_index` | character | Cap index. |
| `outs` | character | Outs. |
| `batter` | character | MLBAM id of the batter. |
| `stand` | character | Batter stance side (R/L). |
| `batter_name` | character | Batter name. |
| `pitcher` | character | MLBAM id of the pitcher. |
| `p_throws` | character | Pitcher throwing hand (R/L). |
| `pitcher_name` | character | Pitcher name. |
| `catcher` | character | Catcher. |
| `catcher_name` | character | Catcher name. |
| `team_batting` | character | Team batting. |
| `team_fielding` | character | Team fielding. |
| `team_batting_id` | character | Team batting id. |
| `team_fielding_id` | character | Team fielding id. |
| `result` | character | Result. |
| `des` | character | Des. |
| `events` | character | Events. |
| `strikes` | character | Strikes. |
| `balls` | character | Balls. |
| `pre_strikes` | character | Pre strikes. |
| `pre_balls` | character | Pre balls. |
| `call` | character | Call. |
| `call_name` | character | Call name. |
| `pitch_type` | character | Pitch type code. |
| `pitch_name` | character | Pitch type name. |
| `description` | character | Description. |
| `result_code` | character | Result code. |
| `pitch_call` | character | Pitch call. |
| `is_strike_swinging` | character | Is strike swinging. |
| `balls_and_strikes` | character | Balls and strikes. |
| `start_speed` | character | Start speed. |
| `end_speed` | character | End speed. |
| `sz_top` | character | Sz top. |
| `sz_bot` | character | Sz bot. |
| `extension` | character | Release extension (ft). |
| `plate_time` | character | Plate time. |
| `zone` | character | Zone. |
| `spin_rate` | character | Spin rate (rpm). |
| `break_x` | character | Break x. |
| `induced_break_z` | character | Induced break z. |
| `break_z` | character | Break z. |
| `px` | character | Px. |
| `pz` | character | Pz. |
| `pfx_x` | character | Horizontal movement (in, pitcher perspective). |
| `pfx_z` | character | Induced vertical movement (in). |
| `is_bip_out` | character | Is bip out. |
| `pitch_number` | character | Pitch number. |
| `plate_x` | character | Plate x. |
| `plate_z` | character | Plate z. |
| `hit_speed` | character | Hit speed. |
| `hit_distance` | character | Hit distance. |
| `xba` | character | Expected batting average. |
| `hit_angle` | character | Hit angle. |
| `is_barrel` | character | Is barrel. |
| `hc_x` | character | Hc x. |
| `hc_y` | character | Hc y. |
| `launch_speed` | character | Exit velocity of the batted ball (mph). |
| `launch_angle` | character | Launch angle (deg). |
| `game_total_pitches` | character | Game total pitches. |
| `game_pk` | character | MLBAM game id. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_statcast_gamefeed()
```

_Last validated n/a._

## `mlb_statcast_schedule`

GET /schedule — Savant schedule feed (one row per game).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/schedule`

**Valid URL:** [https://baseballsavant.mlb.com/schedule](https://baseballsavant.mlb.com/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_pk` | integer | MLBAM game id. |
| `game_guid` | character | Game GUID. |
| `link` | character | Stats API resource link. |
| `game_type` | character | Game type code (R/F/D/L/W/S/E/A). |
| `season` | character | Season year. |
| `game_date` | character | Game date/time (ISO 8601, UTC offset). |
| `official_date` | character | Official game date (YYYY-MM-DD). |
| `game_number` | integer | Game number (1, or 2 for the nightcap of a doubleheader). |
| `public_facing` | logical | Public facing. |
| `double_header` | character | Doubleheader flag (Y/N/S). |
| `gameday_type` | character | Gameday type. |
| `tiebreaker` | character | Tiebreaker. |
| `calendar_event_id` | character | Calendar event id. |
| `season_display` | character | Season display. |
| `day_night` | character | Day or night game. |
| `scheduled_innings` | integer | Scheduled innings (usually 9). |
| `reverse_home_away_status` | logical | Reverse home away status. |
| `inning_break_length` | integer | Inning break length. |
| `games_in_series` | integer | Total games in the series. |
| `series_game_number` | integer | Game number within the series. |
| `series_description` | character | Series description. |
| `record_source` | character | Record source. |
| `if_necessary` | character | If necessary. |
| `if_necessary_description` | character | If necessary description. |
| `status_abstract_game_state` | character | Status abstract game state. |
| `status_coded_game_state` | character | Status coded game state. |
| `status_detailed_state` | character | Status detailed state. |
| `status_status_code` | character | Status status code. |
| `status_start_time_tbd` | logical | Status start time tbd. |
| `status_abstract_game_code` | character | Status abstract game code. |
| `teams_away_team_spring_league_id` | integer | Away team team spring league id. |
| `teams_away_team_spring_league_name` | character | Away team team spring league name. |
| `teams_away_team_spring_league_link` | character | Away team team spring league link. |
| `teams_away_team_spring_league_abbreviation` | character | Away team team spring league abbreviation. |
| `teams_away_team_all_star_status` | character | Away team team all star status. |
| `teams_away_team_id` | integer | Away team team id. |
| `teams_away_team_name` | character | Away team team name. |
| `teams_away_team_link` | character | Away team team link. |
| `teams_away_team_season` | integer | Away team team season. |
| `teams_away_team_venue_id` | integer | Away team team venue id. |
| `teams_away_team_venue_name` | character | Away team team venue name. |
| `teams_away_team_venue_link` | character | Away team team venue link. |
| `teams_away_team_spring_venue_id` | integer | Away team team spring venue id. |
| `teams_away_team_spring_venue_link` | character | Away team team spring venue link. |
| `teams_away_team_team_code` | character | Away team team team code. |
| `teams_away_team_file_code` | character | Away team team file code. |
| `teams_away_team_abbreviation` | character | Away team team abbreviation. |
| `teams_away_team_team_name` | character | Away team team team name. |
| `teams_away_team_location_name` | character | Away team team location name. |
| `teams_away_team_first_year_of_play` | character | Away team team first year of play. |
| `teams_away_team_league_id` | integer | Away team team league id. |
| `teams_away_team_league_name` | character | Away team team league name. |
| `teams_away_team_league_link` | character | Away team team league link. |
| `teams_away_team_division_id` | integer | Away team team division id. |
| `teams_away_team_division_name` | character | Away team team division name. |
| `teams_away_team_division_link` | character | Away team team division link. |
| `teams_away_team_sport_id` | integer | Away team team sport id. |
| `teams_away_team_sport_link` | character | Away team team sport link. |
| `teams_away_team_sport_name` | character | Away team team sport name. |
| `teams_away_team_short_name` | character | Away team team short name. |
| `teams_away_team_franchise_name` | character | Away team team franchise name. |
| `teams_away_team_club_name` | character | Away team team club name. |
| `teams_away_team_active` | logical | Away team team active. |
| `teams_away_league_record_wins` | integer | Away team league record wins. |
| `teams_away_league_record_losses` | integer | Away team league record losses. |
| `teams_away_league_record_ties` | integer | Away team league record ties. |
| `teams_away_league_record_pct` | character | Away team league record rate. |
| `teams_away_probable_pitcher_id` | integer | Away team probable pitcher id. |
| `teams_away_probable_pitcher_full_name` | character | Away team probable pitcher full name. |
| `teams_away_probable_pitcher_link` | character | Away team probable pitcher link. |
| `teams_away_probable_pitcher_first_name` | character | Away team probable pitcher first name. |
| `teams_away_probable_pitcher_last_name` | character | Away team probable pitcher last name. |
| `teams_away_probable_pitcher_primary_number` | character | Away team probable pitcher primary number. |
| `teams_away_probable_pitcher_birth_date` | character | Away team probable pitcher birth date. |
| `teams_away_probable_pitcher_current_age` | integer | Away team probable pitcher current age. |
| `teams_away_probable_pitcher_birth_city` | character | Away team probable pitcher birth city. |
| `teams_away_probable_pitcher_birth_state_province` | character | Away team probable pitcher birth state province. |
| `teams_away_probable_pitcher_birth_country` | character | Away team probable pitcher birth country. |
| `teams_away_probable_pitcher_height` | character | Away team probable pitcher height. |
| `teams_away_probable_pitcher_weight` | integer | Away team probable pitcher weight. |
| `teams_away_probable_pitcher_active` | logical | Away team probable pitcher active. |
| `teams_away_probable_pitcher_primary_position_code` | character | Away team probable pitcher primary position code. |
| `teams_away_probable_pitcher_primary_position_name` | character | Away team probable pitcher primary position name. |
| `teams_away_probable_pitcher_primary_position_type` | character | Away team probable pitcher primary position type. |
| `teams_away_probable_pitcher_primary_position_abbreviation` | character | Away team probable pitcher primary position abbreviation. |
| `teams_away_probable_pitcher_use_name` | character | Away team probable pitcher use name. |
| `teams_away_probable_pitcher_use_last_name` | character | Away team probable pitcher use last name. |
| `teams_away_probable_pitcher_middle_name` | character | Away team probable pitcher middle name. |
| `teams_away_probable_pitcher_boxscore_name` | character | Away team probable pitcher boxscore name. |
| `teams_away_probable_pitcher_gender` | character | Away team probable pitcher gender. |
| `teams_away_probable_pitcher_is_player` | logical | Away team probable pitcher is player. |
| `teams_away_probable_pitcher_is_verified` | logical | Away team probable pitcher is verified. |
| `teams_away_probable_pitcher_draft_year` | integer | Away team probable pitcher draft year. |
| `teams_away_probable_pitcher_mlb_debut_date` | character | Away team probable pitcher mlb debut date. |
| `teams_away_probable_pitcher_bat_side_code` | character | Away team probable pitcher bat side code. |
| `teams_away_probable_pitcher_bat_side_description` | character | Away team probable pitcher bat side description. |
| `teams_away_probable_pitcher_pitch_hand_code` | character | Away team probable pitcher pitch hand code. |
| `teams_away_probable_pitcher_pitch_hand_description` | character | Away team probable pitcher pitch hand description. |
| `teams_away_probable_pitcher_name_first_last` | character | Away team probable pitcher name first last. |
| `teams_away_probable_pitcher_name_slug` | character | Away team probable pitcher name slug. |
| `teams_away_probable_pitcher_first_last_name` | character | Away team probable pitcher first last name. |
| `teams_away_probable_pitcher_last_first_name` | character | Away team probable pitcher last first name. |
| `teams_away_probable_pitcher_last_init_name` | character | Away team probable pitcher last init name. |
| `teams_away_probable_pitcher_init_last_name` | character | Away team probable pitcher init last name. |
| `teams_away_probable_pitcher_full_fml_name` | character | Away team probable pitcher full fml name. |
| `teams_away_probable_pitcher_full_lfm_name` | character | Away team probable pitcher full lfm name. |
| `teams_away_probable_pitcher_strike_zone_top` | numeric | Away team probable pitcher strike zone top. |
| `teams_away_probable_pitcher_strike_zone_bottom` | numeric | Away team probable pitcher strike zone bottom. |
| `teams_away_split_squad` | logical | Away team split squad. |
| `teams_away_series_number` | integer | Away team series number. |
| `teams_away_spring_league_id` | integer | Away team spring league id. |
| `teams_away_spring_league_name` | character | Away team spring league name. |
| `teams_away_spring_league_link` | character | Away team spring league link. |
| `teams_away_spring_league_abbreviation` | character | Away team spring league abbreviation. |
| `teams_home_team_spring_league_id` | integer | Home team team spring league id. |
| `teams_home_team_spring_league_name` | character | Home team team spring league name. |
| `teams_home_team_spring_league_link` | character | Home team team spring league link. |
| `teams_home_team_spring_league_abbreviation` | character | Home team team spring league abbreviation. |
| `teams_home_team_all_star_status` | character | Home team team all star status. |
| `teams_home_team_id` | integer | Home team team id. |
| `teams_home_team_name` | character | Home team team name. |
| `teams_home_team_link` | character | Home team team link. |
| `teams_home_team_season` | integer | Home team team season. |
| `teams_home_team_venue_id` | integer | Home team team venue id. |
| `teams_home_team_venue_name` | character | Home team team venue name. |
| `teams_home_team_venue_link` | character | Home team team venue link. |
| `teams_home_team_spring_venue_id` | integer | Home team team spring venue id. |
| `teams_home_team_spring_venue_link` | character | Home team team spring venue link. |
| `teams_home_team_team_code` | character | Home team team team code. |
| `teams_home_team_file_code` | character | Home team team file code. |
| `teams_home_team_abbreviation` | character | Home team team abbreviation. |
| `teams_home_team_team_name` | character | Home team team team name. |
| `teams_home_team_location_name` | character | Home team team location name. |
| `teams_home_team_first_year_of_play` | character | Home team team first year of play. |
| `teams_home_team_league_id` | integer | Home team team league id. |
| `teams_home_team_league_name` | character | Home team team league name. |
| `teams_home_team_league_link` | character | Home team team league link. |
| `teams_home_team_division_id` | integer | Home team team division id. |
| `teams_home_team_division_name` | character | Home team team division name. |
| `teams_home_team_division_link` | character | Home team team division link. |
| `teams_home_team_sport_id` | integer | Home team team sport id. |
| `teams_home_team_sport_link` | character | Home team team sport link. |
| `teams_home_team_sport_name` | character | Home team team sport name. |
| `teams_home_team_short_name` | character | Home team team short name. |
| `teams_home_team_franchise_name` | character | Home team team franchise name. |
| `teams_home_team_club_name` | character | Home team team club name. |
| `teams_home_team_active` | logical | Home team team active. |
| `teams_home_league_record_wins` | integer | Home team league record wins. |
| `teams_home_league_record_losses` | integer | Home team league record losses. |
| `teams_home_league_record_ties` | integer | Home team league record ties. |
| `teams_home_league_record_pct` | character | Home team league record rate. |
| `teams_home_probable_pitcher_id` | integer | Home team probable pitcher id. |
| `teams_home_probable_pitcher_full_name` | character | Home team probable pitcher full name. |
| `teams_home_probable_pitcher_link` | character | Home team probable pitcher link. |
| `teams_home_probable_pitcher_first_name` | character | Home team probable pitcher first name. |
| `teams_home_probable_pitcher_last_name` | character | Home team probable pitcher last name. |
| `teams_home_probable_pitcher_primary_number` | character | Home team probable pitcher primary number. |
| `teams_home_probable_pitcher_birth_date` | character | Home team probable pitcher birth date. |
| `teams_home_probable_pitcher_current_age` | integer | Home team probable pitcher current age. |
| `teams_home_probable_pitcher_birth_city` | character | Home team probable pitcher birth city. |
| `teams_home_probable_pitcher_birth_state_province` | character | Home team probable pitcher birth state province. |
| `teams_home_probable_pitcher_birth_country` | character | Home team probable pitcher birth country. |
| `teams_home_probable_pitcher_height` | character | Home team probable pitcher height. |
| `teams_home_probable_pitcher_weight` | integer | Home team probable pitcher weight. |
| `teams_home_probable_pitcher_active` | logical | Home team probable pitcher active. |
| `teams_home_probable_pitcher_primary_position_code` | character | Home team probable pitcher primary position code. |
| `teams_home_probable_pitcher_primary_position_name` | character | Home team probable pitcher primary position name. |
| `teams_home_probable_pitcher_primary_position_type` | character | Home team probable pitcher primary position type. |
| `teams_home_probable_pitcher_primary_position_abbreviation` | character | Home team probable pitcher primary position abbreviation. |
| `teams_home_probable_pitcher_use_name` | character | Home team probable pitcher use name. |
| `teams_home_probable_pitcher_use_last_name` | character | Home team probable pitcher use last name. |
| `teams_home_probable_pitcher_middle_name` | character | Home team probable pitcher middle name. |
| `teams_home_probable_pitcher_boxscore_name` | character | Home team probable pitcher boxscore name. |
| `teams_home_probable_pitcher_nick_name` | character | Home team probable pitcher nick name. |
| `teams_home_probable_pitcher_gender` | character | Home team probable pitcher gender. |
| `teams_home_probable_pitcher_is_player` | logical | Home team probable pitcher is player. |
| `teams_home_probable_pitcher_is_verified` | logical | Home team probable pitcher is verified. |
| `teams_home_probable_pitcher_draft_year` | integer | Home team probable pitcher draft year. |
| `teams_home_probable_pitcher_mlb_debut_date` | character | Home team probable pitcher mlb debut date. |
| `teams_home_probable_pitcher_bat_side_code` | character | Home team probable pitcher bat side code. |
| `teams_home_probable_pitcher_bat_side_description` | character | Home team probable pitcher bat side description. |
| `teams_home_probable_pitcher_pitch_hand_code` | character | Home team probable pitcher pitch hand code. |
| `teams_home_probable_pitcher_pitch_hand_description` | character | Home team probable pitcher pitch hand description. |
| `teams_home_probable_pitcher_name_first_last` | character | Home team probable pitcher name first last. |
| `teams_home_probable_pitcher_name_slug` | character | Home team probable pitcher name slug. |
| `teams_home_probable_pitcher_first_last_name` | character | Home team probable pitcher first last name. |
| `teams_home_probable_pitcher_last_first_name` | character | Home team probable pitcher last first name. |
| `teams_home_probable_pitcher_last_init_name` | character | Home team probable pitcher last init name. |
| `teams_home_probable_pitcher_init_last_name` | character | Home team probable pitcher init last name. |
| `teams_home_probable_pitcher_full_fml_name` | character | Home team probable pitcher full fml name. |
| `teams_home_probable_pitcher_full_lfm_name` | character | Home team probable pitcher full lfm name. |
| `teams_home_probable_pitcher_strike_zone_top` | numeric | Home team probable pitcher strike zone top. |
| `teams_home_probable_pitcher_strike_zone_bottom` | numeric | Home team probable pitcher strike zone bottom. |
| `teams_home_split_squad` | logical | Home team split squad. |
| `teams_home_series_number` | integer | Home team series number. |
| `teams_home_spring_league_id` | integer | Home team spring league id. |
| `teams_home_spring_league_name` | character | Home team spring league name. |
| `teams_home_spring_league_link` | character | Home team spring league link. |
| `teams_home_spring_league_abbreviation` | character | Home team spring league abbreviation. |
| `linescore_scheduled_innings` | integer | Linescore scheduled innings. |
| `linescore_innings` | character | Linescore innings. |
| `linescore_defense_team_id` | integer | Linescore defense team id. |
| `linescore_defense_team_name` | character | Linescore defense team name. |
| `linescore_defense_team_link` | character | Linescore defense team link. |
| `linescore_offense_team_id` | integer | Linescore offense team id. |
| `linescore_offense_team_name` | character | Linescore offense team name. |
| `linescore_offense_team_link` | character | Linescore offense team link. |
| `venue_id` | integer | MLBAM venue id. |
| `venue_name` | character | Ballpark name. |
| `venue_link` | character | Venue link. |
| `content_link` | character | Content link. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_statcast_schedule()
```

_Last validated n/a._
