---
title: "MLB — MLB Statcast (Baseball Savant) — Leaderboard (2)"
sidebar_label: "Leaderboard (2)"
sidebar_position: 2
description: "MLB — MLB Statcast (Baseball Savant) — Leaderboard (2) — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Statcast (Baseball Savant) — Leaderboard (2)

## mlb_statcast_leaderboard_timer_infractions

GET /leaderboard/pitch-timer-infractions — pitch-timer infractions leaderboard.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/pitch-timer-infractions`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/pitch-timer-infractions](https://baseballsavant.mlb.com/leaderboard/pitch-timer-infractions)

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

### Example {#mlb_statcast_leaderboard_timer_infractions-example}

```python
mlb_statcast_leaderboard_timer_infractions()
```

_Last validated n/a._

## mlb_statcast_leaderboard_custom

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

### Example {#mlb_statcast_leaderboard_custom-example}

```python
mlb_statcast_leaderboard_custom()
```

_Last validated n/a._

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

### Example {#mlb_statcast_leaderboard_fielding_run_value-example}

```python
mlb_statcast_leaderboard_fielding_run_value()
```

_Last validated n/a._

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

### Example {#mlb_statcast_leaderboard_park_factors-example}

```python
mlb_statcast_leaderboard_park_factors()
```

_Last validated n/a._
