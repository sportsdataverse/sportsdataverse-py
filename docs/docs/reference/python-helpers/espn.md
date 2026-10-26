---
title: "Package — additional Python functions — ESPN"
sidebar_label: "ESPN"
sidebar_position: 1
description: "Package — additional Python functions — ESPN — function reference in sdv-py, the SportsDataverse Python package."
---
# Package — additional Python functions — ESPN

### xfl_pbp {#xfl_pbp}

`xfl_pbp(game_id: 'Union[str, int]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Enriched XFL play-by-play (EP/EPA/WP/WPA/CP/CPOE).

Fetches the ESPN game summary, unrolls its drives into an nflverse-shape
frame, and scores it with the same parity-validated NFL EP/WP pipeline
used league-wide (see
`sportsdataverse.football.spring_football_ep_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[str, int]` |  | ESPN XFL event id. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

One row per play with `ep`/`epa`/`wp`/`wpa`/`cp`/`cpoe` and the other `enrich_nfl_pbp` output columns. Zero rows for a game ESPN has no play-by-play for.

| col_name | type | description |
|---|---|---|
| `defteam` | String |  |
| `defteam_score` | Int64 |  |
| `defteam_timeouts_remaining` | Int64 |  |
| `dome` | Int64 |  |
| `down` | Int64 |  |
| `game_half` | String |  |
| `game_id` | String |  |
| `game_seconds_remaining` | Float64 |  |
| `half_seconds_remaining` | Float64 |  |
| `home` | Int64 |  |
| `home_team` | String |  |
| `outdoors` | Int64 |  |
| `play_id` | String |  |
| `posteam` | String |  |
| `posteam_score` | Int64 |  |
| `posteam_timeouts_remaining` | Int64 |  |
| `receive_2h_ko` | Int64 |  |
| `retractable` | Int64 |  |
| `roof` | String |  |
| `score_differential` | Int64 |  |
| `season` | Int64 |  |
| `spread_line` | Float64 |  |
| `yardline_100` | Int64 |  |
| `ydstogo` | Int64 |  |
| `air_yards` | Float64 |  |
| `kickoff_attempt` | Int64 |  |
| `play_type` | String |  |
| `qtr` | Int64 |  |
| `ep` | Float64 |  |
| `td_prob` | Float64 |  |
| `opp_td_prob` | Float64 |  |
| `fg_prob` | Float64 |  |
| `opp_fg_prob` | Float64 |  |
| `safety_prob` | Float64 |  |
| `opp_safety_prob` | Float64 |  |
| `no_score_prob` | Float64 |  |
| `epa` | Float64 |  |
| `qb_epa` | Float64 |  |
| `air_epa` | Float64 |  |
| `yac_epa` | Float64 |  |
| `comp_air_epa` | Float64 |  |
| `comp_yac_epa` | Float64 |  |
| `total_home_comp_air_epa` | Float64 |  |
| `total_away_comp_air_epa` | Float64 |  |
| `total_home_comp_yac_epa` | Float64 |  |
| `total_away_comp_yac_epa` | Float64 |  |
| `total_home_raw_air_epa` | Float64 |  |
| `total_away_raw_air_epa` | Float64 |  |
| `total_home_raw_yac_epa` | Float64 |  |
| `total_away_raw_yac_epa` | Float64 |  |
| `posteam_spread` | Float64 |  |
| `elapsed_share` | Float64 |  |
| `spread_time` | Float64 |  |
| `Diff_Time_Ratio` | Float64 |  |
| `wp` | Float64 |  |
| `vegas_wp` | Float64 |  |
| `home_wp` | Float64 |  |
| `away_wp` | Float64 |  |
| `def_wp` | Float64 |  |
| `vegas_home_wpa` | Float64 |  |
| `vegas_wpa` | Float64 |  |
| `wpa` | Float64 |  |
| `air_wpa` | Float64 |  |
| `yac_wpa` | Float64 |  |
| `comp_air_wpa` | Float64 |  |
| `comp_yac_wpa` | Float64 |  |
| `total_home_comp_air_wpa` | Float64 |  |
| `total_away_comp_air_wpa` | Float64 |  |
| `total_home_comp_yac_wpa` | Float64 |  |
| `total_away_comp_yac_wpa` | Float64 |  |
| `total_home_raw_air_wpa` | Float64 |  |
| `total_away_raw_air_wpa` | Float64 |  |
| `total_home_raw_yac_wpa` | Float64 |  |
| `total_away_raw_yac_wpa` | Float64 |  |
| `cp` | Float64 |  |
| `cpoe` | Float64 |  |
| `xpass` | Float64 |  |
| `pass_oe` | Float64 |  |
| `xyac_epa` | Float64 |  |
| `xyac_mean_yardage` | Float64 |  |
| `xyac_median_yardage` | Float64 |  |
| `xyac_success` | Float64 |  |
| `xyac_fd` | Float64 |  |
| `home_opening_kickoff` | Float64 |  |
| `go_wp` | Float64 |  |
| `first_down_prob` | Float64 |  |
| `wp_succeed` | Float64 |  |
| `wp_fail` | Float64 |  |
| `fg_make_prob` | Float64 |  |
| `make_fg_wp` | Float64 |  |
| `miss_fg_wp` | Float64 |  |
| `fg_wp` | Float64 |  |
| `punt_wp` | Float64 |  |
| `go_boost` | Float64 |  |
| `go_wp_diff` | Float64 |  |
| `punt_wp_diff` | Float64 |  |
| `fg_wp_diff` | Float64 |  |
| `fourth_down_recommendation` | String |  |

**Example**

```python
from sportsdataverse.football.xfl import xfl_pbp

df = xfl_pbp("401517780")
print(df.select("play_id", "epa", "wp").head())
```
