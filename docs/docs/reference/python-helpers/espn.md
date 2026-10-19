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
| `defteam` | character |  |
| `defteam_score` | integer |  |
| `defteam_timeouts_remaining` | integer |  |
| `dome` | integer |  |
| `down` | integer |  |
| `game_half` | character |  |
| `game_id` | character |  |
| `game_seconds_remaining` | double |  |
| `half_seconds_remaining` | double |  |
| `home` | integer |  |
| `home_team` | character |  |
| `outdoors` | integer |  |
| `play_id` | character |  |
| `posteam` | character |  |
| `posteam_score` | integer |  |
| `posteam_timeouts_remaining` | integer |  |
| `receive_2h_ko` | integer |  |
| `retractable` | integer |  |
| `roof` | character |  |
| `score_differential` | integer |  |
| `season` | integer |  |
| `spread_line` | double |  |
| `yardline_100` | integer |  |
| `ydstogo` | integer |  |
| `air_yards` | double |  |
| `kickoff_attempt` | integer |  |
| `play_type` | character |  |
| `qtr` | integer |  |
| `ep` | double |  |
| `td_prob` | double |  |
| `opp_td_prob` | double |  |
| `fg_prob` | double |  |
| `opp_fg_prob` | double |  |
| `safety_prob` | double |  |
| `opp_safety_prob` | double |  |
| `no_score_prob` | double |  |
| `epa` | double |  |
| `qb_epa` | double |  |
| `air_epa` | double |  |
| `yac_epa` | double |  |
| `comp_air_epa` | double |  |
| `comp_yac_epa` | double |  |
| `total_home_comp_air_epa` | double |  |
| `total_away_comp_air_epa` | double |  |
| `total_home_comp_yac_epa` | double |  |
| `total_away_comp_yac_epa` | double |  |
| `total_home_raw_air_epa` | double |  |
| `total_away_raw_air_epa` | double |  |
| `total_home_raw_yac_epa` | double |  |
| `total_away_raw_yac_epa` | double |  |
| `posteam_spread` | double |  |
| `elapsed_share` | double |  |
| `spread_time` | double |  |
| `Diff_Time_Ratio` | double |  |
| `wp` | double |  |
| `vegas_wp` | double |  |
| `home_wp` | double |  |
| `away_wp` | double |  |
| `def_wp` | double |  |
| `vegas_home_wpa` | double |  |
| `vegas_wpa` | double |  |
| `wpa` | double |  |
| `air_wpa` | double |  |
| `yac_wpa` | double |  |
| `comp_air_wpa` | double |  |
| `comp_yac_wpa` | double |  |
| `total_home_comp_air_wpa` | double |  |
| `total_away_comp_air_wpa` | double |  |
| `total_home_comp_yac_wpa` | double |  |
| `total_away_comp_yac_wpa` | double |  |
| `total_home_raw_air_wpa` | double |  |
| `total_away_raw_air_wpa` | double |  |
| `total_home_raw_yac_wpa` | double |  |
| `total_away_raw_yac_wpa` | double |  |
| `cp` | double |  |
| `cpoe` | double |  |
| `xpass` | double |  |
| `pass_oe` | double |  |
| `xyac_epa` | double |  |
| `xyac_mean_yardage` | double |  |
| `xyac_median_yardage` | double |  |
| `xyac_success` | double |  |
| `xyac_fd` | double |  |
| `home_opening_kickoff` | double |  |
| `go_wp` | double |  |
| `first_down_prob` | double |  |
| `wp_succeed` | double |  |
| `wp_fail` | double |  |
| `fg_make_prob` | double |  |
| `make_fg_wp` | double |  |
| `miss_fg_wp` | double |  |
| `fg_wp` | double |  |
| `punt_wp` | double |  |
| `go_boost` | double |  |
| `go_wp_diff` | double |  |
| `punt_wp_diff` | double |  |
| `fg_wp_diff` | double |  |
| `fourth_down_recommendation` | character |  |

**Example**

```python
from sportsdataverse.football.xfl import xfl_pbp

df = xfl_pbp("401517780")
print(df.select("play_id", "epa", "wp").head())
```
