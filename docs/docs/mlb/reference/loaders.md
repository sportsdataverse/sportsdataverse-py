---
title: MLB dataset loaders
sidebar_label: Loaders
description: "MLB dataset loaders in sdv-py: the load_* functions that read the SportsDataverse release assets."
sidebar_position: 1
---
# MLB dataset loaders

```mermaid
flowchart LR
  raw["scrape / raw"] --> enrich["enrich"] --> rel["release asset"] --> load["load_*()"]
```

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_mlb_re24_matrix` | [mlb_game_state](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_game_state) | — |
| `load_mlb_we_table` | [mlb_game_state](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_game_state) | — |
| `load_mlb_wpa` | [mlb_game_state](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_game_state) | — |
| `load_mlb_pbp` | [mlb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pbp) | — |
| `load_mlb_pitches` | [mlb_pitches](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitches) | — |
| `load_mlb_runners` | [mlb_runners](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_runners) | — |
| `load_mlb_expected_stats` | [mlb_hitting_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_hitting_models) | — |
| `load_mlb_expected_hr` | [mlb_hitting_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_hitting_models) | — |
| `load_mlb_batter_projection` | [mlb_hitting_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_hitting_models) | — |
| `load_mlb_oaa` | [mlb_fielding_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_fielding_models) | — |
| `load_mlb_catcher_framing` | [mlb_fielding_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_fielding_models) | — |
| `load_mlb_xera` | [mlb_pitching_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitching_models) | — |
| `load_mlb_stuff_plus` | [mlb_pitching_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitching_models) | — |
| `load_mlb_command_plus` | [mlb_pitching_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitching_models) | — |
| `load_ncaa_baseball_pbp` | [ncaa_baseball_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_pbp) | — |
| `load_ncaa_baseball_schedule` | [ncaa_baseball_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_schedules) | — |
| `load_ncaa_baseball_teams` | [ncaa_baseball_teams](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_teams) | — |
| `load_ncaa_baseball_rosters` | [ncaa_baseball_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_rosters) | — |
| `load_ncaa_baseball_linescore` | [ncaa_baseball_linescore](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_linescore) | — |
| `load_ncaa_baseball_team_stats` | [ncaa_baseball_team_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_team_stats) | — |
| `load_ncaa_baseball_player_stats` | [ncaa_baseball_player_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_player_stats) | — |
| `load_ncaa_baseball_situational_stats` | [ncaa_baseball_situational_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_situational_stats) | — |
| `load_ncaa_baseball_games` | [ncaa_baseball_games](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_games) | — |

## `load_mlb_re24_matrix`

Release: [mlb_game_state](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_game_state) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_game_state/mlb_re24_matrix_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `base_state` | String | Three-character pre-play base occupancy where each slot carries its base number when occupied and an underscore when empty, so ___ is bases empty and 123 is bases loaded. |
| `outs` | Int64 | Outs in the inning after the play. |
| `re` | Float64 | Mean runs the batting team went on to score from this base-out state through the end of the half-inning, with bottom-of-the-9th-and-later halves excluded to avoid walk-off selection bias. |
| `n` | UInt32 | Plate appearances observed starting in this base-out state, the sample size behind re. |
| `season` | Int64 | Season year. |

```python
load_mlb_re24_matrix(seasons=2024)
```

## `load_mlb_we_table`

Release: [mlb_game_state](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_game_state) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_game_state/mlb_we_table_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `inning_capped` | Int64 | Inning number with the ninth and every extra inning collapsed into 9, so extras share the ninth-inning win-expectancy cells. |
| `half` | String | Half of the game (1 or 2). |
| `base_state` | String | Three-character pre-play base occupancy where each slot carries its base number when occupied and an underscore when empty, so ___ is bases empty and 123 is bases loaded. |
| `outs_start` | Int64 | Outs already recorded when the plate appearance began, normally 0 through 2, though a handful of published rows carry a stale 3 that the RE24 matrix filters out but this table does not. |
| `score_diff_bucket` | Int64 | Home score minus away score before the play, clipped to the range -6 through +6 so blowouts collapse into the end buckets. |
| `home_win_exp` | Float64 | Home team win expectancy before the play. |
| `n` | UInt32 | Plate appearances observed in this state bucket, the sample size behind the Laplace-smoothed home_win_exp. |
| `season` | Int64 | Season year. |

```python
load_mlb_we_table(seasons=2024)
```

## `load_mlb_wpa`

Release: [mlb_game_state](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_game_state) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_game_state/mlb_wpa_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique ESPN game/event identifier. |
| `at_bat_index` | Int64 | Zero-based index of the at-bat within the game. |
| `wpa` | Float64 | Win probability added (WPA) for the posteam. |
| `season` | Int64 | Season year. |

```python
load_mlb_wpa(seasons=2024)
```

## `load_mlb_pbp`

Release: [mlb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_pbp/mlb_pbp_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `game_pk` | Int64 | statsapi game identifier; the join key to every other MLB release. |
| `at_bat_index` | Int64 | Zero-based index of the plate appearance within the game; joins to mlb_pitches and mlb_runners. |
| `inning` | Int64 | Inning number, counting from 1; extra innings continue the sequence. |
| `half_inning` | String | `top` or `bottom`. |
| `batter_id` | Int64 | statsapi person id of the batter. |
| `pitcher_id` | Int64 | statsapi person id of the pitcher. |
| `event_type` | String | Machine-readable plate-appearance outcome (e.g. `single`, `strikeout`, `field_out`). |
| `event` | String | Human-readable outcome of the plate appearance. |
| `description` | String | Narrative text for the plate appearance. |
| `rbi` | Int64 | Runs batted in credited to this plate appearance. |
| `away_score` | Int64 | Away score AFTER the plate appearance. |
| `home_score` | Int64 | Home score AFTER the plate appearance. |
| `is_scoring_play` | Boolean | Whether the plate appearance scored a run. |
| `outs` | Int64 | Outs recorded after the plate appearance. |
| `start_time` | String | UTC timestamp when the plate appearance began; null in older seasons. |
| `end_time` | String | UTC timestamp when the plate appearance ended; null in older seasons. |
| `post_on_first_id` | Int64 | Person id on first base after the plate appearance; null when unoccupied. |
| `post_on_second_id` | Int64 | Person id on second base after the plate appearance; null when unoccupied. |
| `post_on_third_id` | Int64 | Person id on third base after the plate appearance; null when unoccupied. |

```python
load_mlb_pbp(seasons=2024)
```

## `load_mlb_pitches`

Release: [mlb_pitches](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitches) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_pitches/mlb_pitches_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `game_pk` | Int64 | statsapi game identifier; the join key to every other MLB release. |
| `at_bat_index` | Int64 | Zero-based index of the plate appearance within the game; joins to mlb_pbp and mlb_runners. |
| `pitch_number` | Int64 | One-based pitch number within the plate appearance. |
| `batter_id` | Int64 | statsapi person id of the batter. |
| `pitcher_id` | Int64 | statsapi person id of the pitcher. |
| `pitch_type` | String | Classified pitch-type code (FF, SL, CH, ...). statsapi `pitchData`, measured fill: ~0% before 2007, 45.7% in 2007 (mid-season PITCHf/x rollout across ballparks), 96.6% in 2008, ~100% from 2015. Null for pitches the system never tracked. |
| `pitch_name` | String | Human-readable pitch type. statsapi `pitchData`, measured fill: ~0% before 2007, 45.7% in 2007 (mid-season PITCHf/x rollout across ballparks), 96.6% in 2008, ~100% from 2015. Null for pitches the system never tracked. |
| `call_code` | String | Umpire call code (B, C, S, X, ...). |
| `call_description` | String | Human-readable umpire call. |
| `balls` | Int64 | Ball count BEFORE the pitch. |
| `strikes` | Int64 | Strike count BEFORE the pitch. |
| `outs` | Int64 | Outs BEFORE the pitch. |
| `start_speed` | Float64 | Release speed in mph. statsapi `pitchData`, measured fill: ~0% before 2007, 45.7% in 2007 (mid-season PITCHf/x rollout across ballparks), 96.6% in 2008, ~100% from 2015. Null for pitches the system never tracked. |
| `end_speed` | Float64 | Speed crossing the plate in mph. statsapi `pitchData`, measured fill: ~0% before 2007, 45.7% in 2007 (mid-season PITCHf/x rollout across ballparks), 96.6% in 2008, ~100% from 2015. Null for pitches the system never tracked. |
| `spin_rate` | Float64 | Spin rate in rpm. statsapi `pitchData`, measured fill: ~0% before 2007, 45.7% in 2007 (mid-season PITCHf/x rollout across ballparks), 96.6% in 2008, ~100% from 2015. Null for pitches the system never tracked. |
| `extension` | Float64 | Release extension toward the plate in feet. Later than the rest of `pitchData`: measured 0% through 2016 and ~100% from 2017, so it is null for two PITCHf/x-era decades that do carry speed and spin. |
| `px` | Float64 | Horizontal location crossing the plate in feet from the plate's centre, catcher's view. statsapi `pitchData`, measured fill: ~0% before 2007, 45.7% in 2007 (mid-season PITCHf/x rollout across ballparks), 96.6% in 2008, ~100% from 2015. Null for pitches the system never tracked. |
| `pz` | Float64 | Height crossing the plate in feet above the ground. statsapi `pitchData`, measured fill: ~0% before 2007, 45.7% in 2007 (mid-season PITCHf/x rollout across ballparks), 96.6% in 2008, ~100% from 2015. Null for pitches the system never tracked. |
| `sz_top` | Float64 | Top of the batter's strike zone in feet. Derived from the batter, not `pitchData`, so it is populated back to 1988. |
| `sz_bot` | Float64 | Bottom of the batter's strike zone in feet. Derived from the batter, not `pitchData`, so it is populated back to 1988. |
| `launch_speed` | Float64 | Exit velocity off the bat in mph. statsapi `hitData`, Statcast-era and batted balls only: null before 2015 and ~17% populated after, which is the share of pitches put in play rather than a coverage gap. |
| `launch_angle` | Float64 | Vertical launch angle in degrees. statsapi `hitData`, Statcast-era and batted balls only: null before 2015 and ~17% populated after, which is the share of pitches put in play rather than a coverage gap. |
| `total_distance` | Float64 | Batted-ball distance travelled in feet. statsapi `hitData`, Statcast-era and batted balls only: null before 2015 and ~17% populated after, which is the share of pitches put in play rather than a coverage gap. |
| `trajectory` | String | Batted-ball trajectory (`ground_ball`, `line_drive`, `fly_ball`, `popup`). Legacy scorer field, batted balls only: ~20% populated in every season back to 1988, and present long before Statcast. |
| `hardness` | String | Scorer's contact-quality grade (`soft`, `medium`, `hard`). Legacy scorer field, batted balls only: ~20% populated in every season back to 1988, and present long before Statcast. |

```python
load_mlb_pitches(seasons=2024)
```

## `load_mlb_runners`

Release: [mlb_runners](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_runners) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_runners/mlb_runners_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `game_pk` | Int64 | statsapi game identifier; the join key to every other MLB release. |
| `at_bat_index` | Int64 | Zero-based index of the plate appearance within the game; joins to mlb_pbp and mlb_pitches. |
| `runner_id` | Int64 | statsapi person id of the baserunner. |
| `origin_base` | String | Base the runner occupied when the plate appearance began; null for the batter. |
| `start_base` | String | Base the runner started this movement from; null when the movement begins at the plate. |
| `end_base` | String | Base the runner finished on; null when retired or when scoring is recorded by `is_scoring_event`. |
| `out_base` | String | Base at which the runner was retired; null when not retired. |
| `is_out` | Boolean | Whether the runner was retired on this movement. |
| `out_number` | Int64 | Which out of the half-inning this retirement was; null when not retired. |
| `event` | String | Human-readable event that caused the movement. |
| `event_type` | String | Machine-readable event that caused the movement. |
| `movement_reason` | String | statsapi reason code for a movement not caused by the plate appearance itself (e.g. `r_stolen_base_2b`). |
| `is_scoring_event` | Boolean | Whether this movement scored a run. |
| `rbi` | Boolean | Whether the run was credited as an RBI to the batter. |
| `earned` | Boolean | Whether the run was earned against the responsible pitcher. |
| `responsible_pitcher_id` | Int64 | statsapi person id of the pitcher charged with the runner. |

```python
load_mlb_runners(seasons=2024)
```

## `load_mlb_expected_stats`

Release: [mlb_hitting_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_hitting_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_hitting_models/mlb_expected_stats_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `batter` | Int64 | MLBAM player id of the batter. |
| `season` | Int64 | Season year. |
| `pa` | Int64 | Plate appearances for the batter in the season, counted as the Statcast rows that END a plate appearance (a non-empty events value). Pitches within a plate appearance are not counted. |
| `ab` | Int64 | At-bats, derived from the same plate-appearance-ending rows by excluding walks, hit-by-pitches, sacrifice flies, sacrifice bunts and catcher's interference. |
| `xwoba` | Float64 | Expected wOBA blending the exit-velocity by launch-angle grid's predicted contact value on balls in play with realized wOBA value on walks, hit-by-pitches and strikeouts, over the wOBA denominator; low-sample batters can exceed 1. |
| `xba` | Float64 | Grid-predicted hit probability summed over the at-bat balls in play that carry launch data, plus the realized hit for balls in play Statcast did not track, divided by at-bats, on the conventional batting-average scale. An untracked ball in play takes its realized outcome exactly as xwoba does, rather than counting in ab with a zero numerator, which deflated league-mean xBA by the untracked share. |
| `xslg` | Float64 | The same construction on total bases -- grid-predicted total bases where launch data exists, realized total bases for untracked balls in play -- divided by at-bats, on the conventional slugging scale. |

```python
load_mlb_expected_stats(seasons=2024)
```

## `load_mlb_expected_hr`

Release: [mlb_hitting_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_hitting_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_hitting_models/mlb_expected_hr_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `batter` | Int64 | MLBAM player id of the batter. |
| `season` | Int64 | Season year. |
| `hr` | Int64 | Home runs hit by the batter over the covered sample. |
| `xhr_neutral` | Float64 | Park-neutral expected home runs, summing over the batter's balls in play the home-run probability read off the exit-velocity by launch-angle by spray-angle grid. |
| `xhr_park_adj` | Float64 | The same expected-home-run sum after scaling each ball by its ballpark's Savant home-run park factor over 100; published values run between 0.77 and 1.26 times xhr_neutral. |
| `hr_above_expected` | Float64 | Home runs actually hit minus xhr_neutral, so it grades over- and under-performance against the park-neutral expectation rather than the park-adjusted one. |

```python
load_mlb_expected_hr(seasons=2024)
```

## `load_mlb_batter_projection`

Release: [mlb_hitting_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_hitting_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_hitting_models/mlb_batter_projection_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `batter` | Int64 | MLBAM player id of the batter. |
| `age` | Int64 | Player age (in years). |
| `proj_xwoba` | Float64 | Projected expected weighted on-base average for the batter. |
| `proj_pa` | Float64 | Combined prior-three-season pa behind the projection, its effective sample size; it inherits the pitch-row counting of load_mlb_expected_stats pa rather than true plate appearances. |

```python
load_mlb_batter_projection(seasons=2024)
```

## `load_mlb_oaa`

Release: [mlb_fielding_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_fielding_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_fielding_models/mlb_oaa_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `fielder_id` | String | MLBAM identifier of the fielder charged with the ball in play, resolved from whichever fielder_N column matches the responsible position and published as a string rather than an integer. |
| `position` | Int64 | Listed roster position (G, F, C, etc.). |
| `opportunities` | UInt32 | Balls in play charged to this fielder at this position, the sample the oaa sum runs over. |
| `oaa` | Float64 | Outs above average: outs the fielder actually recorded minus what a per-position catch-probability logistic expected from the same batted-ball trajectories, summed across their opportunities. |
| `season` | Int64 | Season year. |

```python
load_mlb_oaa(seasons=2024)
```

## `load_mlb_catcher_framing`

Release: [mlb_fielding_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_fielding_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_fielding_models/mlb_catcher_framing_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `catcher_id` | String | MLBAM identifier of the receiving catcher, taken from Savant's fielder_2 and published as a string rather than an integer. |
| `takes` | UInt32 | Called strikes plus balls the catcher received across the season, a pure workload count; the framing figures themselves sum only over the shadow-zone subset of these. |
| `framing_runs` | Float64 | Runs saved by receiving, summing actual called strike minus modeled strike probability times that count's strike run value over shadow-zone takes only. |
| `strikes_gained` | Float64 | The same shadow-zone sum of actual called strike minus modeled strike probability left unweighted by run value, so it measures stolen strikes rather than runs. |
| `season` | Int64 | Season year. |

```python
load_mlb_catcher_framing(seasons=2024)
```

## `load_mlb_xera`

Release: [mlb_pitching_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitching_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_pitching_models/mlb_xera_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | Whether the position is a pitcher. |
| `season` | Int64 | Season year. |
| `x_woba` | Float64 | Expected weighted on-base average, derived from batted-ball quality rather than outcomes. |
| `x_era` | Float64 | ERA-scale conversion of x_woba as league_era plus (x_woba minus league_woba) over woba_scale times pa_per_9, an exact linear function of x_woba that can go negative for extreme pitchers. |

```python
load_mlb_xera(seasons=2024)
```

## `load_mlb_stuff_plus`

Release: [mlb_pitching_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitching_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_pitching_models/mlb_stuff_plus_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | Whether the position is a pitcher. |
| `pitch_type` | String | Abbreviation of the pitch type thrown (e.g. FF, SL, CH). |
| `stuff_rv_hat` | Float64 | Mean predicted per-pitch run value from the bundled xgboost stuff model over this pitcher's pitches of this type, on Savant's batter-perspective delta_run_exp scale so lower is better for the pitcher. |
| `stuff_plus` | Float64 | Stuff+ on the 100-is-average scale, exactly 100 minus 10 times (stuff_rv_hat minus the league mean) over the league SD, so higher is better and outlier run-value predictions can push it well below zero. |
| `season` | Int64 | Season year. |

```python
load_mlb_stuff_plus(seasons=2024)
```

## `load_mlb_command_plus`

Release: [mlb_pitching_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitching_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_pitching_models/mlb_command_plus_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | Whether the position is a pitcher. |
| `location_rv_hat` | Float64 | Mean predicted per-pitch run value from the bundled location model, which sees plate location, count, handedness and pitch type but no raw pitch physics; lower is better for the pitcher. |
| `command_plus` | Float64 | Command+/Location+ on the 100-is-average scale, exactly 100 minus 10 times (location_rv_hat minus the league mean) over the league SD; it grades where the pitch finished, not intent, since Statcast ships no catcher target. |
| `season` | Int64 | Season year. |

```python
load_mlb_command_plus(seasons=2024)
```

## `load_ncaa_baseball_pbp`

Release: [ncaa_baseball_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_pbp/ncaa_baseball_pbp_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `game_date` | String | Game date (YYYY-MM-DD). |
| `location` | String | Team city/region (e.g. "Los Angeles"). |
| `attendance` | Boolean | Reported attendance (NA on the redesigned page). |
| `inning` | String | Inning number. |
| `inning_top_bot` | String | Half-inning ("top" or "bot"). |
| `score` | String | Running score (away-home) after the play. |
| `batting` | String | Whether the situation applies to batting stats. |
| `fielding` | String | Whether the situation applies to fielding stats. |
| `description` | String | Long-form description text. |
| `year` | Int32 | Season (4-digit year). |
| `game_pbp_url` | String | stats.ncaa.org play-by-play url for the game. |
| `game_pbp_id` | Int32 | stats.ncaa.org play-by-play (contest) identifier. |
| `game_info_url` | String | Full stats.ncaa.org box-score url for the game. |
| `contest_id` | Int32 | stats.ncaa.org contest (game) identifier. |

```python
load_ncaa_baseball_pbp(seasons=2023)
```

## `load_ncaa_baseball_schedule`

Release: [ncaa_baseball_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_schedules) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_schedules/ncaa_baseball_schedule_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `year` | Int32 | Season (4-digit year). |
| `season_id` | Int32 | stats.ncaa.org season identifier. |
| `date` | String | Date in YYYY-MM-DD format. |
| `home_team` | String | Home team name. |
| `home_team_id` | Int32 | Home team id. |
| `home_team_score` | Int32 | Home team score. |
| `home_team_conference` | String | Home team conference name. |
| `home_team_conference_id` | Int32 | Home team conference id. |
| `home_team_slug` | String | Relative stats.ncaa.org url for the home team. |
| `home_team_division` | Int32 | Home team NCAA division (1, 2, 3). |
| `away_team` | String | Away team name. |
| `away_team_id` | Int32 | Away team id. |
| `away_team_score` | Int32 | Away team score. |
| `away_team_conference` | String | Away team conference name. |
| `away_team_conference_id` | Int32 | Away team conference id. |
| `away_team_slug` | String | Relative stats.ncaa.org url for the away team. |
| `away_team_division` | Int32 | Away team NCAA division (1, 2, 3). |
| `neutral_site` | String | Neutral-site venue (when not hosted by either team). |
| `innings` | Int32 | Innings played when other than regulation (extras). |
| `slug` | String | URL-safe identifier. |
| `game_info_url` | String | Full stats.ncaa.org box-score url for the game. |
| `contest_id` | Int32 | stats.ncaa.org contest (game) identifier. |

```python
load_ncaa_baseball_schedule(seasons=2023)
```

## `load_ncaa_baseball_teams`

Release: [ncaa_baseball_teams](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_teams) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_teams/ncaa_baseball_teams_{season}.parquet`
```python
load_ncaa_baseball_teams(seasons=2025)
```

## `load_ncaa_baseball_rosters`

Release: [ncaa_baseball_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_rosters/ncaa_baseball_rosters_{season}.parquet`
```python
load_ncaa_baseball_rosters(seasons=2025)
```

## `load_ncaa_baseball_linescore`

Release: [ncaa_baseball_linescore](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_linescore) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_linescore/ncaa_baseball_linescore_{season}.parquet`
```python
load_ncaa_baseball_linescore(seasons=2025)
```

## `load_ncaa_baseball_team_stats`

Release: [ncaa_baseball_team_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_team_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_team_stats/ncaa_baseball_team_stats_{season}.parquet`
```python
load_ncaa_baseball_team_stats(seasons=2025)
```

## `load_ncaa_baseball_player_stats`

Release: [ncaa_baseball_player_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_player_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_player_stats/ncaa_baseball_player_stats_{season}.parquet`
```python
load_ncaa_baseball_player_stats(seasons=2025)
```

## `load_ncaa_baseball_situational_stats`

Release: [ncaa_baseball_situational_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_situational_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_situational_stats/ncaa_baseball_situational_stats_{season}.parquet`
```python
load_ncaa_baseball_situational_stats(seasons=2025)
```

## `load_ncaa_baseball_games`

Release: [ncaa_baseball_games](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_games) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_games/ncaa_baseball_games_{season}.parquet`
```python
load_ncaa_baseball_games(seasons=2024)
```
