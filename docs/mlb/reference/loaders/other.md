# MLB dataset loaders — Other

> MLB dataset loaders — Other — function reference in sdv-py, the SportsDataverse Python package.

## load_mlb_re24_matrix

Release: [mlb_game_state](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_game_state) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_game_state/mlb_re24_matrix_{season}.parquet`
### Returns {#load_mlb_re24_matrix-returns}

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

## load_mlb_we_table

Release: [mlb_game_state](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_game_state) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_game_state/mlb_we_table_{season}.parquet`
### Returns {#load_mlb_we_table-returns}

| col_name | type | description |
|---|---|---|
| `inning_capped` | Int64 | Inning number with the ninth and every extra inning collapsed into 9, so extras share the ninth-inning win-expectancy cells. |
| `half` | String |  |
| `base_state` | String | Three-character pre-play base occupancy where each slot carries its base number when occupied and an underscore when empty, so ___ is bases empty and 123 is bases loaded. |
| `outs_start` | Int64 | Outs already recorded when the plate appearance began, normally 0 through 2, though a handful of published rows carry a stale 3 that the RE24 matrix filters out but this table does not. |
| `score_diff_bucket` | Int64 | Home score minus away score before the play, clipped to the range -6 through +6 so blowouts collapse into the end buckets. |
| `home_win_exp` | Float64 | Home team win expectancy before the play. |
| `n` | UInt32 | Plate appearances observed in this state bucket, the sample size behind the Laplace-smoothed home_win_exp. |
| `thin` | Boolean | Whether the win-expectancy cell was estimated from a thin sample of historical games. |
| `season` | Int64 | Season year. |

```python
load_mlb_we_table(seasons=2024)
```

## load_mlb_wpa

Release: [mlb_game_state](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_game_state) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_game_state/mlb_wpa_{season}.parquet`
### Returns {#load_mlb_wpa-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique ESPN game/event identifier. |
| `at_bat_index` | Int64 | Zero-based index of the at-bat within the game. |
| `wpa` | Float64 |  |
| `season` | Int64 | Season year. |

```python
load_mlb_wpa(seasons=2024)
```

## load_mlb_pbp

Release: [mlb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_pbp/mlb_pbp_{season}.parquet`
### Returns {#load_mlb_pbp-returns}

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

## load_mlb_pitches

Release: [mlb_pitches](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitches) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_pitches/mlb_pitches_{season}.parquet`
### Returns {#load_mlb_pitches-returns}

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
| `extension` | Float64 | Release extension toward the plate in feet. Later than the rest of `pitchData`: measured 0% through 2016 and ~100% from 2017, so it is null for the 2007-2016 PITCHf/x seasons that do carry speed and spin. |
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

## load_mlb_runners

Release: [mlb_runners](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_runners) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_runners/mlb_runners_{season}.parquet`
### Returns {#load_mlb_runners-returns}

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

## load_mlb_statcast

Release: [mlb_statcast](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_statcast) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_statcast/mlb_statcast_{season}.parquet`
### Returns {#load_mlb_statcast-returns}

| col_name | type | description |
|---|---|---|
| `pitch_type` | String | Abbreviation of the pitch type thrown (e.g. FF, SL, CH). |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `release_speed` | Float64 | Pitch velocity out of the hand (mph). |
| `release_pos_x` | Float64 | Horizontal release position of the ball, catcher's perspective (feet). |
| `release_pos_z` | Float64 | Vertical release position of the ball, catcher's perspective (feet). |
| `player_name` | String | Batter name, `Last, First` (Baseball Savant's batter-side `player_name`). |
| `batter` | Int64 | MLBAM player id of the batter. |
| `pitcher` | Int64 | MLBAM player id of the pitcher. |
| `events` | String | Plate-appearance result on the pitch that ended it (e.g. `single`, `strikeout`, `walk`); null on every other pitch. |
| `description` | String | Pitch outcome (e.g. `ball`, `called_strike`, `swinging_strike`, `foul`, `hit_into_play`). |
| `spin_dir` | Float64 | Deprecated spin direction field, no longer populated. |
| `spin_rate_deprecated` | Float64 | Deprecated legacy spin-rate field, no longer populated. |
| `break_angle_deprecated` | Float64 | Deprecated legacy break-angle field, no longer populated. |
| `break_length_deprecated` | Float64 | Deprecated legacy break-length field, no longer populated. |
| `zone` | Float64 | Strike-zone region the pitch crossed (1-14 Gameday zone). |
| `des` | String | Full text description of the play. |
| `game_type` | String | Game-type code; spring training (`S`) and exhibition (`E`) games are included alongside `R` and the postseason. |
| `stand` | String | Side of the plate the batter is standing (L or R). |
| `p_throws` | String | Hand the pitcher throws with (L or R). |
| `home_team` | String | Home team abbreviation (e.g. `SF`). |
| `away_team` | String | Away team abbreviation (e.g. `LAD`). |
| `type` | String | Pitch result class -- `B` ball, `S` strike (including fouls), `X` ball in play. |
| `hit_location` | Float64 | Fielder position number that fielded the ball. |
| `bb_type` | String | Batted-ball type (ground_ball, line_drive, fly_ball, popup). |
| `balls` | Int64 | Ball count before the pitch. |
| `strikes` | Int64 | Strike count before the pitch. |
| `game_year` | Int64 | Season year of the game. |
| `pfx_x` | Float64 | Horizontal pitch movement from the catcher's perspective (feet). |
| `pfx_z` | Float64 | Vertical pitch movement from the catcher's perspective (feet). |
| `plate_x` | Float64 | Horizontal position of the pitch crossing the plate (feet from center). |
| `plate_z` | Float64 | Vertical position of the pitch crossing the plate (feet above ground). |
| `on_3b` | Int64 | MLBAM ID of the runner on third base, if any; published as a float and cast to Int64 by the loader. |
| `on_2b` | Int64 | MLBAM ID of the runner on second base, if any; published as a float and cast to Int64 by the loader. |
| `on_1b` | Int64 | MLBAM ID of the runner on first base, if any; published as a float and cast to Int64 by the loader. |
| `outs_when_up` | Int64 | Number of outs when the batter came to the plate. |
| `inning` | Int64 | Inning number. |
| `inning_topbot` | String | Half of the inning (Top or Bot). |
| `hc_x` | Float64 | Hit coordinate X on the field diagram. |
| `hc_y` | Float64 | Hit coordinate Y on the field diagram. |
| `tfs_deprecated` | Float64 | Deprecated time-from-start field, no longer populated. |
| `tfs_zulu_deprecated` | Float64 | Deprecated Zulu time-from-start field, no longer populated. |
| `umpire` | Float64 | Deprecated umpire field; always null. |
| `sv_id` | String | Deprecated Sportvision/Statcast pitch identifier, no longer populated. |
| `vx0` | Float64 | Velocity of the pitch in the x-direction at y=50 ft (ft/s). |
| `vy0` | Float64 | Velocity of the pitch in the y-direction at y=50 ft (ft/s). |
| `vz0` | Float64 | Velocity of the pitch in the z-direction at y=50 ft (ft/s). |
| `ax` | Float64 | Acceleration of the pitch in the x-direction at y=50 ft (ft/s^2). |
| `ay` | Float64 | Acceleration of the pitch in the y-direction at y=50 ft (ft/s^2). |
| `az` | Float64 | Acceleration of the pitch in the z-direction at y=50 ft (ft/s^2). |
| `sz_top` | Float64 | Top of the batter's strike zone for the pitch (feet). |
| `sz_bot` | Float64 | Bottom of the batter's strike zone for the pitch (feet). |
| `hit_distance_sc` | Float64 | Statcast-measured projected distance of the batted ball (feet). |
| `launch_speed` | Float64 | Exit velocity of the batted ball (mph). |
| `launch_angle` | Float64 | Vertical launch angle of the batted ball (degrees). |
| `effective_speed` | Float64 | Perceived velocity adjusted for release extension (mph). |
| `release_spin_rate` | Float64 | Spin rate of the pitch at release (rpm). |
| `release_extension` | Float64 | Distance toward the plate at release (feet). |
| `game_pk` | Int64 | Unique game identifier. |
| `fielder_2` | Int64 | MLBAM ID of the catcher. |
| `fielder_3` | Int64 | MLBAM ID of the first baseman. |
| `fielder_4` | Int64 | MLBAM ID of the second baseman. |
| `fielder_5` | Int64 | MLBAM ID of the third baseman. |
| `fielder_6` | Int64 | MLBAM ID of the shortstop. |
| `fielder_7` | Int64 | MLBAM ID of the left fielder. |
| `fielder_8` | Int64 | MLBAM ID of the center fielder. |
| `fielder_9` | Int64 | MLBAM ID of the right fielder. |
| `release_pos_y` | Float64 | Release position of the ball toward the plate (feet). |
| `estimated_ba_using_speedangle` | Float64 | Expected batting average based on exit velocity and launch angle. |
| `estimated_woba_using_speedangle` | Float64 | Expected wOBA based on exit velocity and launch angle. |
| `woba_value` | Float64 | wOBA value assigned to the event. |
| `woba_denom` | Float64 | wOBA denominator (plate-appearance weight) for the event. |
| `babip_value` | Float64 | BABIP value assigned to the event (0 or 1). |
| `iso_value` | Float64 | Isolated power value assigned to the event. |
| `launch_speed_angle` | Float64 | Batted-ball classification code (1-6) from exit velocity and angle. |
| `at_bat_number` | Int64 | Sequential plate-appearance number within the game. |
| `pitch_number` | Int64 | Pitch number within the plate appearance. |
| `pitch_name` | String | Full name of the pitch type (e.g. 4-Seam Fastball, Slider). |
| `home_score` | Int64 | Home team score before the pitch (see post_home_score). |
| `away_score` | Int64 | Away team score before the pitch (see post_away_score). |
| `bat_score` | Int64 | Batting team score before the pitch. |
| `fld_score` | Int64 | Fielding team score before the pitch. |
| `post_away_score` | Int64 | Away team score after the pitch. |
| `post_home_score` | Int64 | Home team score after the pitch. |
| `post_bat_score` | Int64 | Batting team score after the pitch. |
| `post_fld_score` | Int64 | Fielding team score after the pitch. |
| `if_fielding_alignment` | String | Infield defensive alignment (Standard, Strategic, Infield shift). |
| `of_fielding_alignment` | String | Outfield defensive alignment (Standard, Strategic, 4th outfielder). |
| `spin_axis` | Float64 | Spin axis of the pitch as a clock-face angle (degrees). |
| `delta_home_win_exp` | Float64 | Change in home team win expectancy on the play. |
| `delta_run_exp` | Float64 | Change in run expectancy on the play. |
| `bat_speed` | Float64 | Bat speed at the point of contact (mph). |
| `swing_length` | Float64 | Length of the swing path to contact (feet). |
| `miss_distance` | Float64 | How far the bat missed the ball on a swing and miss (Savant bat tracking); null on every other pitch and before bat tracking. |
| `estimated_slg_using_speedangle` | Float64 | Expected slugging based on exit velocity and launch angle. |
| `delta_pitcher_run_exp` | Float64 | Change in run expectancy credited to the pitcher. |
| `hyper_speed` | Float64 | Adjusted (90th-percentile) exit velocity (mph). |
| `home_score_diff` | Int64 | Home team score minus away team score before the pitch. |
| `bat_score_diff` | Int64 | Batting team score minus fielding team score before the pitch. |
| `home_win_exp` | Float64 | Home team win expectancy before the play. |
| `bat_win_exp` | Float64 | Batting team win expectancy before the play. |
| `age_pit_legacy` | Int64 | Pitcher age using the legacy calculation. |
| `age_bat_legacy` | Int64 | Batter age using the legacy calculation. |
| `age_pit` | Int64 | Pitcher age for the season. |
| `age_bat` | Int64 | Batter age for the season. |
| `n_thruorder_pitcher` | Int64 | Times through the order the pitcher is facing the lineup. |
| `n_priorpa_thisgame_player_at_bat` | Int64 | Number of prior plate appearances by the batter in the game. |
| `pitcher_days_since_prev_game` | Float64 | Days since the pitcher's previous game appearance. |
| `batter_days_since_prev_game` | Float64 | Days since the batter's previous game appearance. |
| `pitcher_days_until_next_game` | Float64 | Days until the pitcher's next game appearance. |
| `batter_days_until_next_game` | Float64 | Days until the batter's next game appearance. |
| `api_break_z_with_gravity` | Float64 | Vertical pitch break including gravity (inches). |
| `api_break_x_arm` | Float64 | Horizontal pitch break to the pitcher's arm side (inches). |
| `api_break_x_batter_in` | Float64 | Horizontal pitch break toward/away from the batter (inches). |
| `arm_angle` | Float64 | Pitcher's arm angle at release (degrees). |
| `attack_angle` | Float64 | Angle of the bat's path at contact (degrees). |
| `attack_direction` | Float64 | Horizontal direction of the swing at contact (degrees). |
| `swing_path_tilt` | Float64 | Vertical tilt of the swing path (degrees). |
| `intercept_ball_minus_batter_pos_x_inches` | Float64 | Horizontal offset of ball-bat intercept from batter position (inches). |
| `intercept_ball_minus_batter_pos_y_inches` | Float64 | Depth offset of ball-bat intercept from batter position (inches). |
| `season` | Int64 | Season year. |

```python
load_mlb_statcast(seasons=2024)
```

## load_mlb_schedule

Release: [mlb_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_schedules) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_schedules/mlb_schedule_{season}.parquet`
### Returns {#load_mlb_schedule-returns}

| col_name | type | description |
|---|---|---|
| `game_pk` | Int64 | statsapi game identifier; the join key to every other MLB release. |
| `game_date` | String | Official game date (YYYY-MM-DD, local to the venue). |
| `game_datetime` | String | Scheduled first-pitch time as an ISO-8601 UTC timestamp string (e.g. `2024-02-22T20:10:00Z`). |
| `season` | Int64 | MLB season year the game belongs to (the asset's season). |
| `game_type` | String | statsapi game-type code (`R` regular season, `S` spring training, `E` exhibition, `A` All-Star, `F`/`D`/`L`/`W` wild card / division series / LCS / World Series). |
| `status_code` | String | Status code identifier (e.g. 'S', 'P', 'I', 'F'). |
| `detailed_state` | String | Detailed status description (e.g. 'Scheduled', 'Pre-Game', 'In Progress'). |
| `abstract_state` | String | Coarse game state (`Preview`, `Live` or `Final`). |
| `home_team_id` | Int64 | Home team id. |
| `home_team_name` | String | Home team name. |
| `home_score` | Int64 | Home team's final run total (null for a game not yet played). |
| `away_team_id` | Int64 | Away team id. |
| `away_team_name` | String | Away team name. |
| `away_score` | Int64 | Away team's final run total (null for a game not yet played). |
| `venue_id` | Int64 | MLBAM venue ID. |
| `venue_name` | String | Venue name. |
| `double_header` | String | Doubleheader indicator ('N', 'S', 'Y'). |
| `game_number` | Int64 | Game number within a doubleheader. |
| `series_description` | String | Description of the series. |

```python
load_mlb_schedule(seasons=2024)
```

## load_mlb_expected_stats

Release: [mlb_hitting_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_hitting_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_hitting_models/mlb_expected_stats_{season}.parquet`
### Returns {#load_mlb_expected_stats-returns}

| col_name | type | description |
|---|---|---|
| `batter` | Int64 | MLBAM player id of the batter. |
| `season` | Int64 | Season year. |
| `pa` | Int64 | Plate appearances for the batter in the season, counted as the Statcast rows that END a plate appearance (a non-empty events value). Pitches within a plate appearance are not counted. |
| `ab` | Int64 | At-bats, derived from the same plate-appearance-ending rows by excluding walks, hit-by-pitches, sacrifice flies, sacrifice bunts and catcher's interference. |
| `xwoba` | Float64 | Expected wOBA blending the exit-velocity by launch-angle grid's predicted contact value on balls in play with realized wOBA value on walks, hit-by-pitches and strikeouts, over the wOBA denominator; low-sample batters can exceed 1. |
| `xba` | Float64 | Grid-predicted hit probability summed over the at-bat balls in play that carry launch data, plus the realized hit for balls in play Statcast did not track, divided by at-bats, on the conventional batting-average scale. An untracked ball in play takes its realized outcome exactly as xwoba does, rather than counting in ab with a zero numerator, which deflated league-mean xBA by the untracked share. |
| `xslg` | Float64 | The same construction on total bases -- grid-predicted total bases where launch data exists, realized total bases for untracked balls in play -- divided by at-bats, on the conventional slugging scale. |
| `woba` | Float64 | Actual weighted on-base average (wOBA) for the player over the sample. |
| `ba` | Float64 | Actual batting average for the player over the sample (0-1), alongside the expected-stat columns. |

```python
load_mlb_expected_stats(seasons=2024)
```

## load_mlb_expected_hr

Release: [mlb_hitting_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_hitting_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_hitting_models/mlb_expected_hr_{season}.parquet`
### Returns {#load_mlb_expected_hr-returns}

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

## load_mlb_batter_projection

Release: [mlb_hitting_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_hitting_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_hitting_models/mlb_batter_projection_{season}.parquet`
### Returns {#load_mlb_batter_projection-returns}

| col_name | type | description |
|---|---|---|
| `batter` | Int64 | MLBAM player id of the batter. |
| `age` | Int64 | Player age (in years). |
| `proj_xwoba` | Float64 | Projected expected weighted on-base average for the batter. |
| `proj_pa` | Float64 | Combined prior-three-season pa behind the projection, its effective sample size; it inherits the pitch-row counting of load_mlb_expected_stats pa rather than true plate appearances. |

```python
load_mlb_batter_projection(seasons=2024)
```

## load_mlb_oaa

Release: [mlb_fielding_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_fielding_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_fielding_models/mlb_oaa_{season}.parquet`
### Returns {#load_mlb_oaa-returns}

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

## load_mlb_catcher_framing

Release: [mlb_fielding_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_fielding_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_fielding_models/mlb_catcher_framing_{season}.parquet`
### Returns {#load_mlb_catcher_framing-returns}

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

## load_mlb_xera

Release: [mlb_pitching_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitching_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_pitching_models/mlb_xera_{season}.parquet`
### Returns {#load_mlb_xera-returns}

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | MLBAM player id of the pitcher. |
| `season` | Int64 | Season year. |
| `x_woba` | Float64 | Expected weighted on-base average, derived from batted-ball quality rather than outcomes. |
| `x_era` | Float64 | ERA-scale conversion of x_woba as league_era plus (x_woba minus league_woba) over woba_scale times pa_per_9, an exact linear function of x_woba that can go negative for extreme pitchers. |

```python
load_mlb_xera(seasons=2024)
```

## load_mlb_stuff_plus

Release: [mlb_pitching_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitching_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_pitching_models/mlb_stuff_plus_{season}.parquet`
### Returns {#load_mlb_stuff_plus-returns}

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | MLBAM player id of the pitcher. |
| `pitch_type` | String | Abbreviation of the pitch type thrown (e.g. FF, SL, CH). |
| `stuff_rv_hat` | Float64 | Mean predicted per-pitch run value from the bundled xgboost stuff model over this pitcher's pitches of this type, on Savant's batter-perspective delta_run_exp scale so lower is better for the pitcher. |
| `stuff_plus` | Float64 | Stuff+ on the 100-is-average scale, exactly 100 minus 10 times (stuff_rv_hat minus the league mean) over the league SD, so higher is better and outlier run-value predictions can push it well below zero. |
| `season` | Int64 | Season year. |

```python
load_mlb_stuff_plus(seasons=2024)
```

## load_mlb_command_plus

Release: [mlb_pitching_models](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_pitching_models) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_pitching_models/mlb_command_plus_{season}.parquet`
### Returns {#load_mlb_command_plus-returns}

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | MLBAM player id of the pitcher. |
| `location_rv_hat` | Float64 | Mean predicted per-pitch run value from the bundled location model, which sees plate location, count, handedness and pitch type but no raw pitch physics; lower is better for the pitcher. |
| `command_plus` | Float64 | Command+/Location+ on the 100-is-average scale, exactly 100 minus 10 times (location_rv_hat minus the league mean) over the league SD; it grades where the pitch finished, not intent, since Statcast ships no catcher target. |
| `season` | Int64 | Season year. |

```python
load_mlb_command_plus(seasons=2024)
```

## load_mlb_groups

Release: [mlb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_groups/mlb_groups.parquet`

:::caution[Coverage]
One season-less file: one row per group lineage (the league, subdivisions, conferences, divisions) with the first and last season it had members. group_id is SDV's own id (e.g. mlb:al-east) and names a lineage: a rename that keeps continuity keeps the id, a new body gets a new one, and notes records each call. Seasons are the calendar year.
:::

### Returns {#load_mlb_groups-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("mlb"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `level` | String | Hierarchy level of the group: "league", "subdivision", "conference" or "division". |
| `first_season` | Int32 | First season in which the group had at least one member (calendar year). |
| `last_season` | Int32 | Last season in which the group had at least one member (calendar year). |
| `notes` | String | Builder notes on the group: the lineage decisions behind its group_id and any source caveats. |

```python
load_mlb_groups()
```

## load_mlb_group_seasons

Release: [mlb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_groups/mlb_group_seasons.parquet`

:::caution[Coverage]
One season-less file: one row per group per season it existed, with its name, short name, abbreviation and parent group AS OF that season (never today's label applied to the past) and its member count. season is the calendar year.
:::

### Returns {#load_mlb_group_seasons-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("mlb"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `season` | Int32 | Season the row describes (calendar year). |
| `level` | String | Hierarchy level of the group: "league", "subdivision", "conference" or "division". |
| `name` | String | Full name of the group as of that season -- the label in use then, not today's name. |
| `short_name` | String | Short display name of the group as of that season. |
| `abbreviation` | String | Abbreviation of the group as of that season. |
| `parent_group_id` | String | group_id one level up as of that season (division -> conference -> subdivision -> league); null at the top level or where no higher group applied that season. |
| `n_teams` | Int32 | Number of member teams in the group that season. |

```python
load_mlb_group_seasons()
```

## load_mlb_group_aliases

Release: [mlb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_groups/mlb_group_aliases.parquet`

:::caution[Coverage]
One season-less file: every name, abbreviation, slug and source id that a source (espn, mlb) uses for a group, each with the seasons it is valid for (valid_from / valid_to, inclusive; null = unbounded). Match a source's conference or division label here to reach group_id.
:::

### Returns {#load_mlb_group_aliases-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("mlb"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `source` | String | Source that uses this label or id (in this table: espn, mlb); "sdv" marks SDV's own labels. |
| `source_id` | String | The source's own id for the group (ESPN group id, NCAA conf_id, CFBD id, MLB division id) when it has one; null otherwise. |
| `name_kind` | String | Kind of label in value: "name", "short_name", "abbreviation", "slug" or "code". |
| `value` | String | The label exactly as the source writes it; match a source's conference or division label against it to reach group_id. |
| `valid_from` | Int32 | First season the alias is valid for, inclusive (calendar year); null = unbounded. |
| `valid_to` | Int32 | Last season the alias is valid for, inclusive (calendar year); null = unbounded (still in use). |

```python
load_mlb_group_aliases()
```

## load_mlb_team_group_seasons

Release: [mlb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_groups/mlb_team_group_seasons_{season}.parquet`

:::caution[Coverage]
One row per team per season: the SDV subdivision, conference and division group ids the team belonged to that season (null where a level does not apply), the team name as of that season, where the membership came from, and whether a second source agreed (null when only one source covers the season). team_id is a string: the ESPN team id where ESPN covers the team, otherwise the MLB Stats API id; team_id_source names the id space. season is the calendar year; seasons 1901-2026.
:::

### Returns {#load_mlb_team_group_seasons-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("mlb"); the prefix of every group_id in it. |
| `season` | Int32 | Season of the membership (calendar year). |
| `team_id` | String | Team id as a string: the ESPN team id where ESPN covers the team, otherwise the league's own id; team_id_source says which. |
| `team_id_source` | String | Id space of team_id (in this table: espn, mlb). |
| `team_name` | String | Team name as of that season, not today's. |
| `subdivision_id` | String | SDV group_id of the team's subdivision that season (e.g. FBS / FCS, Division I); null where the league has no subdivision level. |
| `conference_id` | String | SDV group_id of the team's conference that season; null where the team had no conference (an independent, or a season played without conferences). |
| `division_id` | String | SDV group_id of the team's division that season; null where the level does not apply. |
| `source` | String | Source the membership was taken from -- the most reliable per-season source for that era. |
| `sources_agree` | Boolean | Whether a second source agreed on the membership; null when only one source covers the season. |
| `notes` | String | Builder notes on the team-season, such as a source disagreement or which of several listed memberships was kept. |

```python
load_mlb_team_group_seasons(seasons=2024)
```

## load_mlb_park_dimensions

Release: [mlb_parks](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mlb_parks) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mlb_parks/mlb_park_dimensions.parquet`

:::caution[Coverage]
One season-less file, seasons 2001 on: one row per MLB venue per season (regular-season, spring-training, neutral and international sites) from the MLB Stats API venues endpoint, with fence distances in feet at MLB's seven markers, capacity, turf, roof, azimuth, elevation and coordinates as of that season. venue_id is a string (the MLB Stats API venue id, venue.id in game feeds); venue_name is the name in use that season. The API lags or misses some fence moves: cited corrections (Camden Yards, Petco Park, T-Mobile Park, Comerica Park, 2022 Rate Field and Progressive Field) are applied and described in notes, which is null on uncorrected rows.
:::

### Returns {#load_mlb_park_dimensions-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("mlb"). |
| `season` | Int32 | Season the row describes (calendar year, 2001 on). |
| `venue_id` | String | MLB Stats API venue id (venue.id in MLB game feeds and schedules), published as a string. |
| `venue_name` | String | Venue name as of that season (e.g. PacBell Park 2001-03, SBC Park 2004-05, AT&T Park 2006-18, Oracle Park 2019-), not today's name. |
| `retro_park_id` | String | Retrosheet park id (e.g. "BOS07") from the MLB Stats API's cross-reference; null for most spring-training and minor-league parks. |
| `left_line_ft` | Int32 | Distance in feet from home plate to the fence at the left-field foul pole. |
| `left_ft` | Int32 | Distance in feet from home plate to the fence at MLB's left-field marker. |
| `left_center_ft` | Int32 | Distance in feet from home plate to the fence at MLB's left-centre marker; a park may re-label which point this is (Oracle Park's reads 364 through 2019, then the 399 ft deep left-centre). |
| `center_ft` | Int32 | Distance in feet from home plate to the fence in straightaway centre field. |
| `right_center_ft` | Int32 | Distance in feet from home plate to the fence at MLB's right-centre marker. |
| `right_ft` | Int32 | Distance in feet from home plate to the fence at MLB's right-field marker. |
| `right_line_ft` | Int32 | Distance in feet from home plate to the fence at the right-field foul pole. |
| `capacity` | Int32 | Seating capacity that season. |
| `turf_type` | String | Playing surface: "Grass" or "Artificial Turf". |
| `roof_type` | String | Roof: "Open", "Retractable" or "Dome". |
| `azimuth_deg` | Float64 | MLB's azimuthAngle: degrees clockwise from north of the line from home plate to centre field (Fenway Park 45, Progressive Field 0). |
| `elevation_ft` | Int32 | Elevation of the venue in feet above sea level. |
| `latitude` | Float64 | Latitude of the venue in decimal degrees. |
| `longitude` | Float64 | Longitude of the venue in decimal degrees. |
| `notes` | String | Null unless a curated correction applies to the row; then which columns changed, from what to what, why, and the citation. |

```python
load_mlb_park_dimensions()
```
