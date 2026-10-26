---
title: "MLB — additional Python functions — Analytics"
sidebar_label: "Analytics"
sidebar_position: 6
description: "MLB — additional Python functions — Analytics — function reference in sdv-py, the SportsDataverse Python package."
---
# MLB — additional Python functions — Analytics

### add_sequence_features {#add_sequence_features}

`add_sequence_features(feats: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Add within-game sequence, times-through-order, and workload features.

Consumes the output of `pitch_features`. Within each plate
appearance (`game_pk`, `pitcher`, `at_bat_number`, sorted by
`pitch_number`), adds `prev_pitch_type`/`prev_release_pos_x`/
`prev_release_pos_z`/`prev_plate_x`/`prev_plate_z` via
`shift(1)`. Within each game (`game_pk`, `pitcher`, sorted by
`at_bat_number` then `pitch_number`), adds `cum_pitches_game`
(running pitch count), `batter_faced_index` (distinct-`at_bat_number`
rank), and `times_through_order` (`min(3, (batter_faced_index-1)//9+1)`).
Every lag/rank is `.over(...)` scoped to avoid cross-game leakage.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `feats` | `DataFrame` |  | Output of `pitch_features`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`feats` plus the sequence/TTO/workload columns described above. Empty input returns a zero-row frame carrying the full schema.

| col_name | type | description |
|---|---|---|
| `prev_pitch_type` | String | Pitch type of the previous pitch in the same plate appearance (null on the first pitch). |
| `prev_release_pos_x` | Float64 | Horizontal release position of the previous pitch in the same plate appearance. |
| `prev_release_pos_z` | Float64 | Vertical release position of the previous pitch in the same plate appearance. |
| `prev_plate_x` | Float64 | Horizontal plate location of the previous pitch in the same plate appearance. |
| `prev_plate_z` | Float64 | Vertical plate location of the previous pitch in the same plate appearance. |
| `cum_pitches_game` | Int64 | Running count of pitches thrown by this pitcher so far in this game (inclusive of the current pitch). |
| `batter_faced_index` | Int64 | Dense rank of this plate appearance's at_bat_number within the game (1 = first batter faced). |
| `times_through_order` | Int64 | Times through the batting order, min(3, (batter_faced_index-1)//9 + 1). |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
feats = add_sequence_features(pitch_features(raw))
print(feats.select("times_through_order", "cum_pitches_game").tail())
```

### advancement_opportunities {#advancement_opportunities}

`advancement_opportunities(events: "'pl.DataFrame'") -> "'pl.DataFrame'"`

Extract first-to-third / second-to-home / tag-up opportunities and outcomes.

One plate-appearance row (the terminal, non-null-`events` pitch of
each `(game_pk, at_bat_number)`) is matched against the *next*
plate appearance's pre-play occupancy (`on_1b`/`on_2b`/`on_3b`,
shifted within `game_pk`) to read the post-play base state.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `events` | `DataFrame` |  | Pitch-level frame (a `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search` output) with `game_pk`, `at_bat_number`, `on_1b`, `on_2b`, `on_3b`, `events`. |

**Returns**

one row per detected opportunity. | Column | Type | Description | |---|---|---| | runner_id | Utf8 | MLBAM id of the runner facing the advancement decision | | opp_type | Utf8 | `first_to_third` \| `second_to_home` \| `tag_up` | | took_extra | Int8 | 1 if the runner advanced the extra base, else 0 |

| col_name | type | description |
|---|---|---|
| `runner_id` | String |  |
| `opp_type` | String |  |
| `took_extra` | Int64 |  |

**Example**

```python
from sportsdataverse.mlb.mlb_baserunning import advancement_opportunities
opps = advancement_opportunities(pitches)
```

### bip_trajectory_features {#bip_trajectory_features}

`bip_trajectory_features(bip: "'pl.DataFrame'") -> "'pl.DataFrame'"`

Add spray angle / hit distance / launch-angle bin / out label / position.

`spray_angle = atan2(hc_x - 125.42, 198.27 - hc_y)` (Savant's standard
`hc_x`/`hc_y` transform, home plate at the origin, positive = toward
first base).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `bip` | `DataFrame` |  | Balls-in-play frame (`hc_x`, `hc_y`, `hit_distance_sc`, `launch_angle`, `events`, `hit_location`). |

**Returns**

`bip` with added `spray_angle` (Float64), `hit_dist` (Float64), `la_bin` (Int64), `is_out` (Int8), `position` (Int64).

| col_name | type | description |
|---|---|---|
| `pitch_type` | String | Abbreviation of the pitch type thrown (e.g. FF, SL, CH). |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `release_speed` | Float64 | Pitch velocity out of the hand (mph). |
| `release_pos_x` | Float64 | Horizontal release position of the ball, catcher's perspective (feet). |
| `release_pos_z` | Float64 | Vertical release position of the ball, catcher's perspective (feet). |
| `player_name` | String | Player name carried through from the input Statcast frame, `Last, First` -- the batter for a default mlb_statcast_search pull, the pitcher for a `player_type="pitcher"` search. |
| `batter` | Int64 | MLBAM player id of the batter. |
| `pitcher` | Int64 | MLBAM player id of the pitcher. |
| `events` | String | Nested list of non-game events. |
| `description` | String | Long-form description text. |
| `spin_dir` | Float64 | Deprecated spin direction field, no longer populated. |
| `spin_rate_deprecated` | Float64 | Deprecated legacy spin-rate field, no longer populated. |
| `break_angle_deprecated` | Float64 | Deprecated legacy break-angle field, no longer populated. |
| `break_length_deprecated` | Float64 | Deprecated legacy break-length field, no longer populated. |
| `zone` | Float64 | Strike-zone region the pitch crossed (1-14 Gameday zone). |
| `des` | String | Full text description of the play. |
| `game_type` | String | Game type code (R, P, etc.). |
| `stand` | String | Side of the plate the batter is standing (L or R). |
| `p_throws` | String | Hand the pitcher throws with (L or R). |
| `home_team` | String | Home team name. |
| `away_team` | String | Away team name. |
| `type` | String | Record type / category. |
| `hit_location` | Float64 | Fielder position number that fielded the ball. |
| `bb_type` | String | Batted-ball type (ground_ball, line_drive, fly_ball, popup). |
| `balls` | Int64 | Ball count before the pitch. |
| `strikes` | Int64 | Strike count before the pitch. |
| `game_year` | Int64 | Season year of the game. |
| `pfx_x` | Float64 | Horizontal pitch movement from the catcher's perspective (feet). |
| `pfx_z` | Float64 | Vertical pitch movement from the catcher's perspective (feet). |
| `plate_x` | Float64 | Horizontal position of the pitch crossing the plate (feet from center). |
| `plate_z` | Float64 | Vertical position of the pitch crossing the plate (feet above ground). |
| `on_3b` | Int64 | MLBAM ID of the runner on third base, if any. |
| `on_2b` | Int64 | MLBAM ID of the runner on second base, if any. |
| `on_1b` | Int64 | MLBAM ID of the runner on first base, if any. |
| `outs_when_up` | Int64 | Number of outs when the batter came to the plate. |
| `inning` | Int64 | Inning number. |
| `inning_topbot` | String | Half of the inning (Top or Bot). |
| `hc_x` | Float64 | Hit coordinate X on the field diagram. |
| `hc_y` | Float64 | Hit coordinate Y on the field diagram. |
| `tfs_deprecated` | Float64 | Deprecated time-from-start field, no longer populated. |
| `tfs_zulu_deprecated` | Float64 | Deprecated Zulu time-from-start field, no longer populated. |
| `umpire` | Float64 | Deprecated umpire field, no longer populated. |
| `sv_id` | Float64 | Deprecated Sportvision/Statcast pitch identifier, no longer populated. |
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
| `miss_distance` | Float64 |  |
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
| `batter_days_since_prev_game` | Int64 | Days since the batter's previous game appearance. |
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
| `spray_angle` | Float64 |  |
| `hit_dist` | Float64 |  |
| `la_bin` | Int64 |  |
| `is_out` | Int64 |  |
| `position` | Int64 | Listed roster position (G, F, C, etc.). |

**Example**

```python
from sportsdataverse.mlb.mlb_fielding_oaa import bip_trajectory_features
feats = bip_trajectory_features(bip)
```

### called_strike_prob_grid {#called_strike_prob_grid}

`called_strike_prob_grid(pitches: "'pl.DataFrame'", *, x_bin: 'float' = 0.1, z_bin: 'float' = 0.1, alpha: 'float' = 1.0) -> "'pl.DataFrame'"`

Empirical called-strike-probability grid over `(stand, plate_x, pz_norm)`.

Pitch height is normalized within the batter's strike zone
(`pz_norm = (plate_z - sz_bot) / (sz_top - sz_bot)`) so the grid is
zone-relative and comparable across batters; `plate_x` is kept raw
(feet from the plate's center). Rate per bin is Laplace-smoothed:
`(strikes + alpha) / (n + 2 * alpha)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Pitch-level takes frame (`plate_x`, `plate_z`, `sz_top`, `sz_bot`, `stand`, `description`). |
| `x_bin` | `float` | `0.1` | Bin width for `plate_x`, in feet. Defaults to `0.1`. |
| `z_bin` | `float` | `0.1` | Bin width for zone-normalized height. Defaults to `0.1`. |
| `alpha` | `float` | `1.0` | Laplace smoothing strength. Defaults to `1.0`. |

**Returns**

one row per observed `(stand, px_bin, pz_bin)`. | Column | Type | Description | |---|---|---| | stand | Utf8 | Batter handedness (`L`/`R`) | | px_bin | Int64 | Horizontal plate-location bin index | | pz_bin | Int64 | Zone-normalized vertical bin index | | p_strike | Float64 | Laplace-smoothed empirical called-strike probability | | n | Int64 | Takes observed in this bin |

| col_name | type | description |
|---|---|---|
| `stand` | String | Side of the plate the batter is standing (L or R). |
| `px_bin` | Int64 |  |
| `pz_bin` | Int64 |  |
| `p_strike` | Float64 |  |
| `n` | Int64 |  |

**Example**

```python
from sportsdataverse.mlb.mlb_catcher_framing import called_strike_prob_grid
grid = called_strike_prob_grid(pitches, alpha=1.0)
```

### catch_prob_surface {#catch_prob_surface}

`catch_prob_surface(bip: "'pl.DataFrame'", *, dist_bin: 'float' = 10.0, spray_bin: 'float' = 0.1, alpha: 'float' = 2.0) -> "'pl.DataFrame'"`

Empirical catch-probability surface over `(position, distance, spray, launch angle)`.

Rate per bin is Laplace-smoothed: `(outs + alpha) / (n + 2 * alpha)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `bip` | `DataFrame` |  | Balls-in-play frame (see `bip_trajectory_features`). |
| `dist_bin` | `float` | `10.0` | Bin width for hit distance, in feet. Defaults to `10.0`. |
| `spray_bin` | `float` | `0.1` | Bin width for spray angle, in radians. Defaults to `0.1`. |
| `alpha` | `float` | `2.0` | Laplace smoothing strength. Defaults to `2.0`. |

**Returns**

one row per observed `(position, dist_b, spray_b, la_bin)`. | Column | Type | Description | |---|---|---| | position | Int64 | Responsible fielder position (Savant `hit_location`, 1-9) | | dist_b | Int64 | Hit-distance bin index | | spray_b | Int64 | Spray-angle bin index | | la_bin | Int64 | Launch-angle bin index (hang-time proxy) | | p_catch | Float64 | Laplace-smoothed empirical out (catch) probability | | n | Int64 | Balls in play observed in this bin |

| col_name | type | description |
|---|---|---|
| `position` | Int64 | Listed roster position (G, F, C, etc.). |
| `dist_b` | Int64 |  |
| `spray_b` | Int64 |  |
| `la_bin` | Int64 |  |
| `p_catch` | Float64 |  |
| `n` | Int64 |  |

**Example**

```python
from sportsdataverse.mlb.mlb_fielding_oaa import catch_prob_surface
surface = catch_prob_surface(bip, alpha=2.0)
```

### fit_zone_model {#fit_zone_model}

`fit_zone_model(pitches: 'pl.DataFrame') -> 'Dict[str, Any]'`

Fit a logistic P(called strike | zone coordinates) on called pitches.

Compute-on-demand -- **no artifact is bundled or cached to disk.**
L2-regularized (`1e-4`) mean log-loss, minimized via
`scipy.optimize.minimize(method="L-BFGS-B")`. When the frame spans 2026
(`game_year` / `game_date`), the 2026+ pitches are excluded with a
`UserWarning`: their locations are middle-of-plate against the ABS zone,
not the frame the rest of the corpus is in
(`rules/mlb.yaml#mlb-2026-savant-plate-location-middle-of-plate`). A frame
entirely from 2026 on is fit on its own pitches.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Frame of pitches with `description` (filtered to `{"called_strike", "ball"}`), `plate_x`, `plate_z`, `sz_top`, `sz_bot`, and `game_year` or `game_date` when the frame spans the 2026 boundary. |

**Returns**

`{"coef": list[float] (7,), "intercept": float, "features": list[str]}`. All-zero `coef` and `intercept` 0.0 (P = 0.5 for every pitch), with a `UserWarning`, if fewer than 2 called pitches are available (degenerate fit).

**Example**

```python
from sportsdataverse.mlb.mlb_umpire_zone import fit_zone_model
model = fit_zone_model(pitches)
```

### mlb_baserunning_value {#mlb_baserunning_value}

`mlb_baserunning_value(events: "'pl.DataFrame'", sprint_speed: "'pl.DataFrame'", *, speed_bin: 'float' = 1.0, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-runner baserunning runs from extra-bases-taken above expected.

Expected extra-base probability is an empirical rate by `(opp_type,
speed_bin)`; `baserunning_runs = extra_bases_above_expected *
RUN_VALUES["extra_base"]`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `events` | `DataFrame` |  | Pitch-level frame passed to `advancement_opportunities`. MiLB feeds run through the same function -- there is no Savant baserunning leaderboard oracle for MiLB. |
| `sprint_speed` | `DataFrame` |  | A `sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_sprint_speed` frame with `runner_id` (Utf8) and `sprint_speed`. |
| `speed_bin` | `float` | `1.0` | Bin width (ft/sec) for the sprint-speed bucket. Defaults to `1.0`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

one row per runner. | Column | Type | Description | |---|---|---| | runner_id | Utf8 | Runner MLBAM id | | opportunities | Int64 | Advancement opportunities faced | | extra_bases_above_expected | Float64 | Sum of (took_extra - expected rate) | | baserunning_runs | Float64 | extra_bases_above_expected x RUN_VALUES["extra_base"] |

No returns table is published for this function: no capture: it raises ColumnNotFoundError (runner_id) on its documented sprint-speed input, which carries player_id.

**Example**

```python
from sportsdataverse.mlb.mlb_baserunning import mlb_baserunning_value
baserunning = mlb_baserunning_value(pitches, sprint_speed)
```

### mlb_catcher_blocking {#mlb_catcher_blocking}

`mlb_catcher_blocking(pitches: "'pl.DataFrame'", *, dirt_bin_width: 'float' = 0.2, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-catcher blocking runs from a dirt-pitch block-probability model.

A block opportunity is a pitch below the strike zone (`pz_norm < 0`)
with a runner on base, or a pitch whose `des` narrates a wild
pitch/passed ball (see module docstring for why `des`, not
`events`). Expected block probability is the empirical block rate
within the pitch's dirt-depth bin; `blocking_runs =
blocks_above_expected * RUN_VALUES["wp_pb"]` (the documented fallback
constant -- the narrating row's `delta_run_exp` bundles the primary
batter outcome with the WP/PB and cannot isolate the latter's value).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Pitch-level frame with `plate_z`/`sz_top`/`sz_bot`, `fielder_2`, `des` (or `events` as a fallback), and (if present) `on_1b`/`on_2b`/`on_3b`. MiLB feeds (e.g. `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search_minors`) run through the same function -- there is no Savant blocking leaderboard oracle for MiLB. |
| `dirt_bin_width` | `float` | `0.2` | Bin width for the below-zone depth bucket. Defaults to `0.2` (zone-normalized units). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

one row per catcher. | Column | Type | Description | |---|---|---| | catcher_id | Utf8 | Catcher MLBAM id (Savant `fielder_2`) | | block_opps | Int64 | Dirt-pitch block opportunities faced | | blocks_above_expected | Float64 | Sum of (blocked - expected block rate) | | blocking_runs | Float64 | blocks_above_expected x RUN_VALUES["wp_pb"] |

| col_name | type | description |
|---|---|---|
| `catcher_id` | String |  |
| `block_opps` | Int64 |  |
| `blocks_above_expected` | Float64 |  |
| `blocking_runs` | Float64 |  |

**Example**

```python
from sportsdataverse.mlb.mlb_catcher_defense import mlb_catcher_blocking
blocking = mlb_catcher_blocking(pitches)

# Pipeline next step (one line)

blocking.filter(pl.col("block_opps") >= 50).sort("blocking_runs", descending=True)
```

### mlb_catcher_framing {#mlb_catcher_framing}

`mlb_catcher_framing(pitches: "'pl.DataFrame'", *, shadow_lo: 'float' = 0.1, shadow_hi: 'float' = 0.9, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-catcher framing runs from a smooth called-strike logistic (Savant method).

A logistic P(called strike | zone location) is fit on all takes (the T6.4
zone model, `sportsdataverse.mlb.mlb_umpire_zone.mlb_umpire_called_strike_prob`);
then, over **shadow-zone** takes only -- those with `shadow_lo <=
P_strike <= shadow_hi`, i.e. near the rulebook edge where receiving
actually moves the call -- `framing_run = (actual_strike - P_strike) *
strike_run_value(count)`. `strike_run_value` is the count's defensive
run value from
`sportsdataverse.mlb.mlb_run_values.count_strike_run_value`. Summed
per catcher (Savant's `fielder_2`, cast `Utf8` at the boundary). The
`takes` column counts *all* takes handled (workload), while the runs sum
only over the frameable shadow-zone subset.

From 2026 every call is challengeable and Savant's location frame moves to
the middle of the plate (`rules/mlb.yaml#mlb-2026-abs-challenge-system`,
`#mlb-2026-savant-plate-location-middle-of-plate`): the location logistic
is fit on pre-2026 takes only (a `UserWarning` when 2026+ rows are
present) and a catcher's 2026+ takes are summed on their own row,
`abs_era = True`, so framing runs earned against challengeable calls are
never added to pre-2026 ones.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Pitch-level frame with the take columns (`plate_x`/`plate_z`/`sz_top`/`sz_bot`/`stand`/ `description`/`balls`/`strikes`/`delta_run_exp`/ `fielder_2`). MiLB feeds (e.g. `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search_minors`) run through the same function -- there is simply no Savant leaderboard oracle to gate MiLB output against. |
| `shadow_lo` | `float` | `0.1` | Lower P(strike) bound of the frameable shadow zone. Defaults to `0.1`. |
| `shadow_hi` | `float` | `0.9` | Upper P(strike) bound of the frameable shadow zone. Defaults to `0.9`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

one row per catcher and era (one per catcher on a frame that does not span 2026). | Column | Type | Description | |---|---|---| | catcher_id | Utf8 | Catcher MLBAM id (Savant `fielder_2`) | | abs_era | Boolean | The takes are from 2026 on: challengeable calls, middle-of-plate frame | | takes | Int64 | Called-strike + ball takes caught (all, workload) | | framing_runs | Float64 | Sum over shadow-zone takes of (actual - P_strike) x count run-value | | strikes_gained | Float64 | Sum over shadow-zone takes of (actual - P_strike), run-value-free |

| col_name | type | description |
|---|---|---|
| `catcher_id` | String |  |
| `abs_era` | Boolean | The pitch is from 2026 on -- Savant location at the middle of the plate against the ABS zone, and the call was challengeable (rules/mlb.yaml#mlb-2026-abs-challenge-system); excluded from the fit, scored and reported on its own row. |
| `takes` | Int64 |  |
| `framing_runs` | Float64 |  |
| `strikes_gained` | Float64 |  |

**Example**

```python
from sportsdataverse.mlb.mlb_catcher_framing import mlb_catcher_framing
framing = mlb_catcher_framing(pitches)

# Useful parameter combination

framing_pd = mlb_catcher_framing(pitches, shadow_lo=0.15, shadow_hi=0.85, return_as_pandas=True)

# Pipeline next step (one line)

framing.filter(pl.col("takes") >= 500).sort("framing_runs", descending=True)
```

### mlb_catcher_throwing {#mlb_catcher_throwing}

`mlb_catcher_throwing(sb_attempts: "'pl.DataFrame'", poptime: "'Optional[pl.DataFrame]'" = None, *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-catcher caught-stealing (throwing) value = caught-stealing above average.

Mirrors Savant's `catcher_stealing_runs` leaderboard: `throwing_runs =
cs_above_expected * |RUN_VALUES["cs"] - RUN_VALUES["sb"]|` where
`cs_above_expected` sums `(caught - expected CS rate)` over the
catcher's attempts. **The expected CS rate is catcher-INDEPENDENT** -- the
empirical league CS rate for the attempt's *difficulty* stratum (the
attempted `base`, since a steal of 3rd is caught far more often than a
steal of 2nd), NOT a function of the catcher's own pop time. The catcher's
arm/pop time/exchange is the *skill* that produces caught-stealings above
that baseline; the **prior implementation conditioned the expectation on
the catcher's own binned pop time, which cancelled exactly the signal
Savant measures** (it correlated ~0 / slightly negative with the
leaderboard -- the fixed model correlates positively).

Run value uses the documented `RUN_VALUES` fallback constants (see
`sportsdataverse.mlb.mlb_stolen_base` for why a real-capture
attempt's `delta_run_exp` cannot isolate the steal's own run value from
the bundled primary batter outcome it is narrated alongside).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sb_attempts` | `DataFrame` |  | One row per stolen-base attempt (see `sportsdataverse.mlb.mlb_stolen_base.sb_attempts_from_pitches`), with `catcher_id` (Utf8), `outcome` (`"success"` \\| `"caught"`) and (for the difficulty baseline) `base`. MiLB feeds run through the same function -- there is no Savant throwing leaderboard oracle for MiLB. |
| `poptime` | `Optional[DataFrame]` | `None` | Accepted for call-site compatibility and **not used** -- pop time is the catcher's own skill (the mechanism producing above-average CS), so it must never enter the *expected* CS model. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

one row per catcher. | Column | Type | Description | |---|---|---| | catcher_id | Utf8 | Catcher MLBAM id | | attempts | Int64 | Stolen-base attempts caught behind the plate | | cs_above_expected | Float64 | Sum of (caught - per-base league CS rate) | | throwing_runs | Float64 | cs_above_expected x \|RUN_VALUES["cs"] - RUN_VALUES["sb"]\| |

| col_name | type | description |
|---|---|---|
| `catcher_id` | String |  |
| `attempts` | Int64 | Number of batted-ball events (attempts). |
| `cs_above_expected` | Float64 |  |
| `throwing_runs` | Float64 |  |

**Example**

```python
from sportsdataverse.mlb.mlb_catcher_defense import mlb_catcher_throwing
throwing = mlb_catcher_throwing(sb_attempts)
```

### mlb_fielding_oaa {#mlb_fielding_oaa}

`mlb_fielding_oaa(bip: "'pl.DataFrame'", *, l2: 'float' = 0.0001, min_fit: 'int' = 50, by_direction: 'bool' = False, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-fielder outs above average from a per-position catch-probability logistic.

`oaa = sum(is_out - p_catch)` per `(fielder_id, position)`, where
`p_catch` is a **smooth per-position logistic** P(out | landing
distance, launch angle, exit velocity, spray angle) (exit velocity x
launch angle proxy the hang time). A position with fewer than `min_fit`
balls in play falls back to its mean out rate. This replaced a coarse
empirical bin surface, roughly halving the gap to Savant's leaderboard
(full-season Pearson ~0.40 -> ~0.60). The fielder id is resolved
dynamically from the responsible position's `fielder_{position}` column
(cast `Utf8` at the boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `bip` | `DataFrame` |  | Balls-in-play frame with `hc_x`/`hc_y`, `hit_distance_sc`, `launch_angle`, `launch_speed`, `hit_location`, `events`, and the `fielder_1`..`fielder_9` responsible-player columns. MiLB input (e.g. `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search_minors`) runs through the same function -- there is no Savant OAA leaderboard oracle for MiLB. |
| `l2` | `float` | `0.0001` | L2 penalty for the per-position logistic. Defaults to `1e-4`. |
| `min_fit` | `int` | `50` | Minimum balls in play for a position to fit its own logistic; below this the position's mean out rate is used. Defaults to `50`. |
| `by_direction` | `bool` | `False` | Split each fielder-position into `in` / `back` / `lateral` buckets, using the position's own median landing spot as a stand-in for the fielder start coordinates the public feed lacks (a documented approximation). The split re-groups the same scored balls in play, so the three rows sum exactly to the undirected `oaa`. Defaults to `False`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

one row per `(fielder_id, position)` -- or per `(fielder_id, position, direction)` when `by_direction=True` -- with `opportunities` and `oaa`. The rendered column table comes from the committed returns schema (`tools/codegen/schemas/autodoc/mlb/mlb_fielding_oaa.yaml`); a Markdown table written here would be collapsed onto one line by the docs renderer, which joins the lines of a `Returns:` block.

| col_name | type | description |
|---|---|---|
| `fielder_id` | String | Responsible fielder's MLBAM id (the fielder charged with the ball in play). |
| `position` | Int64 | Fielding position from Savant's `hit_location`, 1-9. |
| `direction` | String | Movement direction the catch required -- `in`, `back` or `lateral`. Present only when the call passes `by_direction=True`; the three rows re-group the same scored balls in play, so they sum exactly to the undirected `oaa`. |
| `opportunities` | Int64 | Balls in play charged to this fielder (the denominator of the OAA sum). |
| `oaa` | Float64 | Outs above average -- the sum of (out - expected catch probability) over this fielder's opportunities. |

**Example**

```python
from sportsdataverse.mlb.mlb_fielding_oaa import mlb_fielding_oaa
oaa = mlb_fielding_oaa(bip)

# Per-direction splits (sum to the undirected OAA)

mlb_fielding_oaa(bip, by_direction=True)

# Pipeline next step (one line)

oaa.filter(pl.col("opportunities") >= 100).sort("oaa", descending=True)
```

### mlb_injury_risk {#mlb_injury_risk}

`mlb_injury_risk(pitches: 'pl.DataFrame', *, as_of_date: 'Optional[dt.date]' = None, window: 'int' = 5, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Composite pitcher injury-risk index from leakage-safe trailing trends.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw pitch frame (from `sportsdataverse.mlb.mlb_statcast_search`). |
| `as_of_date` | `Optional[date]` | `None` | When given, restricts input to `game_date < as_of_date` via `sportsdataverse.mlb.mlb_pitching_constants.as_of_split` before computing trends (an additional leakage boundary on top of the per-appearance trailing-window logic). |
| `window` | `int` | `5` | Trailing-window size passed to `pitcher_appearance_trends`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `game_pk`, `game_date`, `injury_risk_index` — equal-weighted sum of standardized adverse features (`-velo_trend`, `-velo_drop`, `trailing_workload`, `-days_rest`; higher = more risk). Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | MLB Advanced Media (MLBAM) id for the pitcher. |
| `game_pk` | Int64 | Game identifier. |
| `game_date` | Date | Calendar date of the game (YYYY-MM-DD). |
| `injury_risk_index` | Float64 | Equal-weighted composite of standardized adverse trailing features (higher = more risk). |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_injury import mlb_injury_risk
out = mlb_injury_risk(raw_pitches)
print(out.sort("injury_risk_index", descending=True).head())
```

### mlb_pitch_era {#mlb_pitch_era}

`mlb_pitch_era(pitches: 'pl.DataFrame', seasons: 'int', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Combined xERA + SIERA-like estimator (model ③).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw pitch frame (from `sportsdataverse.mlb.mlb_statcast_search`). |
| `seasons` | `int` |  | Season year. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `season`, `x_woba`, `x_era`, `k_pct`, `bb_pct`, `gb_pct`, `siera_like`. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | MLB Advanced Media (MLBAM) id for the pitcher. |
| `season` | Int64 | MLB season (4-digit start year). |
| `x_woba` | Float64 | Mean estimated_woba_using_speedangle allowed on batted balls. |
| `x_era` | Float64 | Parametric xERA converted from x_woba via the season's league baselines. |
| `k_pct` | Float64 | Strikeout rate (strikeouts / batters faced). |
| `bb_pct` | Float64 | Walk rate (walks + HBP / batters faced). |
| `gb_pct` | Float64 | Ground-ball rate among batted balls. |
| `siera_like` | Float64 | SIERA-like ERA estimate from the fitted K%/BB%/GB% OLS coefficients. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_era import mlb_pitch_era
out = mlb_pitch_era(raw_pitches, 2024)
print(out.select("pitcher", "x_era", "siera_like").head())
```

### mlb_pitch_tunneling {#mlb_pitch_tunneling}

`mlb_pitch_tunneling(pitches: 'pl.DataFrame', *, eps: 'float' = 0.01, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-pitch release/plate distance from the previous pitch + tunnel ratio.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.add_sequence_features` (needs `release_pos_x`/`release_pos_z`, `prev_release_pos_x`/ `prev_release_pos_z`, `plate_x`/`plate_z`, `prev_plate_x`/`prev_plate_z`). |
| `eps` | `float` | `0.01` | Minimum `release_dist` denominator (avoids divide-by-zero). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `game_pk`, `at_bat_number`, `pitch_number`, `release_dist`, `plate_dist`, `tunnel_ratio`. First pitch of a plate appearance (no previous pitch) has null geometry. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | MLB Advanced Media (MLBAM) id for the pitcher. |
| `game_pk` | Int64 | Game identifier. |
| `at_bat_number` | Int64 | Game-level plate-appearance sequence number. |
| `pitch_number` | Int64 | Pitch sequence number within the plate appearance. |
| `release_dist` | Float64 | Euclidean distance between this pitch's release point and the previous pitch's release point. |
| `plate_dist` | Float64 | Euclidean distance between this pitch's plate location and the previous pitch's plate location. |
| `tunnel_ratio` | Float64 | plate_dist / max(release_dist, eps) -- higher means better tunneling (similar release, different result). |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
from sportsdataverse.mlb.mlb_pitch_sequencing import mlb_pitch_tunneling
feats = add_sequence_features(pitch_features(raw_pitches))
out = mlb_pitch_tunneling(feats)
print(out.select("tunnel_ratio").describe())
```

### mlb_sequence_run_value {#mlb_sequence_run_value}

`mlb_sequence_run_value(pitches: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Mean run value grouped by the ordered `(prev_pitch_type, pitch_type)` sequence.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.add_sequence_features` (needs `prev_pitch_type`, `pitch_type`, `run_value` / `delta_run_exp`). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`prev_pitch_type`, `pitch_type`, `mean_run_value`, `n` — one row per observed ordered pair (rows with a null `prev_pitch_type`, i.e. the first pitch of a PA, are dropped). Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `prev_pitch_type` | String | Pitch type of the preceding pitch in the sequence. |
| `pitch_type` | String | Pitch type of the current pitch. |
| `mean_run_value` | Float64 | Mean run value of the current pitch, grouped by the ordered (prev_pitch_type, pitch_type) pair. |
| `n` | Int64 | Number of pitches observed for this ordered pair. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_sequencing import mlb_sequence_run_value
out = mlb_sequence_run_value(feats)
print(out.sort("mean_run_value").head())
```

### mlb_stolen_base_value {#mlb_stolen_base_value}

`mlb_stolen_base_value(sb_attempts: "'pl.DataFrame'", sprint_speed: "'pl.DataFrame'", poptime: "'pl.DataFrame'", *, speed_bin: 'float' = 0.5, pop_bin: 'float' = 0.05, pop_col: 'str' = 'pop_2b_sba', alpha: 'float' = 2.0, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-runner stolen-base run value: realized-vs-expected run contribution.

`sb_run_value = sum(p_success * RUN_VALUES["sb"] + (1 - p_success) *
RUN_VALUES["cs"])` per attempt -- the documented fallback constants
(see module docstring for why, not
`sportsdataverse.mlb.mlb_run_values.event_run_value` on these
bundled-`des` rows), weighted by the surface's modeled success
probability for that attempt's bin.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sb_attempts` | `DataFrame` |  | One row per attempt (see `sb_attempts_from_pitches`). MiLB feeds run through the same function -- there is no Savant basestealing leaderboard oracle for MiLB. |
| `sprint_speed` | `DataFrame` |  | Sprint-speed leaderboard frame (`runner_id`, `sprint_speed`). |
| `poptime` | `DataFrame` |  | Pop-time leaderboard frame (`catcher_id`, `pop_col`). |
| `speed_bin` | `float` | `0.5` | Sprint-speed bin width. Defaults to `0.5`. |
| `pop_bin` | `float` | `0.05` | Pop-time bin width. Defaults to `0.05`. |
| `pop_col` | `str` | `'pop_2b_sba'` | Pop-time column name in `poptime`. Defaults to `"pop_2b_sba"`. |
| `alpha` | `float` | `2.0` | Laplace smoothing strength for the surface. Defaults to `2.0`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

one row per runner. | Column | Type | Description | |---|---|---| | runner_id | Utf8 | Runner MLBAM id | | attempts | Int64 | Stolen-base attempts | | p_success_mean | Float64 | Mean modeled success probability across attempts | | sb_run_value | Float64 | Sum of realized-vs-expected run contribution |

| col_name | type | description |
|---|---|---|
| `runner_id` | String |  |
| `attempts` | Int64 | Number of batted-ball events (attempts). |
| `p_success_mean` | Float64 |  |
| `sb_run_value` | Float64 |  |

**Example**

```python
from sportsdataverse.mlb.mlb_stolen_base import mlb_stolen_base_value
sb_value = mlb_stolen_base_value(sb_attempts, sprint_speed, poptime)
```

### mlb_times_through_order {#mlb_times_through_order}

`mlb_times_through_order(pitches: 'pl.DataFrame', *, season: 'int' = 2024, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-pitch fitted times-through-order fatigue adjustment.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.add_sequence_features` (needs `times_through_order`). |
| `season` | `int` | `2024` | Season year, selects the fitted `tto_penalty` coefficients via `sportsdataverse.mlb.mlb_pitching_constants.get_baselines`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `game_pk`, `at_bat_number`, `pitch_number`, `times_through_order`, `fatigue_rv_adj` (the fitted marginal penalty for that TTO level). Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | MLB Advanced Media (MLBAM) id for the pitcher. |
| `game_pk` | Int64 | Game identifier. |
| `at_bat_number` | Int64 | Game-level plate-appearance sequence number. |
| `pitch_number` | Int64 | Pitch sequence number within the plate appearance. |
| `times_through_order` | Int64 | Times through the batting order (1-3). |
| `fatigue_rv_adj` | Float64 | Fitted per-TTO run-value marginal (mlb_pitching_constants.tto_penalty) for this pitch's TTO level. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
from sportsdataverse.mlb.mlb_pitch_fatigue import mlb_times_through_order
feats = add_sequence_features(pitch_features(raw_pitches))
out = mlb_times_through_order(feats, season=2024)
print(out.select("times_through_order", "fatigue_rv_adj").unique())
```

### mlb_umpire_bias {#mlb_umpire_bias}

`mlb_umpire_bias(pitches: 'pl.DataFrame', *, model: 'Optional[Dict[str, Any]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Per-umpire called-strike bias residual (observed minus expected).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Called pitches with `umpire_id`, `description`, `plate_x`, `plate_z`, `sz_top`, `sz_bot`. |
| `model` | `Optional[Dict[str, Any]]` | `None` | Pre-fit model dict from `fit_zone_model`; fits on `pitches` itself when `None`. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per umpire and era (`abs_era` splits an umpire's 2026+ calls from the earlier ones; a frame on one side of 2026 keeps one row per umpire). | Column | Type | Description | |---|---|---| | umpire_id | Utf8 | Umpire identifier | | abs_era | Boolean | The calls are from 2026 on: challengeable, scored in the new plate frame | | n_called | Int64 | Called pitches observed for this umpire | | obs_strike_rate | Float64 | Realized called-strike rate | | exp_strike_rate | Float64 | Mean model-predicted called-strike probability | | bias | Float64 | obs_strike_rate - exp_strike_rate (positive = strike-generous) |

| col_name | type | description |
|---|---|---|
| `umpire_id` | String | Umpire identifier (statsapi people id, stringified). |
| `abs_era` | Boolean | The pitch is from 2026 on -- Savant location at the middle of the plate against the ABS zone, and the call was challengeable (rules/mlb.yaml#mlb-2026-abs-challenge-system); excluded from the fit, scored and reported on its own row. |
| `n_called` | Int64 | Called pitches (strike or ball) observed for this umpire. |
| `obs_strike_rate` | Float64 | Realized called-strike rate for this umpire. |
| `exp_strike_rate` | Float64 | Mean model-predicted called-strike probability for this umpire's pitches. |
| `bias` | Float64 | obs_strike_rate minus exp_strike_rate (positive = strike-generous). |

**Example**

```python
from sportsdataverse.mlb.mlb_umpire_zone import mlb_umpire_bias
bias = mlb_umpire_bias(pitches)
```

### mlb_umpire_called_strike_prob {#mlb_umpire_called_strike_prob}

`mlb_umpire_called_strike_prob(pitches: 'pl.DataFrame', *, model: 'Optional[Dict[str, Any]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

P(called strike) per pitch from the zone logistic.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Frame with `plate_x`, `plate_z`, `sz_top`, `sz_bot` (one row per pitch, not required to be called pitches only). |
| `model` | `Optional[Dict[str, Any]]` | `None` | Pre-fit model dict from `fit_zone_model`; fits on `pitches` itself when `None` (using only its called pitches). |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per input pitch. | Column | Type | Description | |---|---|---| | called_strike_prob | Float64 | P(called strike \| pitch location) | | abs_era | Boolean | The pitch is from 2026 on: middle-of-plate location, ABS zone, challengeable call |

| col_name | type | description |
|---|---|---|
| `called_strike_prob` | Float64 | P(called strike \| pitch location) from the standardized zone-coordinate logistic. |
| `abs_era` | Boolean | The pitch is from 2026 on -- Savant location at the middle of the plate against the ABS zone, and the call was challengeable (rules/mlb.yaml#mlb-2026-abs-challenge-system); excluded from the fit, scored and reported on its own row. |

**Example**

```python
from sportsdataverse.mlb.mlb_umpire_zone import mlb_umpire_called_strike_prob
prob = mlb_umpire_called_strike_prob(pitches)
```

### pitch_features {#pitch_features}

`pitch_features(pitches: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Build the per-pitch feature substrate every pitching model consumes.

Standardizes physics (velocity/spin/movement/release/extension) within
`pitcher` and season (`game_year`, else the year of `game_date`),
derives strike-zone-relative location features, pins id columns to
`Int64`, and passes Savant's per-pitch `delta_run_exp` through
unchanged as `run_value` (the single run-value label used by
Stuff+/Command+/TTO/tunneling). The season is part of the standardization
key because Savant's `release_speed` (and release point / spin) switched
from PITCHf/x readings at 50 ft to TrackMan readings at release in 2017, a
level shift of about +0.5-1 mph
(`rules/mlb.yaml#mlb-2017-release-speed-statcast`); pooling a pitcher's
2016 and 2017 pitches in one mean would read the source change as a
velocity change. A frame with neither season column standardizes within
`pitcher` alone.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw Savant pitch frame (e.g. from `sportsdataverse.mlb.mlb_statcast_search`), one row per pitch, carrying `pitcher`, `release_speed`, `release_spin_rate`, `pfx_x`, `pfx_z`, `release_pos_x`, `release_pos_z`, `release_extension`, `plate_x`, `plate_z`, `sz_top`, `sz_bot`, `delta_run_exp`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

One row per pitch with the input columns plus `velo_z`, `spin_z`, `pfx_x_z`, `pfx_z_z`, `release_pos_x_z`, `release_pos_z_z`, `extension_z` (standardized within `pitcher` and season), `plate_z_norm`, `plate_x_abs`, `in_zone`, `dist_from_heart`, and `run_value`. Empty/malformed input returns a zero-row frame carrying the added schema.

| col_name | type | description |
|---|---|---|
| `velo_z` | Float64 | Release speed standardized (z-score) within pitcher. |
| `spin_z` | Float64 | Release spin rate standardized (z-score) within pitcher. |
| `pfx_x_z` | Float64 | Horizontal movement (pfx_x) standardized (z-score) within pitcher. |
| `pfx_z_z` | Float64 | Vertical movement (pfx_z) standardized (z-score) within pitcher. |
| `release_pos_x_z` | Float64 | Horizontal release position standardized (z-score) within pitcher. |
| `release_pos_z_z` | Float64 | Vertical release position standardized (z-score) within pitcher. |
| `extension_z` | Float64 | Release extension standardized (z-score) within pitcher. |
| `run_value` | Float64 | Savant per-pitch delta_run_exp, passed through unchanged as the spine's single run-value label. |
| `plate_x_abs` | Float64 | Absolute horizontal plate location (distance from the center of the zone). |
| `plate_z_norm` | Float64 | Vertical plate location normalized to the batter's own strike zone, 0 = bottom, 1 = top. |
| `in_zone` | Int64 | 1 if the pitch crossed the strike zone (normalized location + horizontal bound), else 0. |
| `dist_from_heart` | Float64 | Euclidean distance from the normalized zone center (0, 0.5) -- lower is more hittable. |

**Example**

```python
from sportsdataverse.mlb import mlb_statcast_search
from sportsdataverse.mlb.mlb_pitch_features import pitch_features
raw = mlb_statcast_search("2024-06-15", "2024-06-15", player_type="pitcher")
feats = pitch_features(raw)
print(feats.select("pitch_type", "in_zone", "run_value").head())

# Pipeline next step

feats.filter(pl.col("in_zone") == 1).group_by("pitch_type").agg(pl.col("run_value").mean())
```

### pitcher_appearance_trends {#pitcher_appearance_trends}

`pitcher_appearance_trends(pitches: 'pl.DataFrame', *, window: 'int' = 5, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Leakage-safe per-appearance trailing velocity/workload trends.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw (or feature-substrate) pitch frame carrying `pitcher`, `game_pk`, `game_date`, `pitch_type`, `release_speed`. |
| `window` | `int` | `5` | Number of trailing PRIOR appearances used for the rolling statistics (never includes the current appearance). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(pitcher, game_pk, game_date)`: `fb_velo` (this game's mean fastball `release_speed`), `velo_trend` (OLS slope of `fb_velo` over the trailing `window` prior appearances), `velo_drop` (trailing-baseline mean minus this game's `fb_velo`), `pitches_game`, `trailing_workload` (mean `pitches_game` over the trailing window), `days_rest`. The first appearance for a pitcher has null trailing stats (no prior data). Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | MLB Advanced Media (MLBAM) id for the pitcher. |
| `game_pk` | Int64 | Game identifier. |
| `game_date` | Date | Calendar date of the game (YYYY-MM-DD). |
| `fb_velo` | Float64 | Mean fastball release speed for this appearance. |
| `velo_trend` | Float64 | OLS slope of fb_velo over the trailing prior appearances (leakage-safe). |
| `velo_drop` | Float64 | Trailing-baseline mean fb_velo minus this appearance's fb_velo. |
| `pitches_game` | Int64 | Pitches thrown in this appearance. |
| `trailing_workload` | Float64 | Mean pitches_game over the trailing prior appearances (leakage-safe). |
| `days_rest` | Float64 | Days since the pitcher's previous appearance. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_injury import pitcher_appearance_trends
out = pitcher_appearance_trends(raw_pitches, window=5)
print(out.select("game_date", "velo_drop", "days_rest").tail())
```

### predict_sb_success {#predict_sb_success}

`predict_sb_success(upcoming: "'pl.DataFrame'", history: "'pl.DataFrame'", cutoff_date: 'Any', *, speed_bin: 'float' = 0.5, pop_bin: 'float' = 0.05, pop_col: 'str' = 'pop_2b_sba', alpha: 'float' = 2.0, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

As-of-date predictive P(success): the surface is fit on history strictly before `cutoff_date`.

The leakage boundary: `sportsdataverse.mlb.mlb_run_values.as_of_split`
drops every `history` row with `game_date >= cutoff_date` before the
success-rate grid is built, so `upcoming` attempts are scored only
against what was knowable at that date.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `upcoming` | `DataFrame` |  | Attempts to score, each carrying `runner_id`, `base`, `sprint_speed`, and the `pop_col` pop-time column. |
| `history` | `DataFrame` |  | Prior attempts with `game_date`, `outcome`, `sprint_speed`, and `pop_col` -- used to fit the surface via `as_of_split`. |
| `cutoff_date` | `Any` |  | Exclusive upper bound on `history["game_date"]`. |
| `speed_bin` | `float` | `0.5` | Sprint-speed bin width. Defaults to `0.5`. |
| `pop_bin` | `float` | `0.05` | Pop-time bin width. Defaults to `0.05`. |
| `pop_col` | `str` | `'pop_2b_sba'` | Pop-time column name. Defaults to `"pop_2b_sba"`. |
| `alpha` | `float` | `2.0` | Laplace smoothing strength. Defaults to `2.0`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

one row per scored attempt. | Column | Type | Description | |---|---|---| | runner_id | Utf8 | Runner MLBAM id | | base | Utf8 | Attempted base | | p_success | Float64 | Modeled success probability, as-of `cutoff_date` |

| col_name | type | description |
|---|---|---|
| `runner_id` | String |  |
| `base` | String |  |
| `p_success` | Float64 |  |

**Example**

```python
from sportsdataverse.mlb.mlb_stolen_base import predict_sb_success
preds = predict_sb_success(upcoming, history, cutoff_date=dt.date(2024, 6, 15))
```

### sb_attempts_from_pitches {#sb_attempts_from_pitches}

`sb_attempts_from_pitches(pitches: "'pl.DataFrame'") -> "'pl.DataFrame'"`

Extract stolen-base / caught-stealing attempts from pitch-level Statcast rows.

Detects attempts via a `des` regex (see module docstring for why --
the `events` column does not carry these in the flat per-pitch search)
and reads the attempting runner off the pre-play occupancy column
implied by the attempted base (2B attempt -> `on_1b`, 3B -> `on_2b`,
home -> `on_3b`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | A `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search` frame with `des`, `fielder_2`, `on_1b`/`on_2b`/`on_3b`, and (if present) `game_date`. |

**Returns**

one row per attempt. | Column | Type | Description | |---|---|---| | game_date | Date | Game date (if present in the input) | | runner_id | Utf8 | Attempting runner's MLBAM id | | catcher_id | Utf8 | Catcher MLBAM id (Savant `fielder_2`) | | base | Utf8 | `2B` \| `3B` \| `HOME` | | outcome | Utf8 | `success` \| `caught` |

| col_name | type | description |
|---|---|---|
| `game_date` | String | Game date (YYYY-MM-DD). |
| `runner_id` | String |  |
| `catcher_id` | String |  |
| `base` | String |  |
| `outcome` | String |  |

**Example**

```python
from sportsdataverse.mlb.mlb_stolen_base import sb_attempts_from_pitches
sb_attempts = sb_attempts_from_pitches(pitches)
```

### sb_success_surface {#sb_success_surface}

`sb_success_surface(sb_attempts: "'pl.DataFrame'", sprint_speed: "'pl.DataFrame'", poptime: "'pl.DataFrame'", *, speed_bin: 'float' = 0.5, pop_bin: 'float' = 0.05, pop_col: 'str' = 'pop_2b_sba', alpha: 'float' = 2.0) -> "'pl.DataFrame'"`

Empirical P(stolen-base success) surface over `(sprint speed, pop time, base)`.

Rate per bin is Laplace-smoothed: `(successes + alpha) / (n + 2 * alpha)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sb_attempts` | `DataFrame` |  | One row per attempt (`runner_id`, `catcher_id`, `base`, `outcome`). |
| `sprint_speed` | `DataFrame` |  | A `sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_sprint_speed` frame with `runner_id` (Utf8) and `sprint_speed`. |
| `poptime` | `DataFrame` |  | A `sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_poptime` frame with `catcher_id` (Utf8) and the pop-time column named by `pop_col`. |
| `speed_bin` | `float` | `0.5` | Bin width (ft/sec) for sprint speed. Defaults to `0.5`. |
| `pop_bin` | `float` | `0.05` | Bin width (seconds) for pop time. Defaults to `0.05`. |
| `pop_col` | `str` | `'pop_2b_sba'` | Name of the pop-time column in `poptime`. Defaults to `"pop_2b_sba"`. |
| `alpha` | `float` | `2.0` | Laplace smoothing strength. Defaults to `2.0`. |

**Returns**

one row per observed `(speed_b, pop_b, base)`. | Column | Type | Description | |---|---|---| | speed_b | Int64 | Sprint-speed bin index | | pop_b | Int64 | Pop-time bin index | | base | Utf8 | Attempted base | | p_success | Float64 | Laplace-smoothed empirical success probability | | n | Int64 | Attempts observed in this bin |

| col_name | type | description |
|---|---|---|
| `speed_b` | Int64 |  |
| `pop_b` | Int64 |  |
| `base` | String |  |
| `p_success` | Float64 |  |
| `n` | Int64 |  |

**Example**

```python
from sportsdataverse.mlb.mlb_stolen_base import sb_success_surface
surface = sb_success_surface(sb_attempts, sprint_speed, poptime)
```

### siera_like {#siera_like}

`siera_like(pitches: 'pl.DataFrame', season: 'int', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

SIERA-like ERA estimator from K%/BB%/GB% (**experimental / provisional**).

Evaluates the published SIERA functional form with
`mlb_pitching_constants.siera_coef`, which are **SEEDED literature
placeholders** (not yet OLS-fitted — the Task-4.2 next-season-ERA fit has
not landed). Treat the output as directionally indicative, not a calibrated
ERA; use `x_era` (oracle-gated vs Savant's xERA) for a fitted number.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw pitch frame carrying `pitcher`, `events`, and (optionally) `bb_type`. |
| `season` | `int` |  | Season year (unused in the formula itself, carried through for join convenience with `x_era`). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `season`, `k_pct`, `bb_pct`, `gb_pct`, `siera_like`. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | MLB Advanced Media (MLBAM) id for the pitcher. |
| `season` | Int64 | Season year (carried through for join convenience with x_era). |
| `k_pct` | Float64 | Strikeout rate (strikeouts / batters faced). |
| `bb_pct` | Float64 | Walk rate (walks + HBP / batters faced). |
| `gb_pct` | Float64 | Ground-ball rate among batted balls. |
| `siera_like` | Float64 | SIERA-like ERA estimate from the fitted K%/BB%/GB% OLS coefficients. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_era import siera_like
out = siera_like(raw_pitches, 2024)
print(out.sort("siera_like").head())
```

### tto_penalty_table {#tto_penalty_table}

`tto_penalty_table(feats: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Observed mean run value by times-through-order, with the penalty vs TTO=1.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `feats` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.add_sequence_features` (needs `times_through_order` and `run_value`). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`times_through_order`, `mean_run_value`, `penalty_vs_first` (`mean_run_value` minus the TTO=1 mean run value), `n`. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `times_through_order` | Int64 | Times through the batting order (1-3). |
| `mean_run_value` | Float64 | Mean observed run value for pitches at this TTO level. |
| `penalty_vs_first` | Float64 | mean_run_value minus the TTO=1 mean run value. |
| `n` | Int64 | Number of pitches observed at this TTO level. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
from sportsdataverse.mlb.mlb_pitch_fatigue import tto_penalty_table
feats = add_sequence_features(pitch_features(raw_pitches))
out = tto_penalty_table(feats)
print(out.sort("times_through_order"))
```

### x_era {#x_era}

`x_era(pitches: 'pl.DataFrame', season: 'int', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Parametric xERA from Statcast expected wOBA allowed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw (or feature-substrate) pitch frame carrying `pitcher` and `estimated_woba_using_speedangle`. |
| `season` | `int` |  | Season year (selects the league baseline via `sportsdataverse.mlb.mlb_pitching_constants.get_baselines`). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `season`, `x_woba`, `x_era`. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | Whether the position is a pitcher. |
| `season` | Int64 | Season year. |
| `x_woba` | Float64 | Expected wOBA allowed, the mean of Statcast estimated_woba_using_speedangle over the pitcher's pitches that carry one (batted balls). |
| `x_era` | Float64 | Parametric xERA, league ERA + (x_woba - league wOBA) / wOBA scale * PA per 9, from the season's league baselines. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_era import x_era
out = x_era(raw_pitches, 2024)
print(out.sort("x_era").head())
```
