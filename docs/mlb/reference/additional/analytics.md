# MLB — additional Python functions — Analytics

> MLB — additional Python functions — Analytics — function reference in sdv-py, the SportsDataverse Python package.

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
| `prev_pitch_type` | character | Pitch type of the previous pitch in the same plate appearance (null on the first pitch). |
| `prev_release_pos_x` | double | Horizontal release position of the previous pitch in the same plate appearance. |
| `prev_release_pos_z` | double | Vertical release position of the previous pitch in the same plate appearance. |
| `prev_plate_x` | double | Horizontal plate location of the previous pitch in the same plate appearance. |
| `prev_plate_z` | double | Vertical plate location of the previous pitch in the same plate appearance. |
| `cum_pitches_game` | integer | Running count of pitches thrown by this pitcher so far in this game (inclusive of the current pitch). |
| `batter_faced_index` | integer | Dense rank of this plate appearance's at_bat_number within the game (1 = first batter faced). |
| `times_through_order` | integer | Times through the batting order, min(3, (batter_faced_index-1)//9 + 1). |

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
| `runner_id` | character |  |
| `opp_type` | character |  |
| `took_extra` | integer |  |

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
| `pitch_type` | character | Abbreviation of the pitch type thrown (e.g. FF, SL, CH). |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `release_speed` | double | Pitch velocity out of the hand (mph). |
| `release_pos_x` | double | Horizontal release position of the ball, catcher's perspective (feet). |
| `release_pos_z` | double | Vertical release position of the ball, catcher's perspective (feet). |
| `player_name` | character | Player name. |
| `batter` | integer | MLBAM player id of the batter. |
| `pitcher` | integer | Whether the position is a pitcher. |
| `events` | character | Nested list of non-game events. |
| `description` | character | Long-form description text. |
| `spin_dir` | double | Deprecated spin direction field, no longer populated. |
| `spin_rate_deprecated` | double | Deprecated legacy spin-rate field, no longer populated. |
| `break_angle_deprecated` | double | Deprecated legacy break-angle field, no longer populated. |
| `break_length_deprecated` | double | Deprecated legacy break-length field, no longer populated. |
| `zone` | double | Strike-zone region the pitch crossed (1-14 Gameday zone). |
| `des` | character | Full text description of the play. |
| `game_type` | character | Game type code (R, P, etc.). |
| `stand` | character | Side of the plate the batter is standing (L or R). |
| `p_throws` | character | Hand the pitcher throws with (L or R). |
| `home_team` | character | Home team name. |
| `away_team` | character | Away team name. |
| `type` | character | Record type / category. |
| `hit_location` | double | Fielder position number that fielded the ball. |
| `bb_type` | character | Batted-ball type (ground_ball, line_drive, fly_ball, popup). |
| `balls` | integer | Ball count before the pitch. |
| `strikes` | integer | Strike count before the pitch. |
| `game_year` | integer | Season year of the game. |
| `pfx_x` | double | Horizontal pitch movement from the catcher's perspective (feet). |
| `pfx_z` | double | Vertical pitch movement from the catcher's perspective (feet). |
| `plate_x` | double | Horizontal position of the pitch crossing the plate (feet from center). |
| `plate_z` | double | Vertical position of the pitch crossing the plate (feet above ground). |
| `on_3b` | integer | MLBAM ID of the runner on third base, if any. |
| `on_2b` | integer | MLBAM ID of the runner on second base, if any. |
| `on_1b` | integer | MLBAM ID of the runner on first base, if any. |
| `outs_when_up` | integer | Number of outs when the batter came to the plate. |
| `inning` | integer | Inning number. |
| `inning_topbot` | character | Half of the inning (Top or Bot). |
| `hc_x` | double | Hit coordinate X on the field diagram. |
| `hc_y` | double | Hit coordinate Y on the field diagram. |
| `tfs_deprecated` | double | Deprecated time-from-start field, no longer populated. |
| `tfs_zulu_deprecated` | double | Deprecated Zulu time-from-start field, no longer populated. |
| `umpire` | double | Deprecated umpire field, no longer populated. |
| `sv_id` | double | Deprecated Sportvision/Statcast pitch identifier, no longer populated. |
| `vx0` | double | Velocity of the pitch in the x-direction at y=50 ft (ft/s). |
| `vy0` | double | Velocity of the pitch in the y-direction at y=50 ft (ft/s). |
| `vz0` | double | Velocity of the pitch in the z-direction at y=50 ft (ft/s). |
| `ax` | double | Acceleration of the pitch in the x-direction at y=50 ft (ft/s^2). |
| `ay` | double | Acceleration of the pitch in the y-direction at y=50 ft (ft/s^2). |
| `az` | double | Acceleration of the pitch in the z-direction at y=50 ft (ft/s^2). |
| `sz_top` | double | Top of the batter's strike zone for the pitch (feet). |
| `sz_bot` | double | Bottom of the batter's strike zone for the pitch (feet). |
| `hit_distance_sc` | double | Statcast-measured projected distance of the batted ball (feet). |
| `launch_speed` | double | Exit velocity of the batted ball (mph). |
| `launch_angle` | double | Vertical launch angle of the batted ball (degrees). |
| `effective_speed` | double | Perceived velocity adjusted for release extension (mph). |
| `release_spin_rate` | double | Spin rate of the pitch at release (rpm). |
| `release_extension` | double | Distance toward the plate at release (feet). |
| `game_pk` | integer | Unique game identifier. |
| `fielder_2` | integer | MLBAM ID of the catcher. |
| `fielder_3` | integer | MLBAM ID of the first baseman. |
| `fielder_4` | integer | MLBAM ID of the second baseman. |
| `fielder_5` | integer | MLBAM ID of the third baseman. |
| `fielder_6` | integer | MLBAM ID of the shortstop. |
| `fielder_7` | integer | MLBAM ID of the left fielder. |
| `fielder_8` | integer | MLBAM ID of the center fielder. |
| `fielder_9` | integer | MLBAM ID of the right fielder. |
| `release_pos_y` | double | Release position of the ball toward the plate (feet). |
| `estimated_ba_using_speedangle` | double | Expected batting average based on exit velocity and launch angle. |
| `estimated_woba_using_speedangle` | double | Expected wOBA based on exit velocity and launch angle. |
| `woba_value` | double | wOBA value assigned to the event. |
| `woba_denom` | double | wOBA denominator (plate-appearance weight) for the event. |
| `babip_value` | double | BABIP value assigned to the event (0 or 1). |
| `iso_value` | double | Isolated power value assigned to the event. |
| `launch_speed_angle` | double | Batted-ball classification code (1-6) from exit velocity and angle. |
| `at_bat_number` | integer | Sequential plate-appearance number within the game. |
| `pitch_number` | integer | Pitch number within the plate appearance. |
| `pitch_name` | character | Full name of the pitch type (e.g. 4-Seam Fastball, Slider). |
| `home_score` | integer | Home team run total after the play. |
| `away_score` | integer | Away team run total after the play. |
| `bat_score` | integer | Batting team score before the pitch. |
| `fld_score` | integer | Fielding team score before the pitch. |
| `post_away_score` | integer | Away team score after the pitch. |
| `post_home_score` | integer | Home team score after the pitch. |
| `post_bat_score` | integer | Batting team score after the pitch. |
| `post_fld_score` | integer | Fielding team score after the pitch. |
| `if_fielding_alignment` | character | Infield defensive alignment (Standard, Strategic, Infield shift). |
| `of_fielding_alignment` | character | Outfield defensive alignment (Standard, Strategic, 4th outfielder). |
| `spin_axis` | double | Spin axis of the pitch as a clock-face angle (degrees). |
| `delta_home_win_exp` | double | Change in home team win expectancy on the play. |
| `delta_run_exp` | double | Change in run expectancy on the play. |
| `bat_speed` | double | Bat speed at the point of contact (mph). |
| `swing_length` | double | Length of the swing path to contact (feet). |
| `miss_distance` | double |  |
| `estimated_slg_using_speedangle` | double | Expected slugging based on exit velocity and launch angle. |
| `delta_pitcher_run_exp` | double | Change in run expectancy credited to the pitcher. |
| `hyper_speed` | double | Adjusted (90th-percentile) exit velocity (mph). |
| `home_score_diff` | integer | Home team score minus away team score before the pitch. |
| `bat_score_diff` | integer | Batting team score minus fielding team score before the pitch. |
| `home_win_exp` | double | Home team win expectancy before the play. |
| `bat_win_exp` | double | Batting team win expectancy before the play. |
| `age_pit_legacy` | integer | Pitcher age using the legacy calculation. |
| `age_bat_legacy` | integer | Batter age using the legacy calculation. |
| `age_pit` | integer | Pitcher age for the season. |
| `age_bat` | integer | Batter age for the season. |
| `n_thruorder_pitcher` | integer | Times through the order the pitcher is facing the lineup. |
| `n_priorpa_thisgame_player_at_bat` | integer | Number of prior plate appearances by the batter in the game. |
| `pitcher_days_since_prev_game` | double | Days since the pitcher's previous game appearance. |
| `batter_days_since_prev_game` | integer | Days since the batter's previous game appearance. |
| `pitcher_days_until_next_game` | double | Days until the pitcher's next game appearance. |
| `batter_days_until_next_game` | double | Days until the batter's next game appearance. |
| `api_break_z_with_gravity` | double | Vertical pitch break including gravity (inches). |
| `api_break_x_arm` | double | Horizontal pitch break to the pitcher's arm side (inches). |
| `api_break_x_batter_in` | double | Horizontal pitch break toward/away from the batter (inches). |
| `arm_angle` | double | Pitcher's arm angle at release (degrees). |
| `attack_angle` | double | Angle of the bat's path at contact (degrees). |
| `attack_direction` | double | Horizontal direction of the swing at contact (degrees). |
| `swing_path_tilt` | double | Vertical tilt of the swing path (degrees). |
| `intercept_ball_minus_batter_pos_x_inches` | double | Horizontal offset of ball-bat intercept from batter position (inches). |
| `intercept_ball_minus_batter_pos_y_inches` | double | Depth offset of ball-bat intercept from batter position (inches). |
| `spray_angle` | double |  |
| `hit_dist` | double |  |
| `la_bin` | integer |  |
| `is_out` | integer |  |
| `position` | integer | Listed roster position (G, F, C, etc.). |

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
| `stand` | character | Side of the plate the batter is standing (L or R). |
| `px_bin` | integer |  |
| `pz_bin` | integer |  |
| `p_strike` | double |  |
| `n` | integer |  |

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
| `position` | integer | Listed roster position (G, F, C, etc.). |
| `dist_b` | integer |  |
| `spray_b` | integer |  |
| `la_bin` | integer |  |
| `p_catch` | double |  |
| `n` | integer |  |

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
`scipy.optimize.minimize(method="L-BFGS-B")`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Frame of pitches with `description` (filtered to `{"called_strike", "ball"}`), `plate_x`, `plate_z`, `sz_top`, `sz_bot`. |

**Returns**

`{"coef": list[float] (7,), "intercept": float, "features": list[str]}`. `{"coef": [], "intercept": 0.0, "features": [...]}` if fewer than 2 called pitches are available (degenerate fit).

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
| `catcher_id` | character |  |
| `block_opps` | integer |  |
| `blocks_above_expected` | double |  |
| `blocking_runs` | double |  |

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

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Pitch-level frame with the take columns (`plate_x`/`plate_z`/`sz_top`/`sz_bot`/`stand`/ `description`/`balls`/`strikes`/`delta_run_exp`/ `fielder_2`). MiLB feeds (e.g. `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search_minors`) run through the same function -- there is simply no Savant leaderboard oracle to gate MiLB output against. |
| `shadow_lo` | `float` | `0.1` | Lower P(strike) bound of the frameable shadow zone. Defaults to `0.1`. |
| `shadow_hi` | `float` | `0.9` | Upper P(strike) bound of the frameable shadow zone. Defaults to `0.9`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

one row per catcher. | Column | Type | Description | |---|---|---| | catcher_id | Utf8 | Catcher MLBAM id (Savant `fielder_2`) | | takes | Int64 | Called-strike + ball takes caught (all, workload) | | framing_runs | Float64 | Sum over shadow-zone takes of (actual - P_strike) x count run-value | | strikes_gained | Float64 | Sum over shadow-zone takes of (actual - P_strike), run-value-free |

| col_name | type | description |
|---|---|---|
| `catcher_id` | character |  |
| `takes` | integer |  |
| `framing_runs` | double |  |
| `strikes_gained` | double |  |

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
| `catcher_id` | character |  |
| `attempts` | integer | Number of batted-ball events (attempts). |
| `cs_above_expected` | double |  |
| `throwing_runs` | double |  |

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
| `fielder_id` | character | Responsible fielder's MLBAM id (the fielder charged with the ball in play). |
| `position` | integer | Fielding position from Savant's `hit_location`, 1-9. |
| `direction` | character | Movement direction the catch required -- `in`, `back` or `lateral`. Present only when the call passes `by_direction=True`; the three rows re-group the same scored balls in play, so they sum exactly to the undirected `oaa`. |
| `opportunities` | integer | Balls in play charged to this fielder (the denominator of the OAA sum). |
| `oaa` | double | Outs above average -- the sum of (out - expected catch probability) over this fielder's opportunities. |

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
| `pitcher` | integer | MLB Advanced Media (MLBAM) id for the pitcher. |
| `game_pk` | integer | Game identifier. |
| `game_date` | date | Calendar date of the game (YYYY-MM-DD). |
| `injury_risk_index` | double | Equal-weighted composite of standardized adverse trailing features (higher = more risk). |

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
| `pitcher` | integer | MLB Advanced Media (MLBAM) id for the pitcher. |
| `season` | integer | MLB season (4-digit start year). |
| `x_woba` | double | Mean estimated_woba_using_speedangle allowed on batted balls. |
| `x_era` | double | Parametric xERA converted from x_woba via the season's league baselines. |
| `k_pct` | double | Strikeout rate (strikeouts / batters faced). |
| `bb_pct` | double | Walk rate (walks + HBP / batters faced). |
| `gb_pct` | double | Ground-ball rate among batted balls. |
| `siera_like` | double | SIERA-like ERA estimate from the fitted K%/BB%/GB% OLS coefficients. |

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
| `pitcher` | integer | MLB Advanced Media (MLBAM) id for the pitcher. |
| `game_pk` | integer | Game identifier. |
| `at_bat_number` | integer | Game-level plate-appearance sequence number. |
| `pitch_number` | integer | Pitch sequence number within the plate appearance. |
| `release_dist` | double | Euclidean distance between this pitch's release point and the previous pitch's release point. |
| `plate_dist` | double | Euclidean distance between this pitch's plate location and the previous pitch's plate location. |
| `tunnel_ratio` | double | plate_dist / max(release_dist, eps) -- higher means better tunneling (similar release, different result). |

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
| `prev_pitch_type` | character | Pitch type of the preceding pitch in the sequence. |
| `pitch_type` | character | Pitch type of the current pitch. |
| `mean_run_value` | double | Mean run value of the current pitch, grouped by the ordered (prev_pitch_type, pitch_type) pair. |
| `n` | integer | Number of pitches observed for this ordered pair. |

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
| `runner_id` | character |  |
| `attempts` | integer | Number of batted-ball events (attempts). |
| `p_success_mean` | double |  |
| `sb_run_value` | double |  |

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
| `pitcher` | integer | MLB Advanced Media (MLBAM) id for the pitcher. |
| `game_pk` | integer | Game identifier. |
| `at_bat_number` | integer | Game-level plate-appearance sequence number. |
| `pitch_number` | integer | Pitch sequence number within the plate appearance. |
| `times_through_order` | integer | Times through the batting order (1-3). |
| `fatigue_rv_adj` | double | Fitted per-TTO run-value marginal (mlb_pitching_constants.tto_penalty) for this pitch's TTO level. |

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

one row per umpire. | Column | Type | Description | |---|---|---| | umpire_id | Utf8 | Umpire identifier | | n_called | Int64 | Called pitches observed for this umpire | | obs_strike_rate | Float64 | Realized called-strike rate | | exp_strike_rate | Float64 | Mean model-predicted called-strike probability | | bias | Float64 | obs_strike_rate - exp_strike_rate (positive = strike-generous) |

| col_name | type | description |
|---|---|---|
| `umpire_id` | character | Umpire identifier (statsapi people id, stringified). |
| `n_called` | integer | Called pitches (strike or ball) observed for this umpire. |
| `obs_strike_rate` | double | Realized called-strike rate for this umpire. |
| `exp_strike_rate` | double | Mean model-predicted called-strike probability for this umpire's pitches. |
| `bias` | double | obs_strike_rate minus exp_strike_rate (positive = strike-generous). |

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

one row per input pitch. | Column | Type | Description | |---|---|---| | called_strike_prob | Float64 | P(called strike \| pitch location) |

| col_name | type | description |
|---|---|---|
| `called_strike_prob` | double | P(called strike \| pitch location) from the standardized zone-coordinate logistic. |

**Example**

```python
from sportsdataverse.mlb.mlb_umpire_zone import mlb_umpire_called_strike_prob
prob = mlb_umpire_called_strike_prob(pitches)
```

### pitch_features {#pitch_features}

`pitch_features(pitches: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Build the per-pitch feature substrate every pitching model consumes.

Standardizes physics (velocity/spin/movement/release/extension) within
`pitcher`, derives strike-zone-relative location features, pins id
columns to `Int64`, and passes Savant's per-pitch `delta_run_exp`
through unchanged as `run_value` (the single run-value label used by
Stuff+/Command+/TTO/tunneling).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw Savant pitch frame (e.g. from `sportsdataverse.mlb.mlb_statcast_search`), one row per pitch, carrying `pitcher`, `release_speed`, `release_spin_rate`, `pfx_x`, `pfx_z`, `release_pos_x`, `release_pos_z`, `release_extension`, `plate_x`, `plate_z`, `sz_top`, `sz_bot`, `delta_run_exp`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

One row per pitch with the input columns plus `velo_z`, `spin_z`, `pfx_x_z`, `pfx_z_z`, `release_pos_x_z`, `release_pos_z_z`, `extension_z` (standardized within `pitcher`), `plate_z_norm`, `plate_x_abs`, `in_zone`, `dist_from_heart`, and `run_value`. Empty/malformed input returns a zero-row frame carrying the added schema.

| col_name | type | description |
|---|---|---|
| `velo_z` | double | Release speed standardized (z-score) within pitcher. |
| `spin_z` | double | Release spin rate standardized (z-score) within pitcher. |
| `pfx_x_z` | double | Horizontal movement (pfx_x) standardized (z-score) within pitcher. |
| `pfx_z_z` | double | Vertical movement (pfx_z) standardized (z-score) within pitcher. |
| `release_pos_x_z` | double | Horizontal release position standardized (z-score) within pitcher. |
| `release_pos_z_z` | double | Vertical release position standardized (z-score) within pitcher. |
| `extension_z` | double | Release extension standardized (z-score) within pitcher. |
| `run_value` | double | Savant per-pitch delta_run_exp, passed through unchanged as the spine's single run-value label. |
| `plate_x_abs` | double | Absolute horizontal plate location (distance from the center of the zone). |
| `plate_z_norm` | double | Vertical plate location normalized to the batter's own strike zone, 0 = bottom, 1 = top. |
| `in_zone` | integer | 1 if the pitch crossed the strike zone (normalized location + horizontal bound), else 0. |
| `dist_from_heart` | double | Euclidean distance from the normalized zone center (0, 0.5) -- lower is more hittable. |

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
| `pitcher` | integer | MLB Advanced Media (MLBAM) id for the pitcher. |
| `game_pk` | integer | Game identifier. |
| `game_date` | date | Calendar date of the game (YYYY-MM-DD). |
| `fb_velo` | double | Mean fastball release speed for this appearance. |
| `velo_trend` | double | OLS slope of fb_velo over the trailing prior appearances (leakage-safe). |
| `velo_drop` | double | Trailing-baseline mean fb_velo minus this appearance's fb_velo. |
| `pitches_game` | integer | Pitches thrown in this appearance. |
| `trailing_workload` | double | Mean pitches_game over the trailing prior appearances (leakage-safe). |
| `days_rest` | double | Days since the pitcher's previous appearance. |

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
| `runner_id` | character |  |
| `base` | character |  |
| `p_success` | double |  |

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
| `game_date` | character | Game date (YYYY-MM-DD). |
| `runner_id` | character |  |
| `catcher_id` | character |  |
| `base` | character |  |
| `outcome` | character |  |

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
| `speed_b` | integer |  |
| `pop_b` | integer |  |
| `base` | character |  |
| `p_success` | double |  |
| `n` | integer |  |

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
| `pitcher` | integer | MLB Advanced Media (MLBAM) id for the pitcher. |
| `season` | integer | Season year (carried through for join convenience with x_era). |
| `k_pct` | double | Strikeout rate (strikeouts / batters faced). |
| `bb_pct` | double | Walk rate (walks + HBP / batters faced). |
| `gb_pct` | double | Ground-ball rate among batted balls. |
| `siera_like` | double | SIERA-like ERA estimate from the fitted K%/BB%/GB% OLS coefficients. |

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
| `times_through_order` | integer | Times through the batting order (1-3). |
| `mean_run_value` | double | Mean observed run value for pitches at this TTO level. |
| `penalty_vs_first` | double | mean_run_value minus the TTO=1 mean run value. |
| `n` | integer | Number of pitches observed at this TTO level. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
from sportsdataverse.mlb.mlb_pitch_fatigue import tto_penalty_table
feats = add_sequence_features(pitch_features(raw_pitches))
out = tto_penalty_table(feats)
print(out.sort("times_through_order"))
```
