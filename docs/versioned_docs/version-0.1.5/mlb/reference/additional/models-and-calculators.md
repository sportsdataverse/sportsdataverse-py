---
title: "MLB — additional Python functions — Models and calculators"
sidebar_label: "Models and calculators"
sidebar_position: 5
description: "MLB — additional Python functions — Models and calculators — function reference in sdv-py, the SportsDataverse Python package."
---
# MLB — additional Python functions — Models and calculators

### as_of_split {#as_of_split}

`as_of_split(events: "'pl.DataFrame'", cutoff_date: 'Any', *, date_col: 'str' = 'game_date') -> "'pl.DataFrame'"`

Leakage boundary: rows strictly before `cutoff_date` only.

The predictive path of the stolen-base (and, where predictive,
baserunning) model must derive runner/catcher features only from data
known **before** the event being scored -- this helper is the one place
that boundary is enforced, so every predictive caller shares it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `events` | `DataFrame` |  | Any frame carrying a date column. |
| `cutoff_date` | `Any` |  | Exclusive upper bound (rows with `date_col < cutoff_date` are kept). |
| `date_col` | `str` | `'game_date'` | Name of the date column. Defaults to `"game_date"`. |

**Returns**

the filtered frame (unchanged if empty or missing `date_col`).

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

**Example**

```python
from sportsdataverse.mlb.mlb_run_values import as_of_split
history = as_of_split(events, cutoff_date=dt.date(2024, 6, 15))
```

### build_we_table {#build_we_table}

`build_we_table(states: 'pl.DataFrame', results: 'pl.DataFrame', *, laplace: 'float' = 1.0) -> 'pl.DataFrame'`

Empirical, Laplace-smoothed home win-expectancy table.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `states` | `DataFrame` |  | Output of `pbp_base_out_states`. |
| `results` | `DataFrame` |  | Game-level results with `game_id` (same dtype as `states`), `home_score`, `away_score`. |
| `laplace` | `float` | `1.0` | Additive smoothing constant (default 1.0). |

**Returns**

one row per observed state bucket. | Column | Type | Description | |---|---|---| | inning_capped | Int64 | Inning, capped at 9 | | half | Utf8 | `"top"` or `"bottom"` | | base_state | Utf8 | 3-char base occupancy | | outs_start | Int64 | Outs before the play (0-2) | | score_diff_bucket | Int64 | home - away score, clipped to [-6, 6] | | home_win_exp | Float64 | Laplace-smoothed P(home wins \| state) | | n | Int64 | Plate appearances observed in this bucket |

| col_name | type | description |
|---|---|---|
| `inning_capped` | integer | Inning number, capped at 9 (extra innings pooled with the 9th). |
| `half` | character | Half-inning ("top" or "bottom"). |
| `base_state` | character | 3-char base occupancy code. |
| `outs_start` | integer | Outs before the play (0-2). |
| `score_diff_bucket` | integer | home minus away score, clipped to [-6, 6]. |
| `home_win_exp` | double | Laplace-smoothed empirical P(home team wins \| state bucket). |
| `n` | integer | Plate appearances observed in this state bucket. |

**Example**

```python
from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states
from sportsdataverse.mlb.mlb_win_expectancy import build_we_table
states = pbp_base_out_states(pbp)
table = build_we_table(states, results)
```

### count_strike_run_value {#count_strike_run_value}

`count_strike_run_value(pitches: "'pl.DataFrame'") -> "'pl.DataFrame'"`

Ball-to-strike run-expectancy delta per count, from `delta_run_exp`.

`strike_run_value` is positive = runs **saved by the defense** per
stolen strike, since a called strike carries negative `delta_run_exp`
for the batting team relative to a ball in the same count:
`strike_run_value = -(E[delta_run_exp | called_strike, count] -
E[delta_run_exp | ball, count])`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Pitch-level frame with `balls`, `strikes`, `description`, and `delta_run_exp` columns (a `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search` frame). Rows other than `called_strike`/`ball` are ignored. |

**Returns**

one row per observed count. | Column | Type | Description | |---|---|---| | balls | Int64 | Ball count (0-3) entering the pitch | | strikes | Int64 | Strike count (0-2) entering the pitch | | strike_run_value | Float64 | Runs saved by the defense per called strike vs. a ball in this count |

| col_name | type | description |
|---|---|---|
| `balls` | integer | Ball count before the pitch. |
| `strikes` | integer | Strike count before the pitch. |
| `strike_run_value` | double |  |

**Example**

```python
from sportsdataverse.mlb.mlb_run_values import count_strike_run_value
rv = count_strike_run_value(pitches)
```

### event_run_value {#event_run_value}

`event_run_value(pitches: "'pl.DataFrame'", events: "'List[str]'") -> 'float'`

Empirical run value of an event set, from mean `delta_run_exp`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Pitch-level frame with an `events` column and `delta_run_exp`. |
| `events` | `List[str]` |  | Statcast `events` values to average over (e.g. `["stolen_base_2b"]`). |

**Returns**

Mean `delta_run_exp` over rows whose `events` is in `events`. `0.0` if the frame is empty, lacks `delta_run_exp`, or no rows match.

**Example**

```python
from sportsdataverse.mlb.mlb_run_values import event_run_value
rv_sb = event_run_value(pitches, ["stolen_base_2b", "stolen_base_3b"])
```

### mae {#mae}

`mae(a: 'np.ndarray', b: 'np.ndarray') -> 'float'`

Mean absolute error between two arrays.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `ndarray` |  | First array of values. |
| `b` | `ndarray` |  | Second array of values (same length as `a`). |

**Returns**

The mean absolute error.

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import mae
mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
```

### mlb_batter_projection {#mlb_batter_projection}

`mlb_batter_projection(target_season: 'int', *, history: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Next-season xwOBA projection (Marcel + delta-method aging) for every batter.

If `history` is `None`, builds player-season xwOBA history via
`sportsdataverse.mlb.mlb_expected_stats.mlb_expected_stats` across
the three seasons before `target_season` (ages must already be present
on a supplied `history` frame -- this convenience path is intended for
callers who already maintain an age-joined roster history).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_season` | `int` |  | The season being projected. |
| `history` | `Optional[DataFrame]` | `None` | Pre-built player-season history (`batter`, `season`, `age`, `xwoba`, `pa`). If `None`, uses `mlb_expected_stats` over `target_season - 3 .. target_season - 1`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `batter`: `age`, `proj_xwoba`, `proj_pa`. Empty history returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `batter` | integer | MLBAM batter id. |
| `age` | integer | Batter's projected age in target_season (last known age plus one). |
| `proj_xwoba` | double | Marcel-style weighted, PA-regressed, aging-curve-adjusted xwOBA projection for target_season. |
| `proj_pa` | double | Sum of plate appearances across the weighted lookback seasons used to build the projection. |

**Example**

```python
from sportsdataverse.mlb.mlb_batter_projection import mlb_batter_projection

proj = mlb_batter_projection(2024, history=player_season_history)
print(proj.shape)

# Pipeline next step (one line)

proj.sort("proj_xwoba", descending=True).head()
```

### mlb_command_plus {#mlb_command_plus}

`mlb_command_plus(pitches: 'pl.DataFrame', *, level: 'str' = 'pitch', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Score pitches with the bundled Command+/Location+ (②) run-value model.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.pitch_features` (needs `plate_x_abs`, `plate_z_norm`, `in_zone`, `dist_from_heart`, `balls`, `strikes`, `stand`, `p_throws`, `pitch_type`). |
| `level` | `str` | `'pitch'` | `"pitch"` (default) for per-pitch output, or `"pitcher"` for a per-pitcher mean. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`level="pitch"`: `pitcher`, `pitch_type`, `location_rv_hat`, `command_plus`. `level="pitcher"`: `pitcher`, `location_rv_hat`, `command_plus` (per-pitcher mean). Empty input returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLB Advanced Media (MLBAM) id for the pitcher. |
| `pitch_type` | character | Statcast pitch-type abbreviation. |
| `location_rv_hat` | double | Predicted per-pitch run value from the bundled Command+/Location+ xgboost model (location + count/handedness/pitch-type features only). |
| `command_plus` | double | Plus-scale Command+/Location+ score, 100 = league average, higher = better. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features
from sportsdataverse.mlb.mlb_command_plus import mlb_command_plus
feats = pitch_features(raw_pitches)
out = mlb_command_plus(feats)
print(out.select("pitcher", "command_plus").head())

# Pipeline next step

out.group_by("pitcher").agg(pl.col("command_plus").mean()).sort("command_plus", descending=True)
```

### mlb_expected_home_runs {#mlb_expected_home_runs}

`mlb_expected_home_runs(start_dt: 'str', end_dt: 'str', *, puller: 'Optional[Callable[..., pl.DataFrame]]' = None, park_factors: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Per player-season park-neutral xHR, park-adjusted xHR, and HR-above-expected.

Pulls batted balls via `puller(start_dt, end_dt, player_type="batter")`,
builds the EV x LA x spray HR-probability grid from the pull's own batted
balls (season-agnostic algorithm, per-pull empirical constants), predicts
each ball's HR probability with the EV x LA-marginal fallback, park-adjusts
via `hr_factor` (index 100 = neutral, joined on the Statcast
`home_team` abbreviation -> MLBAM team id), then aggregates per batter.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  | Pull start date, `YYYY-MM-DD`. |
| `end_dt` | `str` |  | Pull end date, `YYYY-MM-DD`. |
| `puller` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable Statcast search callable -- defaults to `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`. |
| `park_factors` | `Optional[DataFrame]` | `None` | Pre-fetched park-factors frame (`team_id`, `hr_factor`); if `None`, fetched via `sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_park_factors`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (`batter`, `season`): `hr`, `xhr_neutral`, `xhr_park_adj`, `hr_above_expected` (`hr - xhr_neutral`). Empty pull returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `batter` | integer | MLBAM batter id (join key into Savant's home-runs leaderboard as `player_id`). |
| `season` | integer | Four-digit season year derived from `game_year`/`game_date`. |
| `hr` | integer | Realized home runs in the pulled window. |
| `xhr_neutral` | double | Park-neutral expected home runs from the EV x LA x spray probability grid. |
| `xhr_park_adj` | double | Park-adjusted expected home runs (xhr_neutral cell probabilities scaled by each batted ball's home-park HR factor / 100). |
| `hr_above_expected` | double | hr minus xhr_neutral -- positive means the batter over-performed the park-neutral HR model. |

**Example**

```python
from sportsdataverse.mlb.mlb_expected_home_runs import mlb_expected_home_runs

df = mlb_expected_home_runs("2024-06-01", "2024-06-21")
print(df.shape)

# Pipeline next step (one line)

df.sort("hr_above_expected", descending=True).head()
```

### mlb_expected_stats {#mlb_expected_stats}

`mlb_expected_stats(start_dt: 'str', end_dt: 'str', *, puller: 'Optional[Callable[..., pl.DataFrame]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Per player-season xwOBA/xBA/xSLG from an on-the-fly EV x LA empirical grid.

Pulls pitches via `puller(start_dt, end_dt, player_type="batter")`, builds
the outcome grid from the pull's own batted balls (season-agnostic
algorithm, per-pull empirical constants -- see `CLAUDE.md`), predicts
contact `woba`/`ba`/`slg` per batted ball with the launch-angle-
marginal fallback, then aggregates:

* `xwoba = (sum(predicted_woba over balls in play) + sum(woba_value over
  non-batted-ball PA-ENDING outcomes)) / derived_woba_denom` -- the
  denominator is DERIVED from `events` (PA enders minus intentional
  walks / sac bunts / catcher interference), never trusted from a cache
  vintage's `woba_denom` column. The numerator excludes those same
  zero-denominator events, and a PA-ending walk/HBP whose `woba_value`
  is null in a given vintage is filled with the fixed weights .69 / .72.
* `xba = (sum(predicted_ba over TRACKED at-bat balls in play) +
  sum(realized hits over UNTRACKED ones)) / ab` -- a ball in play with no
  launch data cannot be predicted from the grid, so it takes its realized
  outcome exactly as `xwoba` does, rather than counting in `ab` with a
  zero numerator (which deflated league-mean xBA by the untracked share).
* `xslg` -- same construction on total bases.
* `woba` / `ba` -- the OBSERVED counterparts on the same denominators,
  so `xwoba - woba` is a luck-vs-skill delta needing no second source.

`pa` counts PLATE-APPEARANCE-ENDING rows only (`events` non-null),
never raw pitches -- a Statcast search pull carries every pitch, and
counting them (the pre-fix behavior) inflated `pa`/`ab` by ~4x and
corrupted `xba`/`xslg` scales.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  | Pull start date, `YYYY-MM-DD`. |
| `end_dt` | `str` |  | Pull end date, `YYYY-MM-DD`. |
| `puller` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable Statcast search callable -- defaults to `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (`batter`, `season`): `pa`, `ab`, `xwoba`, `xba`, `xslg`, plus the observed `woba` / `ba`. Empty pull returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `batter` | integer | MLBAM batter id (join key into Savant's expected-stats leaderboard as `player_id`). |
| `season` | integer | Four-digit season year derived from `game_year`/`game_date`. |
| `pa` | integer | Plate appearances in the pulled window. |
| `ab` | integer | At-bats (PA minus walks, HBP, and sacrifices) in the pulled window. |
| `xwoba` | double | Expected wOBA from the EV x LA empirical grid (contact) plus realized non-contact outcome value, divided by wOBA denominator. |
| `xba` | double | Expected batting average from the EV x LA empirical grid's hit-indicator cell means on tracked balls in play, plus the realized hit indicator on balls in play carrying no launch data, over at-bats. |
| `xslg` | double | Expected slugging percentage from the EV x LA empirical grid's total-bases cell means on tracked balls in play, plus realized total bases on balls in play carrying no launch data, over at-bats. |
| `woba` | double | Observed wOBA over the same denominator as `xwoba`, so `xwoba - woba` is the batter's contact-quality luck gap without needing a second source. |
| `ba` | double | Observed batting average over the same at-bat denominator as `xba`, so `xba - ba` is the batted-ball luck gap without needing a second source. |

**Example**

```python
from sportsdataverse.mlb.mlb_expected_stats import mlb_expected_stats

df = mlb_expected_stats("2024-06-01", "2024-06-21")
print(df.shape)

# Pipeline next step (one line)

df.sort("xwoba", descending=True).head()
```

### mlb_pitch_classify {#mlb_pitch_classify}

`mlb_pitch_classify(pitches: 'pl.DataFrame', *, max_components: 'int' = 6, seed: 'int' = 0, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-pitcher Gaussian-mixture pitch reclassification.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.pitch_features` (needs `velo_z`, `spin_z`, `pfx_x_z`, `pfx_z_z`). |
| `max_components` | `int` | `6` | Cap on GMM components considered per pitcher (BIC picks the best `1..min(max_components, n_pitch_types)`). |
| `seed` | `int` | `0` | Random seed for reproducible cluster labels. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `pitch_type`, `pitch_type_reclass`, `reclass_confidence` (max posterior cluster responsibility). Pitchers with fewer than `MIN_PITCHES_FOR_CLUSTERING` pitches pass through the Savant label unchanged with `reclass_confidence = 1.0`. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLB Advanced Media (MLBAM) id for the pitcher. |
| `pitch_type` | character | Savant-reported pitch-type abbreviation. |
| `pitch_type_reclass` | character | Reclassified pitch-type label from the per-pitcher GMM clustering (may differ from pitch_type). |
| `reclass_confidence` | double | Max posterior cluster responsibility (1.0 for low-volume pitchers passed through unchanged). |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features
from sportsdataverse.mlb.mlb_pitch_classify import mlb_pitch_classify
feats = pitch_features(raw_pitches)
out = mlb_pitch_classify(feats, seed=0)
print(out.filter(out["pitch_type"] != out["pitch_type_reclass"]).head())
```

### mlb_prop_strikeouts {#mlb_prop_strikeouts}

`mlb_prop_strikeouts(team_k9: 'float', opp_k_rate: 'float', lg_k_rate: 'float', *, innings: 'float' = 9.0) -> 'float'`

Expected pitcher/team strikeouts via a K/9-and-opponent-K-rate blend.

`team_k9 / 9 * innings * (opp_k_rate / lg_k_rate)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_k9` | `float` |  | Team/pitcher strikeouts per 9 innings pitched. |
| `opp_k_rate` | `float` |  | Opponent's own strikeout rate (K per PA). |
| `lg_k_rate` | `float` |  | League-average strikeout rate. |
| `innings` | `float` | `9.0` | Innings pitched in this outing (default 9.0). |

**Returns**

expected strikeouts.

**Example**

```python
from sportsdataverse.mlb.mlb_prop_projection import mlb_prop_strikeouts
mlb_prop_strikeouts(9.0, 0.22, 0.22)
```

### mlb_prop_team_runs {#mlb_prop_team_runs}

`mlb_prop_team_runs(home_off: 'float', away_def: 'float', lg_rpg: 'float', *, park_factor: 'float' = 1.0) -> 'float'`

Expected team runs via a log5-style rate blend.

`lg_rpg * (home_off / lg_rpg) * (away_def / lg_rpg) * park_factor`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_off` | `float` |  | Team's own runs-scored-per-game rate. |
| `away_def` | `float` |  | Opponent's runs-allowed-per-game rate. |
| `lg_rpg` | `float` |  | League-average runs-per-game rate. |
| `park_factor` | `float` | `1.0` | Park run-scoring multiplier (default neutral 1.0; a real park-factor table is a documented follow-on). |

**Returns**

expected runs for the team in this matchup.

**Example**

```python
from sportsdataverse.mlb.mlb_prop_projection import mlb_prop_team_runs
mlb_prop_team_runs(5.5, 5.0, 4.5)
```

### mlb_props {#mlb_props}

`mlb_props(matchups: 'pl.DataFrame', ratings: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Expected team runs + strikeouts for a slate of matchups.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `matchups` | `DataFrame` |  | One row per game: `game_id`, `home_team_id`, `away_team_id`. |
| `ratings` | `DataFrame` |  | Per-team as-of-date rate table: `team_id`, `off_rpg` (runs scored/game), `def_rpg` (runs allowed/game), and optionally `k9` + `k_rate` (see the module docstring -- strikeout columns are null without them). `team_id` must share a dtype with `matchups`' team-id columns. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per matchup. | Column | Type | Description | |---|---|---| | game_id | Utf8 | Game identifier | | home_team_id | Utf8 | Home team identifier | | away_team_id | Utf8 | Away team identifier | | exp_runs_home | Float64 | Expected home-team runs | | exp_runs_away | Float64 | Expected away-team runs | | exp_strikeouts_home | Float64 | Expected home-pitcher strikeouts (null if `ratings` lacks k9/k_rate) | | exp_strikeouts_away | Float64 | Expected away-pitcher strikeouts (null if `ratings` lacks k9/k_rate) |

| col_name | type | description |
|---|---|---|
| `game_id` | character | Game identifier. |
| `home_team_id` | character | Home team identifier. |
| `away_team_id` | character | Away team identifier. |
| `exp_runs_home` | double | Expected home-team runs (log5-style rate blend). |
| `exp_runs_away` | double | Expected away-team runs (log5-style rate blend). |
| `exp_strikeouts_home` | double | Expected home-pitcher strikeouts (null when the ratings input lacks k9/k_rate). |
| `exp_strikeouts_away` | double | Expected away-pitcher strikeouts (null when the ratings input lacks k9/k_rate). |

**Example**

```python
from sportsdataverse.mlb.mlb_prop_projection import mlb_props
props = mlb_props(matchups, ratings)
```

### mlb_pythagenpat {#mlb_pythagenpat}

`mlb_pythagenpat(runs_scored: 'float', runs_allowed: 'float', games: 'int', *, exponent: 'float' = 0.287) -> 'float'`

Pythagenpat expected win percentage (Smyth-Patriot, run-environment adaptive exponent).

`x = ((runs_scored + runs_allowed) / games) ** exponent`;
`win_pct = runs_scored**x / (runs_scored**x + runs_allowed**x)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `runs_scored` | `float` |  | Total runs scored. |
| `runs_allowed` | `float` |  | Total runs allowed. |
| `games` | `int` |  | Games played. |
| `exponent` | `float` | `0.287` | Run-environment exponent (default the published 0.287). |

**Returns**

expected win percentage in `[0, 1]`. Returns `0.5` when `games == 0` or `runs_scored + runs_allowed == 0` (guard against a zero-division/degenerate input).

**Example**

```python
from sportsdataverse.mlb.mlb_team_projection import mlb_pythagenpat
mlb_pythagenpat(800, 600, 162)
```

### mlb_pythagenpat_table {#mlb_pythagenpat_table}

`mlb_pythagenpat_table(results: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Per-(season, team) pythagenpat table from game-level results.

This is a **same-window estimator**, not a forward-looking prediction:
pythagenpat smooths a team's *already-known* run differential into an
implied "true-talent" win rate over that same window (the classic
Bill James validation is exactly "does the formula's win% track the
actual win% over the same season"). To use it predictively for a
future game, pre-filter `results` to games strictly before that date
with `sportsdataverse.mlb.mlb_game_state_constants.as_of_split`
first -- this function does not do that filtering itself.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | Game-level results (`season`, `home_team_id`, `away_team_id`, `home_score`, `away_score`). |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per (season, team). | Column | Type | Description | |---|---|---| | season | Int64 | Season | | team_id | Utf8 | Team identifier | | runs_scored | Int64 | Total runs scored | | runs_allowed | Int64 | Total runs allowed | | games | Int64 | Games played | | win_pct | Float64 | Realized win percentage | | pythag_win_pct | Float64 | Pythagenpat expected win percentage |

| col_name | type | description |
|---|---|---|
| `season` | integer | MLB season (4-digit start year). |
| `team_id` | character | Team identifier (statsapi team id, stringified). |
| `runs_scored` | integer | Total runs scored across the covered games. |
| `runs_allowed` | integer | Total runs allowed across the covered games. |
| `games` | integer | Count of games played in the season. |
| `win_pct` | double | Realized win percentage. |
| `pythag_win_pct` | double | Pythagenpat expected win percentage (exponent 0.287). |

**Example**

```python
from sportsdataverse.mlb.mlb_team_projection import mlb_pythagenpat_table
table = mlb_pythagenpat_table(results)
```

### mlb_run_expectancy_matrix {#mlb_run_expectancy_matrix}

`mlb_run_expectancy_matrix(seasons: 'Union[int, List[int], None]' = None, *, pbp: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Empirical RE24 run-expectancy matrix by base-out state.

`re[base_state, outs] = mean(runs_rest_of_inning)` over all plate
appearances starting in that state, excluding the bottom of the 9th
inning and beyond (the standard RE24 exclusion -- those half-innings
are only played while the home team trails or is tied, a
score-differential selection bias that would otherwise distort the
matrix). Computed on demand from statsapi play-by-play; **no bundled
artifact**.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int], None]` | `None` | One season (int) or a list of seasons to collect via `sportsdataverse.mlb.mlb_api_extra.mlb_schedule`. Ignored when `pbp` is supplied. |
| `pbp` | `Optional[DataFrame]` | `None` | Pre-collected parsed play-by-play frame (skips the network collector -- primarily for tests / offline reuse). |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

up to 24 rows (base_state x outs). | Column | Type | Description | |---|---|---| | base_state | Utf8 | 3-char base occupancy (e.g. `"1_3"`) | | outs | Int64 | Outs at the start of the state (0-2) | | re | Float64 | Mean runs scored through the end of the half-inning | | n | Int64 | Number of plate appearances observed in this state |

| col_name | type | description |
|---|---|---|
| `base_state` | character | 3-char base occupancy code ("_" = empty, "1"/"2"/"3" = occupied), e.g. "1_3" for runners on first and third. |
| `outs` | integer | Outs at the start of the base-out state (0-2). |
| `re` | double | Empirical mean runs scored from this state through the end of the half-inning (RE24). |
| `n` | integer | Number of plate appearances observed starting in this base-out state. |

**Example**

```python
from sportsdataverse.mlb.mlb_run_expectancy import mlb_run_expectancy_matrix
matrix = mlb_run_expectancy_matrix(pbp=pbp)

# Pipeline next step (one line)

matrix.filter(pl.col("base_state") == "___").sort("outs")
```

### mlb_stuff_plus {#mlb_stuff_plus}

`mlb_stuff_plus(pitches: 'pl.DataFrame', *, level: 'str' = 'pitch', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Score pitches with the bundled Stuff+ (①) run-value model.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.pitch_features` (needs `velo_z`, `spin_z`, `pfx_x_z`, `pfx_z_z`, `release_pos_x_z`, `release_pos_z_z`, `extension_z`). |
| `level` | `str` | `'pitch'` | `"pitch"` (default) for per-pitch output, or `"arsenal"` for a per `(pitcher, pitch_type)` mean. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `pitch_type`, `stuff_rv_hat`, `stuff_plus` — one row per pitch (`level="pitch"`) or per pitcher-pitchtype (`level="arsenal"`). Empty input returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLB Advanced Media (MLBAM) id for the pitcher. |
| `pitch_type` | character | Statcast pitch-type abbreviation. |
| `stuff_rv_hat` | double | Predicted per-pitch run value from the bundled Stuff+ xgboost model (physics + fastball-relative features only). |
| `stuff_plus` | double | Plus-scale Stuff+ score, 100 = league average, higher = better (sign-inverted from stuff_rv_hat). |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features
from sportsdataverse.mlb.mlb_stuff_plus import mlb_stuff_plus
feats = pitch_features(raw_pitches)
out = mlb_stuff_plus(feats, level="arsenal")
print(out.sort("stuff_plus", descending=True).head())

# Pipeline next step

out.filter(pl.col("pitch_type") == "FF").sort("stuff_plus", descending=True)
```

### mlb_swing_decision {#mlb_swing_decision}

`mlb_swing_decision(start_dt: 'str', end_dt: 'str', *, puller: 'Optional[Callable[..., pl.DataFrame]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Per player-season swing/take run value + selective-aggression (SEAGER analog).

Pulls pitches via `puller(start_dt, end_dt, player_type="batter")`,
builds the RV(swing)/RV(take) zone x count surfaces (and the league
swing-rate table) from the pull itself, then per batter:

* `swing_take_runs` = sum of the **actual** per-pitch `delta_run_exp`
  credited to the batter's swing/take decisions (matching Savant's
  swing/take run-value definition -- the run value of what actually
  happened on each pitch, not a league-average lookup).
* `selective_agg` = sum of `rv_chosen - rv_neutral`, where
  `rv_neutral = swing_rate * rv_swing + (1 - swing_rate) * rv_take` uses
  the **league** swing rate for that zone x count cell -- positive means
  the batter swings at hittable pitches and takes bad ones more than a
  league-average decision-maker would.
* `chase_rate` = swings / pitches seen in the waste/chase zones
  (`zone in {11,12,13,14}`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  | Pull start date, `YYYY-MM-DD`. |
| `end_dt` | `str` |  | Pull end date, `YYYY-MM-DD`. |
| `puller` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable Statcast search callable -- defaults to `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (`batter`, `season`): `pitches`, `swing_take_runs`, `selective_agg`, `chase_rate`, `n_swings`. Empty pull returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `batter` | integer | MLBAM batter id (join key into Savant's swing/take leaderboard as `player_id`). |
| `season` | integer | Four-digit season year derived from `game_year`/`game_date`. |
| `pitches` | integer | Total pitches seen with a non-null zone/decision in the pulled window. |
| `swing_take_runs` | double | Sum of the run value of the batter's actual swing/take decisions (delta_run_exp of the chosen decision at that zone x count). |
| `selective_agg` | double | SEAGER-analog selective-aggression score -- sum of (chosen run value minus the league-neutral-rate run value) per pitch. |
| `chase_rate` | double | Share of pitches in the waste/chase attack zones (11-14) that the batter swung at. |
| `n_swings` | integer | Count of pitches the batter swung at. |

**Example**

```python
from sportsdataverse.mlb.mlb_swing_decision import mlb_swing_decision

df = mlb_swing_decision("2024-06-01", "2024-06-21")
print(df.shape)

# Pipeline next step (one line)

df.sort("selective_agg", descending=True).head()
```

### mlb_team_elo {#mlb_team_elo}

`mlb_team_elo(results: 'pl.DataFrame', *, k: 'float' = 4.0, hfa: 'float' = 24.0, init: 'float' = 1500.0, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

As-of-date iterative Elo run-differential rating.

Games are folded **in date order** (ties broken by `game_id`); each
team's rating updates only *after* its game is scored, so the
`home_rating`/`away_rating` columns are strictly as-of-date (no
leakage from later games). `home_win_prob_elo` uses the standard
logistic Elo formula with a home-field-advantage offset.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | Game-level results (`game_id`, `date`, `home_team_id`, `away_team_id`, `home_score`, `away_score`). |
| `k` | `float` | `4.0` | Elo K-factor (rating-update step size). |
| `hfa` | `float` | `24.0` | Home-field-advantage Elo-point offset. |
| `init` | `float` | `1500.0` | Initial rating for a team with no prior games. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per game, in date order. | Column | Type | Description | |---|---|---| | game_id | Utf8 | Game identifier | | date | Date | Game date | | home_team_id | Utf8 | Home team identifier | | away_team_id | Utf8 | Away team identifier | | home_rating | Float64 | Home team's rating **before** this game | | away_rating | Float64 | Away team's rating **before** this game | | home_win_prob_elo | Float64 | Elo-implied P(home wins) before this game | | home_rating_post | Float64 | Home team's rating **after** this game | | away_rating_post | Float64 | Away team's rating **after** this game |

| col_name | type | description |
|---|---|---|
| `game_id` | character | Game identifier (statsapi gamePk, stringified). |
| `date` | date | Calendar date of the game (YYYY-MM-DD). |
| `home_team_id` | character | Home team identifier. |
| `away_team_id` | character | Away team identifier. |
| `home_rating` | double | Home team's Elo rating before this game (as-of-date). |
| `away_rating` | double | Away team's Elo rating before this game (as-of-date). |
| `home_win_prob_elo` | double | Elo-implied P(home team wins) before this game. |
| `home_rating_post` | double | Home team's Elo rating after this game. |
| `away_rating_post` | double | Away team's Elo rating after this game. |

**Example**

```python
from sportsdataverse.mlb.mlb_team_projection import mlb_team_elo
elo = mlb_team_elo(results)

# Pipeline next step (one line)

elo.group_by("home_team_id").agg(pl.col("home_rating_post").last())
```

### mlb_team_projection {#mlb_team_projection}

`mlb_team_projection(seasons: 'Union[int, List[int], None]' = None, *, results: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Combined pythagenpat + Elo team projection.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int], None]` | `None` | Reserved for a future network-collector path (currently unused -- pass `results` directly; see `sportsdataverse.mlb.mlb_run_expectancy.mlb_run_expectancy_matrix` for the collector pattern this will follow once wired). |
| `results` | `Optional[DataFrame]` | `None` | Game-level results (see `mlb_pythagenpat_table` and `mlb_team_elo` for the required columns). |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per (season, team). | Column | Type | Description | |---|---|---| | season | Int64 | Season | | team_id | Utf8 | Team identifier | | win_pct | Float64 | Realized win percentage | | pythag_win_pct | Float64 | Pythagenpat expected win percentage | | rating | Float64 | Final (as of the last observed game) Elo rating | | exp_margin | Float64 | Elo-implied expected run margin vs a league-average opponent |

| col_name | type | description |
|---|---|---|
| `season` | integer | MLB season (4-digit start year). |
| `team_id` | character | Team identifier (statsapi team id, stringified). |
| `win_pct` | double | Realized win percentage. |
| `pythag_win_pct` | double | Pythagenpat expected win percentage. |
| `rating` | double | Final (as of the last observed game) Elo rating. |
| `exp_margin` | double | Elo-implied expected run margin vs a league-average opponent. |

**Example**

```python
from sportsdataverse.mlb.mlb_team_projection import mlb_team_projection
projection = mlb_team_projection(results=results)
```

### mlb_win_expectancy {#mlb_win_expectancy}

`mlb_win_expectancy(pbp: 'pl.DataFrame', results: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Per-play home win expectancy from the empirical state table.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Parsed `mlb_play_by_play` frame (see `sportsdataverse.mlb.mlb_run_expectancy.pbp_base_out_states`). |
| `results` | `DataFrame` |  | Game-level results (`game_id`, `home_score`, `away_score`). |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per plate appearance, **plus one terminal "game over" row per game** (`at_bat_index` = last real PA's index + 1, `home_win_exp` pinned to the actual final outcome: 1.0 if home won, 0.0 otherwise). Without this anchor, the last real play's own WPA swing (e.g. a walk-off) would never be captured by `mlb_win_probability_added`'s per-game diff, and the game-level WPA sum would not telescope to the exact +-0.5 identity. | Column | Type | Description | |---|---|---| | game_id | Utf8 | Game identifier | | at_bat_index | Int64 | Game-global sequential PA index (last row is a synthetic terminal marker) | | half | Utf8 | `"top"` or `"bottom"` (offense side); the terminal row repeats the last real half | | home_win_exp | Float64 | P(home team wins \| state before the play); 1.0/0.0 on the terminal row |

| col_name | type | description |
|---|---|---|
| `game_id` | character | Game identifier (statsapi gamePk, stringified). |
| `at_bat_index` | integer | Game-global sequential plate-appearance index. |
| `half` | character | Half-inning ("top" or "bottom") -- which side is on offense. |
| `home_win_exp` | double | Empirical P(home team wins \| base-out-score-inning state before the play). |

**Example**

```python
from sportsdataverse.mlb.mlb_win_expectancy import mlb_win_expectancy
we = mlb_win_expectancy(pbp, results)

# Pipeline next step (one line)

we.filter(pl.col("game_id") == "716390").sort("at_bat_index")
```

### mlb_win_probability_added {#mlb_win_probability_added}

`mlb_win_probability_added(we: 'pl.DataFrame', *, perspective: 'str' = 'home', return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Per-play win-probability-added from a `mlb_win_expectancy` frame.

`wpa_i = home_win_exp_i - home_win_exp_{i-1}` within each game (the
first play of a game is measured against the neutral 0.5 baseline).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `we` | `DataFrame` |  | Output of `mlb_win_expectancy` (needs `game_id`, `at_bat_index`, `home_win_exp`). |
| `perspective` | `str` | `'home'` | `"home"` (default) returns home-team WPA; any other value (e.g. `"away"`) returns the sign-flipped (away-team) WPA. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per plate appearance. | Column | Type | Description | |---|---|---| | game_id | Utf8 | Game identifier | | at_bat_index | Int64 | Game-global sequential PA index | | wpa | Float64 | Win-probability added, from `perspective` |

| col_name | type | description |
|---|---|---|
| `game_id` | character | Game identifier (statsapi gamePk, stringified). |
| `at_bat_index` | integer | Game-global sequential plate-appearance index. |
| `wpa` | double | Win-probability added on this play, from the requested perspective. |

**Example**

```python
from sportsdataverse.mlb.mlb_win_expectancy import mlb_win_probability_added
wpa = mlb_win_probability_added(we)
```

### pbp_base_out_states {#pbp_base_out_states}

`pbp_base_out_states(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Reconstruct pre-play base-out state from statsapi play-by-play.

Within each `(game_id, inning, half)` half-inning, ordered by the
game-global `at_bat_index`: `base_state`/`outs_start` before PA
*i* are the post-occupancy / out-count of PA *i-1* (empty/0 at the
half's first PA -- occupancy and outs both genuinely reset at every
half-inning boundary). `runs_on_play` is the score delta since the
*previous PA in the game* (`over("game_id")`, **not** reset per
half-inning -- the score itself carries across the half-inning
boundary even though outs/bases do not).  `runs_rest_of_inning` is
the suffix-sum of `runs_on_play` within the half.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Parsed `mlb_play_by_play` frame (optionally concatenated across games), carrying `game_id`, `about_inning`, `about_half_inning`, `about_at_bat_index`, `count_outs`, `result_home_score`, `result_away_score`, `matchup_post_on_{first,second,third}_id`. |

**Returns**

one row per plate appearance. | Column | Type | Description | |---|---|---| | game_id | Utf8 | Game identifier | | inning | Int64 | Inning number | | half | Utf8 | `"top"` or `"bottom"` | | at_bat_index | Int64 | Game-global sequential PA index | | base_state | Utf8 | 3-char occupancy before the PA (`"1_3"` etc.) | | outs_start | Int64 | Outs before the PA (0-2) | | runs_on_play | Int64 | Runs scored on this PA | | runs_rest_of_inning | Int64 | Runs scored from this PA through the half's end | | score_diff | Int64 | home - away score at the start of the PA |

No returns table is published for this function: no capture: its play-by-play input comes from statsapi.mlb.com, which answers HTTP 406 to the datacenter IP the docs are built on, and load_mlb_pbp lacks its game_id / about_* columns.

**Example**

```python
from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states
states = pbp_base_out_states(pbp)
```

### pearson_corr {#pearson_corr}

`pearson_corr(a: "'np.ndarray'", b: "'np.ndarray'") -> 'float'`

Pearson correlation coefficient between two 1-D arrays.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `ndarray` |  | First sample array. |
| `b` | `ndarray` |  | Second sample array, same length as `a`. |

**Returns**

Pearson's r. `nan` if either input has zero variance.

**Example**

```python
from sportsdataverse.mlb.mlb_run_values import pearson_corr
r = pearson_corr(mine["framing_runs"].to_numpy(), sav["runs_extra_strikes"].to_numpy())
```

### prop_over_prob {#prop_over_prob}

`prop_over_prob(line: 'float', expected: 'float') -> 'float'`

P(realized count > line) under a Poisson(expected) model.

`1 - poisson.cdf(floor(line), expected)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `line` | `float` |  | The prop betting line (e.g. 8.5 runs). |
| `expected` | `float` |  | The Poisson mean (expected runs/strikeouts/etc.). |

**Returns**

P(over), in `[0, 1]`.

**Example**

```python
from sportsdataverse.mlb.mlb_prop_projection import prop_over_prob
prop_over_prob(3.5, 4.5)
```

### spearman_corr {#spearman_corr}

`spearman_corr(a: 'np.ndarray', b: 'np.ndarray') -> 'float'`

Spearman rank correlation between two arrays.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `ndarray` |  | First array of values. |
| `b` | `ndarray` |  | Second array of values (same length as `a`). |

**Returns**

The Spearman rank correlation coefficient.

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import spearman_corr
spearman_corr(np.array([1, 2, 3]), np.array([3, 1, 2]))
```
