---
title: "MLB — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 5
description: "MLB — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# MLB — additional Python functions — Other

### most_recent_mlb_season {#most_recent_mlb_season}

`most_recent_mlb_season() -> 'int'`

most_recent_mlb_season - return the most recent / current MLB season year.

MLB seasons run calendar-year. Before April we still consider the *previous* year
the "most recent" season (since spring training only starts in late February).

**Returns**

The most recent MLB season year (e.g. `2024`).

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

**Example**

```python
from sportsdataverse.mlb.mlb_baserunning import advancement_opportunities
opps = advancement_opportunities(pitches)
```

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

**Example**

```python
from sportsdataverse.mlb.mlb_run_values import as_of_split
history = as_of_split(events, cutoff_date=dt.date(2024, 6, 15))
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

**Example**

```python
from sportsdataverse.mlb.mlb_fielding_oaa import bip_trajectory_features
feats = bip_trajectory_features(bip)
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

**Example**

```python
from sportsdataverse.mlb.mlb_fielding_oaa import catch_prob_surface
surface = catch_prob_surface(bip, alpha=2.0)
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

**Example**

```python
from sportsdataverse.mlb.mlb_run_values import count_strike_run_value
rv = count_strike_run_value(pitches)
```

### espn_mlb_teams {#espn_mlb_teams}

`espn_mlb_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mlb_teams - look up MLB teams from ESPN's Site v2 API.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False (default), returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for MLB. This function caches by default, so if you want to refresh the data, use `sportsdataverse.mlb.espn_mlb_teams.cache_clear()`.

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character | Short team abbreviation (e.g. 'NYY'). |
| `team_alternate_color` | character | Team alternate color (hex). |
| `team_color` | character | Team primary color (hex, no leading '#'). |
| `team_display_name` | character | Full team display name (e.g. 'New York Yankees'). |
| `team_id` | character | Unique ESPN team identifier. |
| `team_is_active` | logical | Team is active. |
| `team_is_all_star` | logical | Team is all star. |
| `team_location` | character | Team city / location. |
| `team_logos` | integer | Team logo metadata. |
| `team_name` | character | Team name. |
| `team_nickname` | character | Team nickname. |
| `team_short_display_name` | character | Short team display name. |
| `team_slug` | character | URL-safe team identifier. |
| `team_uid` | character | ESPN universal team identifier (UID). |

**Example**

```python
from sportsdataverse.mlb import espn_mlb_teams
teams = espn_mlb_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

# Find Los Angeles Dodgers (team_id 19)

import polars as pl
teams.filter(pl.col("team_id") == "19").to_dicts()

# Refresh the cache (the call is ``lru_cache``'d) and round-trip to pandas

espn_mlb_teams.cache_clear()
teams_pd = espn_mlb_teams(return_as_pandas=True)
teams_pd[["team_id", "team_abbreviation", "team_display_name"]].head()
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

**Example**

```python
from sportsdataverse.mlb.mlb_stolen_base import predict_sb_success
preds = predict_sb_success(upcoming, history, cutoff_date=dt.date(2024, 6, 15))
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
