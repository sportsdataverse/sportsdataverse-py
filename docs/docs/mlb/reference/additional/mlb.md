---
title: "MLB — additional Python functions — Mlb"
sidebar_label: "Mlb"
sidebar_position: 2
description: "MLB — additional Python functions — Mlb — function reference in sdv-py, the SportsDataverse Python package."
---
# MLB — additional Python functions — Mlb

### mlb_attendance {#mlb_attendance}

`mlb_attendance(team_id: 'Optional[int]' = None, league_id: 'Optional[Union[int, str]]' = None, season: 'Optional[Union[int, str]]' = None, league_list_id: 'Optional[str]' = None, game_type: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/attendance — game attendance figures.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Optional[int]` | `None` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `league_list_id` | `Optional[str]` | `None` |  |
| `game_type` | `Optional[str]` | `None` |  |

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

**Example**

```python
from sportsdataverse.mlb.mlb_baserunning import mlb_baserunning_value
baserunning = mlb_baserunning_value(pitches, sprint_speed)
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

**Example**

```python
from sportsdataverse.mlb.mlb_catcher_defense import mlb_catcher_throwing
throwing = mlb_catcher_throwing(sb_attempts)
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

### mlb_divisions {#mlb_divisions}

`mlb_divisions(sport_id: 'int' = 1, league_id: 'Optional[Union[int, str]]' = None, division_id: 'Optional[int]' = None, **kwargs) -> 'Dict'`

GET /api/v1/divisions — list divisions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport_id` | `int` | `1` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `division_id` | `Optional[int]` | `None` |  |

### mlb_draft_prospects {#mlb_draft_prospects}

`mlb_draft_prospects(year: 'Union[int, str]', scouting_report: 'Optional[bool]' = None, limit: 'int' = 100, **kwargs) -> 'Dict'`

GET /api/v1/draft/prospects/{year} — draft prospect list for a year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  |  |
| `scouting_report` | `Optional[bool]` | `None` |  |
| `limit` | `int` | `100` |  |

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

### mlb_pbp_diff {#mlb_pbp_diff}

`mlb_pbp_diff(game_pk: 'int', start_timecode: 'str', end_timecode: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/game/{gamePk}/feed/live/diffPatch — JSON-patch diff of the live feed.

Replays of in-game state for low-bandwidth clients.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_pk` | `int` |  |  |
| `start_timecode` | `str` |  |  |
| `end_timecode` | `Optional[str]` | `None` |  |

### mlb_pbp_live {#mlb_pbp_live}

`mlb_pbp_live(game_pk: 'int', language: 'Optional[str]' = None, timecode: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1.1/game/{gamePk}/feed/live — live firehose (v1.1).

Top-level keys: `copyright, gamePk, link, metaData, gameData, liveData`.
Includes Statcast metrics where available. The historical name
`mlb_pbp` is preserved as an alias in the generated module.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_pk` | `int` |  |  |
| `language` | `Optional[str]` | `None` |  |
| `timecode` | `Optional[str]` | `None` |  |
| `hydrate` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### mlb_person_stats {#mlb_person_stats}

`mlb_person_stats(person_id: 'int', stats: 'str', group: 'str' = 'hitting', season: 'Optional[Union[int, str]]' = None, season_type: 'Optional[str]' = None, sport_ids: 'Optional[Union[int, List[int]]]' = None, game_type: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/people/{personId}/stats — player aggregate stats.

`stats`: `season`, `career`, `yearByYear`, `vsTeam`, `vsPlayer`,
`byMonth`, `byDayOfWeek`, `homeAndAway`, `gameLog`, `lastXGames`, …

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `person_id` | `int` |  |  |
| `stats` | `str` |  |  |
| `group` | `str` | `'hitting'` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `season_type` | `Optional[str]` | `None` |  |
| `sport_ids` | `Optional[Union[int, List[int]]]` | `None` |  |
| `game_type` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

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

### mlb_schedule {#mlb_schedule}

`mlb_schedule(date: 'Optional[str]' = None, start_date: 'Optional[str]' = None, end_date: 'Optional[str]' = None, team_id: 'Optional[int]' = None, opponent_id: 'Optional[int]' = None, season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, game_type: 'Optional[str]' = None, league_id: 'Optional[Union[int, str]]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/schedule — schedule of games for a date, range, team, or season.

Response: `dates[].games[]`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `Optional[str]` | `None` |  |
| `start_date` | `Optional[str]` | `None` |  |
| `end_date` | `Optional[str]` | `None` |  |
| `team_id` | `Optional[int]` | `None` |  |
| `opponent_id` | `Optional[int]` | `None` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `sport_id` | `int` | `1` |  |
| `game_type` | `Optional[str]` | `None` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `hydrate` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### mlb_seasons {#mlb_seasons}

`mlb_seasons(sport_id: 'int' = 1, season: 'Optional[Union[int, str]]' = None, all_seasons: 'bool' = False, **kwargs) -> 'Dict'`

GET /api/v1/seasons — list of seasons for a sport.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport_id` | `int` | `1` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `all_seasons` | `bool` | `False` |  |

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

### mlb_standings {#mlb_standings}

`mlb_standings(league_id: 'Union[int, str, List[int]]' = '103,104', season: 'Optional[Union[int, str]]' = None, date: 'Optional[str]' = None, standings_types: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/standings — league standings.

`league_id`: `103` AL, `104` NL (comma-separated for both, the default).
`standings_types` e.g. `regularSeason`, `wildCard`, `divisionLeaders`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_id` | `Union[int, str, List[int]]` | `'103,104'` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `date` | `Optional[str]` | `None` |  |
| `standings_types` | `Optional[str]` | `None` |  |
| `hydrate` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### mlb_statcast_player {#mlb_statcast_player}

`mlb_statcast_player(player_id: 'int', stats: 'Optional[str]' = None, *, section: 'str' = 'statcast', raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "'Union[pl.DataFrame, pd.DataFrame, str]'"`

GET /savant-player/{player_id} and parse one embedded table into a tidy frame.

Returns a tidy frame **by default** (the parsed Statcast page); pass
`raw=True` to get the underlying HTML string instead (the page embeds ~12
other tables you can mine yourself, or feed to
`sportsdataverse.mlb.parse_mlb_statcast_player` with a different
`section`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  | MLBAM player id (shared with the Stats API `personId`). |
| `stats` | `Optional[str]` | `None` | optional `stats` query value to scope the embedded payload. |
| `section` | `str` | `'statcast'` | which embedded `serverVals` table to flatten (default `"statcast"`, the seasonal aggregate; e.g. `"statcastGameLogs"`). |
| `raw` | `bool` | `False` | return the raw page HTML string instead of a parsed frame. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame of the player's Statcast metrics by default (zero rows when the page/section is absent); the raw HTML `str` when `raw=True`.

| col_name | type | description |
|---|---|---|
| `aggregate` | integer | Aggregate. |
| `year` | integer | Season year. |
| `yearhidden` | integer | Yearhidden. |
| `player_id` | integer | MLBAM player id. |
| `age` | integer | Player age. |
| `bat_side` | character | Batter side (R/L/S). |
| `pitch_hand` | character | Pitcher handedness (R/L). |
| `month` | character | Month. |
| `grouping_code` | character | Grouping code. |
| `grouping_cat` | character | Grouping cat. |
| `pitch_count` | integer | Pitch count. |
| `in_zone_percent` | numeric | In zone rate. |
| `out_zone_percent` | numeric | Out zone rate. |
| `edge_percent` | numeric | Edge rate. |
| `z_swing_percent` | numeric | Z swing rate. |
| `oz_swing_percent` | numeric | Oz swing rate. |
| `iz_contact_percent` | numeric | Iz contact rate. |
| `oz_contact_percent` | numeric | Oz contact rate. |
| `whiff_percent` | numeric | Whiff rate (swings and misses / swings). |
| `f_strike_percent` | numeric | F strike rate. |
| `f_swing_percent` | numeric | F swing rate. |
| `swing_percent` | numeric | Swing rate. |
| `meatball_swing_percent` | integer | Meatball swing rate. |
| `meatball_percent` | numeric | Meatball rate. |
| `z_swing_miss_percent` | numeric | Z swing miss rate. |
| `oz_swing_miss_percent` | numeric | Oz swing miss rate. |
| `in_zone` | integer | In zone. |
| `out_zone` | integer | Out zone. |
| `edge` | integer | Edge. |
| `popups` | integer | Popups. |
| `flyballs` | integer | Flyballs. |
| `linedrives` | integer | Linedrives. |
| `groundballs` | integer | Groundballs. |
| `airballs` | integer | Airballs. |
| `popups_percent` | numeric | Popups rate. |
| `flyballs_percent` | numeric | Flyballs rate. |
| `linedrives_percent` | numeric | Linedrives rate. |
| `groundballs_percent` | numeric | Groundballs rate. |
| `airballs_percent` | numeric | Airballs rate. |
| `pull_percent` | numeric | Pull rate. |
| `straightaway_percent` | numeric | Straightaway rate. |
| `opposite_percent` | numeric | Opposite rate. |
| `pull_percent_airballs` | numeric | Pull percent airballs. |
| `straightaway_percent_airballs` | numeric | Straightaway percent airballs. |
| `opposite_percent_airballs` | integer | Opposite percent airballs. |
| `pull_percent_groundballs` | numeric | Pull percent groundballs. |
| `straightaway_percent_groundballs` | numeric | Straightaway percent groundballs. |
| `opposite_percent_groundballs` | numeric | Opposite percent groundballs. |
| `pull_percent_popups` | numeric | Pull percent popups. |
| `straightaway_percent_popups` | numeric | Straightaway percent popups. |
| `opposite_percent_popups` | numeric | Opposite percent popups. |
| `pull_percent_flyballs` | numeric | Pull percent flyballs. |
| `straightaway_percent_flyballs` | numeric | Straightaway percent flyballs. |
| `opposite_percent_flyballs` | numeric | Opposite percent flyballs. |
| `pull_percent_linedrives` | integer | Pull percent linedrives. |
| `straightaway_percent_linedrives` | integer | Straightaway percent linedrives. |
| `opposite_percent_linedrives` | integer | Opposite percent linedrives. |
| `poorlyweak_percent` | integer | Poorlyweak rate. |
| `poorlytopped_percent` | numeric | Poorlytopped rate. |
| `poorlyunder_percent` | numeric | Poorlyunder rate. |
| `flareburner_percent` | numeric | Flareburner rate. |
| `solidcontact_percent` | numeric | Solidcontact rate. |
| `hr_flyballs_percent` | numeric | Hr flyballs rate. |
| `in_zone_swing` | integer | In zone swing. |
| `out_zone_swing` | integer | Out zone swing. |
| `in_zone_swing_miss` | integer | In zone swing miss. |
| `out_zone_swing_miss` | integer | Out zone swing miss. |
| `pitch_count_fastball` | integer | Pitch count fastball. |
| `pitch_count_offspeed` | integer | Pitch count offspeed. |
| `pitch_count_breaking` | integer | Pitch count breaking. |
| `pa` | integer | Plate appearances. |
| `ab` | integer | At-bats. |
| `hit` | integer | Hit. |
| `single` | integer | Singles. |
| `double` | integer | Doubles. |
| `triple` | integer | Triples. |
| `home_run` | integer | Home run. |
| `walk` | integer | Walk. |
| `strikeout` | integer | Strikeout. |
| `hbp` | integer | Hbp. |
| `k_percent` | numeric | Strikeout rate. |
| `bb_percent` | numeric | Walk rate. |
| `sz_judge` | numeric | Sz judge. |
| `batted_ball` | integer | Batted ball. |
| `barrel` | integer | Barrel. |
| `barrel_batted_rate` | numeric | Barrels per batted ball. |
| `barrels_per_pa` | numeric | Barrels per pa. |
| `launch_angle_avg` | numeric | Launch angle avg. |
| `exit_velocity_avg` | numeric | Exit velocity avg. |
| `exit_velocity_max` | numeric | Exit velocity max. |
| `hard_hit_percent` | numeric | Hard-hit rate (95+ mph EV). |
| `sweet_spot_percent` | numeric | Sweet-spot rate (8-32 deg launch angle). |
| `ba` | numeric | Batting average. |
| `xba` | numeric | Expected batting average. |
| `bacon` | numeric | Bacon. |
| `xbacon` | numeric | Expected batting average on contact. |
| `babip` | numeric | BABIP. |
| `obp` | numeric | On-base percentage. |
| `slg` | numeric | Slugging percentage. |
| `xobp` | numeric | Expected on-base percentage. |
| `xslg` | numeric | Expected slugging. |
| `iso` | numeric | Isolated power. |
| `xiso` | numeric | Expected isolated power. |
| `woba` | numeric | Weighted on-base average. |
| `xwoba` | numeric | Expected wOBA. |
| `wobacon` | numeric | Wobacon. |
| `xwobacon` | numeric | Xwobacon. |
| `xbadiff` | numeric | Xbadiff. |
| `xslgdiff` | numeric | Xslgdiff. |
| `wobadiff` | numeric | Wobadiff. |
| `player_type` | character | Player type. |
| `era` | character | Era. |
| `xera` | character | Expected ERA. |
| `avg_hyper_speed` | numeric | Avg hyper speed. |
| `avg_best_speed` | numeric | Avg best speed. |
| `distance_hr_avg` | integer | Distance hr avg. |
| `sprint_speed` | numeric | Sprint speed (ft/sec, top 50% of competitive runs). |
| `pop_2b` | character | Pop 2b. |
| `arm_cs_2b` | character | Arm cs 2b. |
| `strike_rate` | character | Called-strike rate. |
| `outs_above_average` | integer | Outs Above Average. |
| `jump_v_avg` | integer | Jump v avg. |
| `max_arm_strength` | character | Max arm strength (mph). |
| `arm_overall` | character | Arm overall. |
| `xhr` | numeric | Xhr. |
| `swing_take_run_value` | integer | Swing take run value. |
| `blocks_above_average` | character | Blocks above average. |
| `cs_above_average` | character | Cs above average. |
| `fastball_velo` | character | Fastball velo. |
| `fastball_spin` | character | Fastball spin. |
| `fastball_extension` | character | Fastball extension. |
| `curveball_spin` | character | Curveball spin. |
| `pitch_run_value_fastball` | numeric | Pitch run value fastball. |
| `pitch_run_value_breaking` | numeric | Pitch run value breaking. |
| `pitch_run_value_offspeed` | numeric | Pitch run value offspeed. |
| `group_fastball_velo` | numeric | Group fastball velo. |
| `group_breaking_velo` | numeric | Group breaking velo. |
| `group_offspeed_velo` | numeric | Group offspeed velo. |
| `pitch_usage_fastball` | numeric | Pitch usage fastball. |
| `pitch_usage_breaking` | numeric | Pitch usage breaking. |
| `pitch_usage_offspeed` | numeric | Pitch usage offspeed. |
| `fielding_run_value` | integer | Fielding run value. |
| `runner_run_value` | integer | Runner run value. |
| `fielding_run_value_arm` | integer | Fielding run value arm. |
| `fielding_run_value_framing` | character | Fielding run value framing. |
| `runner_runs_sb` | integer | Runner runs sb. |
| `runner_runs_xb` | integer | Runner runs xb. |
| `net_bases_runner` | integer | Net bases runner. |
| `net_bases_pitcher` | character | Net bases pitcher. |
| `fast_swing_rate` | character | Fast-swing rate (>=75 mph). |
| `squared_up_contact` | character | Squared up contact. |
| `squared_up_swing` | character | Squared up swing. |
| `blasts_contact` | character | Blasts contact. |
| `blasts_swing` | character | Blasts swing. |
| `swords` | character | Swords. |
| `avg_swing_speed` | character | Avg swing speed. |
| `avg_swing_length` | character | Avg swing length. |
| `attack_angle` | character | Attack angle (deg, bat path at contact). |
| `vertical_swing_path` | character | Vertical swing path. |
| `acceleration` | character | Acceleration. |
| `horizontal_swing_path` | character | Horizontal swing path. |
| `attack_direction` | character | Attack direction (deg, pull/oppo). |
| `ideal_angle_rate` | character | Ideal angle rate. |
| `n_squared_up` | character | Number of squared up. |
| `n_blasts` | character | Number of blasts. |
| `arm_angle` | character | Arm angle. |
| `is_qualified` | integer | Is qualified. |
| `percent_rank_barrel_unrounded` | character | Percent rank barrel unrounded. |
| `percent_rank_barrel_batted_rate_unrounded` | character | Percent rank barrel batted rate unrounded. |
| `percent_rank_exit_velocity_avg_unrounded` | character | Percent rank exit velocity avg unrounded. |
| `percent_rank_exit_velocity_max_unrounded` | numeric | Percent rank exit velocity max unrounded. |
| `percent_rank_launch_angle_avg_unrounded` | character | Percent rank launch angle avg unrounded. |
| `percent_rank_xba_unrounded` | character | Percent rank xba unrounded. |
| `percent_rank_xslg_unrounded` | character | Percent rank xslg unrounded. |
| `percent_rank_xwoba_unrounded` | character | Percent rank xwoba unrounded. |
| `percent_rank_woba_unrounded` | character | Percent rank woba unrounded. |
| `percent_rank_hard_hit_percent_unrounded` | character | Percent rank hard hit percent unrounded. |
| `percent_rank_xwobacon_unrounded` | character | Percent rank xwobacon unrounded. |
| `percent_rank_wobacon_unrounded` | character | Percent rank wobacon unrounded. |
| `percent_rank_k_percent_unrounded` | character | Percent rank k percent unrounded. |
| `percent_rank_bb_percent_unrounded` | character | Percent rank bb percent unrounded. |
| `percent_rank_sz_judge_unrounded` | character | Percent rank sz judge unrounded. |
| `percent_rank_whiff_percent_unrounded` | character | Percent rank whiff percent unrounded. |
| `percent_rank_chase_percent_unrounded` | character | Percent rank chase percent unrounded. |
| `percent_rank_ba_unrounded` | character | Percent rank ba unrounded. |
| `percent_rank_bacon_unrounded` | character | Percent rank bacon unrounded. |
| `percent_rank_xbacon_unrounded` | character | Percent rank xbacon unrounded. |
| `percent_rank_babip_unrounded` | character | Percent rank babip unrounded. |
| `percent_rank_obp_unrounded` | character | Percent rank obp unrounded. |
| `percent_rank_slg_unrounded` | character | Percent rank slg unrounded. |
| `percent_rank_xobp_unrounded` | character | Percent rank xobp unrounded. |
| `percent_rank_iso_unrounded` | character | Percent rank iso unrounded. |
| `percent_rank_xiso_unrounded` | character | Percent rank xiso unrounded. |
| `percent_rank_sweet_spot_percent_unrounded` | character | Percent rank sweet spot percent unrounded. |
| `percent_rank_distance_hr_avg_unrounded` | character | Percent rank distance hr avg unrounded. |
| `percent_rank_groundballs_percent_unrounded` | character | Percent rank groundballs percent unrounded. |
| `percent_rank_airballs_percent_unrounded` | character | Percent rank airballs percent unrounded. |
| `percent_rank_avg_hyper_speed_unrounded` | character | Percent rank avg hyper speed unrounded. |
| `percent_rank_avg_best_speed_unrounded` | character | Percent rank avg best speed unrounded. |
| `percent_rank_pitch_run_value_fastball_unrounded` | character | Percent rank pitch run value fastball unrounded. |
| `percent_rank_pitch_run_value_breaking_unrounded` | character | Percent rank pitch run value breaking unrounded. |
| `percent_rank_pitch_run_value_offspeed_unrounded` | character | Percent rank pitch run value offspeed unrounded. |
| `percent_rank_barrel` | character | Percent rank barrel. |
| `percent_rank_barrel_batted_rate` | character | Percent rank barrel batted rate. |
| `percent_rank_exit_velocity_avg` | character | Percent rank exit velocity avg. |
| `percent_rank_exit_velocity_max` | integer | Percent rank exit velocity max. |
| `percent_rank_launch_angle_avg` | character | Percent rank launch angle avg. |
| `percent_rank_xba` | character | Percent rank xba. |
| `percent_rank_xslg` | character | Percent rank xslg. |
| `percent_rank_xwoba` | character | Percent rank xwoba. |
| `percent_rank_woba` | character | Percent rank woba. |
| `percent_rank_hard_hit_percent` | character | Percent rank hard hit rate. |
| `percent_rank_xwobacon` | character | Percent rank xwobacon. |
| `percent_rank_wobacon` | character | Percent rank wobacon. |
| `percent_rank_k_percent` | character | Percent rank k rate. |
| `percent_rank_bb_percent` | character | Percent rank bb rate. |
| `percent_rank_sz_judge` | character | Percent rank sz judge. |
| `percent_rank_whiff_percent` | character | Percent rank whiff rate. |
| `percent_rank_chase_percent` | character | Percent rank chase rate. |
| `percent_rank_ba` | character | Percent rank ba. |
| `percent_rank_bacon` | character | Percent rank bacon. |
| `percent_rank_xbacon` | character | Percent rank xbacon. |
| `percent_rank_babip` | character | Percent rank babip. |
| `percent_rank_obp` | character | Percent rank obp. |
| `percent_rank_slg` | character | Percent rank slg. |
| `percent_rank_xobp` | character | Percent rank xobp. |
| `percent_rank_iso` | character | Percent rank iso. |
| `percent_rank_xiso` | character | Percent rank xiso. |
| `percent_rank_sweet_spot_percent` | character | Percent rank sweet spot rate. |
| `percent_rank_distance_hr_avg` | character | Percent rank distance hr avg. |
| `percent_rank_groundballs_percent` | character | Percent rank groundballs rate. |
| `percent_rank_airballs_percent` | character | Percent rank airballs rate. |
| `percent_rank_avg_hyper_speed` | character | Percent rank avg hyper speed. |
| `percent_rank_avg_best_speed` | character | Percent rank avg best speed. |
| `percent_rank_pitch_run_value_fastball` | character | Percent rank pitch run value fastball. |
| `percent_rank_pitch_run_value_breaking` | character | Percent rank pitch run value breaking. |
| `percent_rank_pitch_run_value_offspeed` | character | Percent rank pitch run value offspeed. |
| `percent_speed_order` | integer | Percent speed order. |
| `percent_rank_speed_order` | integer | Percent rank speed order. |
| `percent_rank_pop_2b` | character | Percent rank pop 2b. |
| `percent_rank_arm_cs_2b` | character | Percent rank arm cs 2b. |
| `percent_rank_oaa` | character | Percent rank oaa. |
| `percent_rank_framing` | character | Percent rank framing. |
| `percent_rank_jump` | character | Percent rank jump. |
| `percent_rank_fastball_velo` | character | Percent rank fastball velo. |
| `percent_rank_fastball_spin` | character | Percent rank fastball spin. |
| `percent_rank_fastball_extension` | character | Percent rank fastball extension. |
| `percent_rank_cu_spin` | character | Percent rank cu spin. |
| `percent_rank_xera` | character | Percent rank xera. |
| `percent_rank_arm_max` | character | Percent rank arm max. |
| `percent_rank_arm_overall` | character | Percent rank arm overall. |
| `percent_rank_xhr` | character | Percent rank xhr. |
| `percent_rank_swing_take_run_value` | character | Percent rank swing take run value. |
| `percent_rank_blocks_above_average` | character | Percent rank blocks above average. |
| `percent_rank_cs_above_average` | character | Percent rank cs above average. |
| `percent_rank_fielding_run_value` | character | Percent rank fielding run value. |
| `percent_rank_runner_run_value` | character | Percent rank runner run value. |
| `percent_rank_fielding_run_value_arm` | character | Percent rank fielding run value arm. |
| `percent_rank_fielding_run_value_framing` | character | Percent rank fielding run value framing. |
| `percent_rank_swing_speed` | character | Percent rank swing speed. |
| `percent_rank_swing_length` | character | Percent rank swing length. |
| `percent_rank_squared_up_swing` | character | Percent rank squared up swing. |
| `percent_rank_attack_angle` | character | Percent rank attack angle. |
| `percent_rank_vertical_swing_path` | character | Percent rank vertical swing path. |
| `percent_rank_acceleration` | character | Percent rank acceleration. |
| `percent_rank_ideal_angle_rate` | character | Percent rank ideal angle rate. |

**Example**

```python
from sportsdataverse.mlb import mlb_statcast_player
df = mlb_statcast_player(592450)
html = mlb_statcast_player(592450, raw=True)
```
