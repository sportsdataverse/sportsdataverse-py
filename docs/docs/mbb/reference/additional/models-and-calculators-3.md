---
title: "Men's college basketball — additional Python functions — Models and calculators: mbb_transfer–win_prob"
sidebar_label: "Models and calculators: mbb_transfer–win_prob"
sidebar_position: 12
description: "Men's college basketball — additional Python functions — Models and calculators: mbb_transfer–win_prob — function reference in sdv-py, the SportsDataverse Python package."
---
# Men's college basketball — additional Python functions — Models and calculators: mbb_transfer–win_prob

### mbb_transfer_projection {#mbb_transfer_projection}

`mbb_transfer_projection(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Projected post-transfer box-BPM for each transfer arriving in `seasons`.

Detects the transfer cohort from BOXSCORE discontinuity -- a player who
logged qualifying minutes for different teams in consecutive seasons
(the roster release under-reports moves ~70x, so production is the
cohort source of record; bench-riders pre-move are excluded, which is
fine because they carry no pre production to project from). Joins each
player's pre-transfer (from-season) `box_bpm` and scores the bundled
ridge. `proj_delta = proj_box_bpm - pre_box_bpm` (the expected
move-related change; typically shrinks stars toward the mean).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | Destination season(s), e.g. `2026` = arrived for 2025-26. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per transfer: `player_id:Utf8, player, from_team_id:Utf8, to_team_id:Utf8, to_season:Int64, pre_box_bpm, proj_box_bpm, proj_delta`. Transfers without a qualifying pre-season sample are dropped. Empty input returns the schema with zero rows.

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `player` | String | Player name. |
| `from_team_id` | String | Unique identifier for from team. |
| `to_team_id` | String | Unique identifier for to team. |
| `to_season` | Int64 |  |
| `pre_box_bpm` | Float64 |  |
| `proj_box_bpm` | Float64 |  |
| `proj_delta` | Float64 |  |

**Example**

```python
from sportsdataverse.mbb import mbb_transfer_projection
proj = mbb_transfer_projection(2026)

# Pipeline next step (one line)

proj.sort("proj_box_bpm", descending=True).head(15)
```

### pick_ridge_regression {#pick_ridge_regression}

`pick_ridge_regression(off_weights: 'NDArray[np.float64]', def_weights: 'NDArray[np.float64]', ctx: 'RapmPlayerContext', adaptive_correl_weights: 'list[float] | None', diag_mode: 'bool', agg_value_key: 'ValueKey' = 'value', lineup_value_keys: 'tuple[ValueKey, ValueKey]' = ('value', 'value')) -> 'tuple[RapmProcessingInputs, RapmProcessingInputs]'`

Adaptively pick a ridge-regression lambda and blend in the RAPM priors.

Faithful port of `RapmUtils.pickRidgeRegression` (`RapmUtils.ts:1001-1540`)
-- the top-level driver that, per off/def side: scales a dimensionless
`lambda_range` by the design matrix's mean singular value
(`avg_eigen_val`) into an actual ridge strength, solves via
`slow_regression`/`calculate_rapm`, blends in each player's
strong prior (`_get_strong_weight`), reconciles the possession
-weighted team total against the actual team efficiency
(`[IMPORTANT-EQUATION-01]`, see below), nudges the result back towards
the weak priors on any remaining error (`apply_weak_priors`), and
decides whether to keep sweeping `lambda` upward, roll back to the
previous step, or stop.

**`[IMPORTANT-EQUATION-01]`** (`RapmUtils.ts:1306-1314`/`:1325-1333`):
`combined_adj_eff = sum(pct_by_player[i] * rapm[i] for i) +
add_low_volume_adj_rtg`, compared against `actual_eff[off_or_def]`
(the team's actual, prior-basis-adjusted efficiency, including
bench/removed-player possessions) to derive `adj_eff_err` -- the error
signal both the weak-prior nudge and the stopping rule react to.

**Stopping rule** (checked once per `lambda` step, in order): (1) once a
*second* step has run (`not_first_step`) and, unless in `diag_mode`,
the current step is past `lambda_range_to_use[3]`, roll back to the
*previous* step's `soln_matrix`/`ridge_lambda` (but **not**
`rapm_adj_ppp`/`rapm_raw_adj_ppp`/`sd_rapm`, which stay at the
current, over-threshold step's values -- a faithful, non-obvious TS
asymmetry, `RapmUtils.ts:1443-1448` vs `:1483-1484`) when
`adj_eff_err >= error_exit_thresh` (`1.35` for the low-possession
-count offense special case, else `1.05`) **and** the error is still
increasing (`>= last_error`); else (2) stop in place once
`mean_diff` (the mean per-player RAPM change since the previous step)
drops below `pick_ridge_thresh` (`0.061` off / `0.091` def --
"more confident in offensive priors"); else (3) keep sweeping.

**Adaptive-weight / prior asymmetry** (the deep-equality oracle's load
-bearing behavior): the per-player strong-prior blend
(`_get_strong_weight(ctx["prior_info"], adaptive_correl_weights[i])`)
only consults `adaptive_correl_weights` when
`ctx["prior_info"]["strong_weight"] < 0` (adaptive mode) -- a fixed,
non-negative `strong_weight` always wins. A fixture whose
`players_strong` entries carry no `def_adj_ppp` key makes the blend's
`stat.get(f"{off_or_def}_adj_ppp") or 0.0` term (and, transitively,
`calc_lineup_outputs`'s own `strong_val` term) contribute exactly
`0` on the def side regardless of `strong_weight` or
`adaptive_correl_weights` -- see the oracle test's `def_results1`/
`def_results2` invariance assertions.

**`svd` is `numpy.linalg.svd(..., compute_uv=False)`, singular values
only.** Upstream's `SVD(weights[side].valueOf())` (`svd-js`) also
computes `u`/`v`, but only `svd.q` (the singular values, via
`mean(svd.off.q)`/`mean(svd.def.q)` at `avg_eigen_val`,
`RapmUtils.ts:1077`) is ever read -- `u`/`v` are dead. Skipping them
is an efficiency-only deviation with an identical result (singular
values are unique to a matrix regardless of the underlying SVD
implementation).

**Dead-debug computation promoted to a real output (Python-side
addition, not upstream's own shape):** upstream also computes
`residuals`/`errSq`/`paramErrs`/`sdRapm` at this point
(`RapmUtils.ts:1363-1394`) purely to feed a `console.log` gated
behind the same hardcoded-`False` `debugMode` as
`apply_weak_priors` -- none of the four is ever stored on
`acc.output` upstream (`RapmProcessingInputs` has no `sdRapm`
field there either). Since this module builds
`calculate_predicted_out`/`calculate_residual_error`/
`calc_slow_pseudo_inverse`/`calculate_sd_rapm` specifically
so this task could surface real standard errors, this port keeps
calling all four (matching TS's actual computation, which reuses the
exact same `XᵀX + ridge_lambda·I` inverse `slow_regression`
already computed -- so no *new* failure mode is introduced by keeping
this) and additionally stores the result on `sd_rapm` -- a superset
of, not a divergence from, the upstream return shape.

**`soln_matrix`/`sd_rapm` are nested Python `list`s, not
`NDArray`s.** Every field on the returned `RapmProcessingInputs`
is a plain (possibly nested) Python `list`/`float` specifically so
the whole dict stays comparable via plain `==` -- the oracle's deep
-equality assertions (e.g. `off_results1 == off_results`) would
otherwise raise `ValueError: truth value of an array with more than one
element is ambiguous` the moment Python's dict/list equality machinery
tried to `bool()` a multi-element `ndarray` comparison.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `off_weights` | `NDArray[float64]` |  | The offensive design matrix (e.g. `calc_player_weights`'s first return value). |
| `def_weights` | `NDArray[float64]` |  | The defensive design matrix. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`. |
| `adaptive_correl_weights` | `list[float] \| None` |  | Optional per-player adaptive-correlation weights (index-aligned with `ctx["col_to_player"]`) -- see the "adaptive-weight / prior asymmetry" note above. |
| `diag_mode` | `bool` |  | If `True`, keeps sweeping every remaining `lambda` step (collecting `prev_attempts` diagnostics for all of them) even after a stopping condition has already fired, and relaxes the rollback/pick eligibility guards for the first few (`< lambda_range_to_use[3]`) diagnostic-only steps. **Not exercised by this task's oracle** (always called with `False`) -- ported faithfully from TS, uncovered by test. |
| `agg_value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- which key team/aggregate-level reads (`actual_eff`, the low-volume player adjustment) prefer when present. |
| `lineup_value_keys` | `tuple[ValueKey, ValueKey]` | `('value', 'value')` | `(off_key, def_key)` -- forwarded to `calc_lineup_outputs` as its `use_old_val_if_possible` flag (translated: `key == "old_value"`). |

**Returns**

`(off_results, def_results)` -- two `RapmProcessingInputs`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import pick_ridge_regression

off_results, def_results = pick_ridge_regression(
    off_weights, def_weights, ctx, None, False
)
print(off_results["ridge_lambda"], off_results["rapm_adj_ppp"][:3])
```

### player_per100_features {#player_per100_features}

`player_per100_features(season_stats: 'pl.DataFrame') -> 'pl.DataFrame'`

Per-100 / rate features for every (player_id, season).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season_stats` | `DataFrame` |  | One row per player-season with the canonical counting columns (`minutes, field_goals_made, field_goals_attempted, three_point_field_goals_made, free_throws_attempted, turnovers, points, fga_rim, fga_mid, fga_three, offensive_rebounds, defensive_rebounds, assists, blocks, steals`) -- built from the player-boxscore aggregation (see the Phase-0 fitters). |

**Returns**

One row per (player_id, season): ids as `Utf8` plus the 17 rate / per-100 features. Empty input returns the schema with zero rows.

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `season` | Int64 | Season year. |
| `team_id` | String | Unique team identifier. |
| `min` | Float64 | Minutes played. |
| `usage` | Float64 | Usage rate. |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `efg_pct` | Float64 |  |
| `ast_pct` | Float64 | Assist percentage. |
| `tov_pct` | Float64 |  |
| `oreb_pct` | Float64 |  |
| `dreb_pct` | Float64 |  |
| `blk_pct` | Float64 | Blocks percentage (0-1 decimal). |
| `stl_pct` | Float64 | Steals percentage (0-1 decimal). |
| `ftr` | Float64 | Free-throw rate (offense). |
| `rim_share` | Float64 |  |
| `mid_share` | Float64 |  |
| `three_share` | Float64 |  |
| `pts_per100` | Float64 |  |
| `reb_per100` | Float64 |  |
| `ast_per100` | Float64 |  |

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import player_per100_features
feats = player_per100_features(season_stats)
```

### poss_calc_fragment_sum {#poss_calc_fragment_sum}

`poss_calc_fragment_sum(a: 'PossCalcFragment', b: 'PossCalcFragment') -> 'PossCalcFragment'`

Field-wise add two `PossCalcFragment`\ s

(`PossCalcFragment.sum`).

The Scala original uses `shapeless.Generic` to zip the two case
classes' fields and sum pairwise; since every field is a plain `Int`,
a plain `zip` over `dataclasses.astuple` reproduces the same
behavior without the generic-programming machinery.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `PossCalcFragment` |  | The left-hand fragment. |
| `b` | `PossCalcFragment` |  | The right-hand fragment. |

**Returns**

A new `PossCalcFragment` with each field summed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import (
    PossCalcFragment,
    poss_calc_fragment_sum,
)

frag1 = PossCalcFragment(1, 2, 3, 4, 5, 6, 7, 8)
frag2 = PossCalcFragment(1, 3, 5, 7, 9, 11, 13, 15)
poss_calc_fragment_sum(frag1, frag2)
# PossCalcFragment(2, 5, 8, 11, 14, 17, 20, 23)
```

### predict_margin {#predict_margin}

`predict_margin(home_adj_em: 'float', away_adj_em: 'float', neutral: 'bool' = False, *, league: 'str' = 'mens') -> 'float'`

Expected home-minus-away margin from two adjusted efficiency margins.

The AdjEM difference is scaled by the league's fitted `em_scale` (AdjEM
is per-100-possessions; a game margin scales by roughly tempo/100, further
attenuated for as-of estimation noise) before the home-court advantage is
added.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_em` | `float` |  | Home team's adjusted efficiency margin (points / 100 poss). |
| `away_adj_em` | `float` |  | Away team's adjusted efficiency margin. |
| `neutral` | `bool` | `False` | True for a neutral-site game (no home-court advantage). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the fitted em_scale / HFA). |

**Returns**

Expected margin in points (positive favors the home team).

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import predict_margin
predict_margin(20.0, 10.0)
```

### predict_total {#predict_total}

`predict_total(home_adj_o: 'float', home_adj_d: 'float', away_adj_o: 'float', away_adj_d: 'float', home_tempo: 'float', away_tempo: 'float', *, league: 'str' = 'mens') -> 'float'`

Expected total points from adjusted efficiencies and tempos.

Expected possessions are `home_tempo * away_tempo / avg_tempo`; each
side's expected points per 100 possessions blend its offense with the
opponent's defense (`0.5 * (off + opp_def)`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_o` | `float` |  | Home adjusted offensive efficiency (points / 100 poss). |
| `home_adj_d` | `float` |  | Home adjusted defensive efficiency. |
| `away_adj_o` | `float` |  | Away adjusted offensive efficiency. |
| `away_adj_d` | `float` |  | Away adjusted defensive efficiency. |
| `home_tempo` | `float` |  | Home adjusted tempo (possessions / game). |
| `away_tempo` | `float` |  | Away adjusted tempo. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the tempo anchor). |

**Returns**

Expected combined points scored by both teams.

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import predict_total
predict_total(110.0, 95.0, 105.0, 100.0, 68.0, 66.0)
```

### preseason_prior {#preseason_prior}

`preseason_prior(prev_eff: 'pl.DataFrame', teams: 'pl.DataFrame', prev_teams: 'pl.DataFrame', *, league: 'str' = 'mens', regression: 'float | None' = None, new_team_quantile: 'float | None' = None) -> 'pl.DataFrame'`

The preseason prior for one season: last season's final ratings, regressed to the mean.

Each Division I team's prior-season final AdjO / AdjD (`adjust_efficiency` over every
game of that season, no prior of its own) is taken as a deviation from that season's league
average and multiplied by the league's fitted `regression`. A team that was not
Division I last season (a program new to Division I, `new_team`) gets the
`new_team_quantile` of last season's Division I AdjEM, unregressed, split evenly between
offense and defense; so does a returning team with no rated game last season.
Both constants come from
`~sportsdataverse.mbb.mbb_prediction_constants.PRESEASON_PRIOR`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `prev_eff` | `DataFrame` |  | `raw_game_efficiency` over the prior season's played games. |
| `teams` | `DataFrame` |  | This season's Division I teams (`season`, `team_id`), e.g. `sportsdataverse.mbb.mbb_college_season.college_teams`. |
| `prev_teams` | `DataFrame` |  | The prior season's Division I teams. |
| `league` | `str` | `'mens'` | `"mens"` / `"womens"`. |
| `regression` | `float \| None` | `None` | Override the fitted regression factor (the fitting script scans it). |
| `new_team_quantile` | `float \| None` | `None` | Override the new-team quantile. |

**Returns**

One row per team in `teams` in `PRIOR_SCHEMA` (`new_team` flags the default). Empty when `prev_eff` is empty: no prior is then applied.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | The season the prior is for (named by its end year). |
| `team_id` | String | ESPN team id of a Division I team this season. |
| `off_dev` | Float64 | The prior's offense, points per 100 possessions above the prior season's league average; last season's final AdjO deviation times the regression factor, or half the new-team default for a program new to Division I. |
| `def_dev` | Float64 | The prior's defense, points per 100 allowed above the prior season's league average (lower is better); last season's final AdjD deviation times the regression factor, or minus half the new-team default. |
| `prior_avg` | Float64 | The prior season's league-average efficiency, the ratings' baseline before this season has a game. |
| `new_team` | Boolean | True when the team was not Division I last season and starts at the new-team quantile of last season's AdjEM. |

**Example**

```python
from sportsdataverse.mbb.mbb_team_ratings import preseason_prior, raw_game_efficiency
prior = preseason_prior(raw_game_efficiency(prev_sched, prev_box), teams, prev_teams)
```

### rank_corr {#rank_corr}

`rank_corr(a: 'np.ndarray', b: 'np.ndarray') -> 'float'`

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

### raw_game_efficiency {#raw_game_efficiency}

`raw_game_efficiency(schedule: 'pl.DataFrame', team_box: 'pl.DataFrame') -> 'pl.DataFrame'`

Per-team, per-game possessions + raw offensive/defensive efficiency.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedule` | `DataFrame` |  | Frame with `game_id, season, date, home_team_id, away_team_id, neutral_site` (ids as strings or ints; cast to `Utf8` here). |
| `team_box` | `DataFrame` |  | Per-team boxscore with `game_id, team_id, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted, team_score`. When it also carries `total_turnovers` / `team_turnovers` (the loaders do), a row whose `turnovers` and `total_turnovers` are both 0 takes its count from `team_turnovers` -- where ESPN's 2009-2012 women's box files it. |

**Returns**

One row per (game_id, team_id): `game_id, season, date, team_id, opp_team_id, is_home, neutral_site, poss, off_eff, def_eff`. Empty input returns that schema with zero rows. Team-game rows whose possession estimate is non-positive (an all-zero ESPN boxscore shell) are dropped with a `UserWarning` -- their efficiency is undefined, and one of them poisons the whole season's fixed point. Games in which either team still has 0 turnovers are dropped the same way: the possession estimate would miss its turnover term.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `season` | Int64 | Season year. |
| `date` | String | Date in YYYY-MM-DD format. |
| `team_id` | String | Unique team identifier. |
| `opp_team_id` | String |  |
| `is_home` | Boolean | Whether the team was home. |
| `neutral_site` | Boolean | Neutral site. |
| `poss` | Float64 | Poss. |
| `off_eff` | Float64 | Off eff. |
| `def_eff` | Float64 | Def eff. |

**Raises**

| Exception | Description |
|---|---|
| `InsufficientInputError` | When more than 10% of a season's games have 0 turnovers under every turnover key (ESPN's pre-2009 women's box), so no rating built from it would be meaningful. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_mbb_team_boxscore
from sportsdataverse.mbb.mbb_team_ratings import raw_game_efficiency
eff = raw_game_efficiency(load_mbb_schedule([2024]), load_mbb_team_boxscore([2024]))
```

### ridge_cv_lambda {#ridge_cv_lambda}

`ridge_cv_lambda(X: 'np.ndarray', y: 'np.ndarray', groups: 'np.ndarray', lams: "'list[float]'") -> 'float'`

Pick lambda by leave-one-group-out CV (groups = seasons/classes).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix. |
| `y` | `ndarray` |  | Targets. |
| `groups` | `ndarray` |  | Group label per row (e.g. season); each held out once. |
| `lams` | `list[float]` |  | Candidate penalties. |

**Returns**

The candidate with the lowest mean held-out MSE.

**Example**

```python
lam = ridge_cv_lambda(X, y, seasons, [0.1, 1, 10, 100])
```

### ridge_fit {#ridge_fit}

`ridge_fit(X: 'np.ndarray', y: 'np.ndarray', lam: 'float') -> 'np.ndarray'`

Closed-form ridge with an unpenalized intercept (coefficient 0).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix `(n, d)`. |
| `y` | `ndarray` |  | Targets `(n,)`. |
| `lam` | `float` |  | L2 penalty on the non-intercept coefficients. |

**Returns**

Coefficient vector of length `d + 1` (intercept first).

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_player_value_constants import ridge_fit
beta = ridge_fit(np.random.rand(50, 3), np.random.rand(50), lam=1.0)
```

### roc_auc {#roc_auc}

`roc_auc(y_true: 'np.ndarray', score: 'np.ndarray') -> 'float'`

Area under the ROC curve via the rank-sum (Mann-Whitney) identity.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Binary outcomes (0/1). |
| `score` | `ndarray` |  | Predicted scores (any monotone scale). |

**Returns**

AUC in `[0, 1]`; `nan` when only one class is present.

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_player_value_constants import roc_auc
roc_auc(np.array([0, 1]), np.array([0.2, 0.9]))
```

### save_artifact {#save_artifact}

`save_artifact(name: 'str', obj: 'dict') -> 'None'`

Write a bundled artifact (dev/fitter use -- writes into the source tree).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | Artifact stem, e.g. `"mbb_box_bpm"`. |
| `obj` | `dict` |  | JSON-serializable artifact payload. |

**Example**

```python
save_artifact("mbb_box_bpm", {"league": "mens", "coef": [0.1]})
```

### score_to_tuple {#score_to_tuple}

`score_to_tuple(s: 'str') -> 'tuple[int, int]'`

Parse a `"scored-allowed"` score string (`ExtractorUtils.score_to_tuple`).

Scala's `str match { case regex(s1, s2) => ... }` on a compiled
`Regex` requires the ENTIRE string to match (`Regex.unapplySeq` calls
`Matcher.matches()`, not `find()`) -- ported here as
`re.fullmatch`, not `re.match`/`re.search`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `s` | `str` |  | The raw score string, e.g. `"55-68"`. |

**Returns**

`(scored, allowed)` as a tuple of ints, or `(0, 0)` if `s` doesn't fully match `([0-9]+)-([0-9]+)`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import score_to_tuple

score_to_tuple("55-68")   # (55, 68)
score_to_tuple("garbage")  # (0, 0)
```

### simulate_game {#simulate_game}

`simulate_game(home_em: 'float', away_em: 'float', neutral: 'bool', rng: 'np.random.Generator', *, league: 'str' = 'mens') -> 'bool'`

Sample one game outcome: margin `~ Normal(exp_margin, margin_sd)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_em` | `float` |  | Home team's adjusted efficiency margin. |
| `away_em` | `float` |  | Away team's adjusted efficiency margin. |
| `neutral` | `bool` |  | True for a neutral-site game. |
| `rng` | `Generator` |  | A seeded `numpy.random.Generator` (caller owns determinism). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |

**Returns**

True if the home team wins the sampled game.

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_season_sim import simulate_game
simulate_game(20.0, 5.0, False, np.random.default_rng(0))
```

### slow_regression {#slow_regression}

`slow_regression(player_weight_matrix: 'NDArray[np.float64]', ridge_lambda: 'float', ctx: 'RapmPlayerContext') -> 'NDArray[np.float64]'`

Build the Tikhonov (ridge) regression solver matrix.

Faithful port of the private `RapmUtils.slowRegression`
(`RapmUtils.ts:756-769`): `(XᵀX + ridge_lambda·I)⁻¹Xᵀ`, where `X`
is `player_weight_matrix` (one row per lineup, one column per player --
see `calc_player_weights`). See the section banner above for why
this is a plain matrix inverse (`numpy.linalg.inv`), not an SVD.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_weight_matrix` | `NDArray[float64]` |  | The off/def design matrix, shape `(num_lineups, ctx["num_players"])`. |
| `ridge_lambda` | `float` |  | The Tikhonov regularization strength. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` -- only `ctx["num_players"]` is read (sizes the identity matrix). |

**Returns**

The `(num_players, num_lineups)` solver matrix; apply it to a target vector via `calculate_rapm`.

**Raises**

| Exception | Description |
|---|---|
| `numpy.linalg.LinAlgError` | If `XᵀX + ridge_lambda·I` is singular -- not reachable with `ridge_lambda > 0` (which always makes the matrix positive-definite), but possible with `ridge_lambda <= 0` on a rank-deficient `X`. |

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_rapm import slow_regression, calculate_rapm

x = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]])
solver = slow_regression(x, 1.0, ctx)  # ctx["num_players"] == 2
rapm = calculate_rapm(solver, [1.0, 2.0, 3.0])
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

### talent_split_mse {#talent_split_mse}

`talent_split_mse(scored: 'pl.DataFrame', *, k: 'float', seed: 'int' = 0) -> 'float'`

Weighted MSE of the k-regressed first half predicting the raw second half.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored` | `DataFrame` |  | `mbb_shot_quality` output. |
| `k` | `float` |  | Shrinkage pseudo-shots to evaluate. |
| `seed` | `int` | `0` | Split seed. |

**Returns**

`sum(n_h2 * (oe_h1 * n_h1/(n_h1+k) - oe_h2)^2) / sum(n_h2)`.

**Example**

```python
from sportsdataverse.mbb.mbb_shooter_talent import talent_split_mse
talent_split_mse(scored, k=200.0)
```

### transfer_cohort {#transfer_cohort}

`transfer_cohort(rosters: 'pl.DataFrame') -> 'pl.DataFrame'`

One row per transfer: same `player_id`, different `team_id` in

consecutive seasons.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rosters` | `DataFrame` |  | Frame with `player_id`, `team_id`, `season` (extra columns ignored; one row per player-season-team). |

**Returns**

`player_id: Utf8, from_team_id:Utf8, to_team_id:Utf8, from_season:Int64, to_season:Int64` -- a player transferring twice appears twice.

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `from_team_id` | String | Unique identifier for from team. |
| `to_team_id` | String | Unique identifier for to team. |
| `from_season` | Int64 |  |
| `to_season` | Int64 |  |

**Example**

```python
from sportsdataverse.mbb import mbb_box_bpm, transfer_cohort
bpm = mbb_box_bpm([2025, 2026]).filter(pl.col("min") >= 150)
moves = transfer_cohort(bpm.select("player_id", "team_id", "season"))
```

### win_prob_from_margin {#win_prob_from_margin}

`win_prob_from_margin(exp_margin: 'float', *, league: 'str' = 'mens') -> 'float'`

Home win probability from an expected margin (normal-CDF closed form).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home-minus-away margin in points. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the fitted margin sigma). |

**Returns**

Probability the home team wins, in `(0, 1)`.

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import win_prob_from_margin
win_prob_from_margin(5.0)
```
