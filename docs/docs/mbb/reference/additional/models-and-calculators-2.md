---
title: "Men's college basketball — additional Python functions — Models and calculators: build_player–mbb_transfer"
sidebar_label: "Models and calculators: build_player–mbb_transfer"
sidebar_position: 11
description: "Men's college basketball — additional Python functions — Models and calculators: build_player–mbb_transfer — function reference in sdv-py, the SportsDataverse Python package."
---
# Men's college basketball — additional Python functions — Models and calculators: build_player–mbb_transfer

### build_player_context {#build_player_context}

`build_player_context(players: 'list[PlayerOnOffStats]', lineups: 'list[LineupStatSet]', players_baseline: 'dict[PlayerId, IndivStatSet]', stats_averages: 'PureStatSet', avg_efficiency: 'float', agg_value_key: 'ValueKey' = 'value', config: 'RapmConfig' = {'prior_mode': -1, 'removal_pct': 0.06, 'fixed_regression': -1}) -> 'RapmPlayerContext'`

Build the context object the RAPM matrix-solve layer consumes.

Faithful port of `RapmUtils.buildPlayerContext` (`RapmUtils.ts:427-541`).
Removes low-possession players (`config["removal_pct"]` of total
on+off possessions), flags fully-removed lineups (mutating `lineups`
in place -- see the module docstring's landmine 5), builds the
player-to-column index, and folds `build_priors` into
`prior_info`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `list[PlayerOnOffStats]` |  | The per-player on/off splits (`PlayerOnOffStats`), e.g. `mbb_lineup_stats.lineup_to_team_report(...)["players"]`. |
| `lineups` | `list[LineupStatSet]` |  | The per-lineup `LineupStatSet` docs feeding this team's aggregate (**mutated in place** -- see landmine 5). |
| `players_baseline` | `dict[PlayerId, IndivStatSet]` |  | `{player_id: IndivStatSet}` -- forwarded to `build_priors` unchanged. |
| `stats_averages` | `PureStatSet` |  | League/context average stat set -- forwarded to `build_priors` unchanged. |
| `avg_efficiency` | `float` |  | League/context average efficiency. |
| `agg_value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- forwarded to `build_priors` as its `value_key` (only affects prior calculations, not the lineup-filtering/aggregation above it). |
| `config` | `RapmConfig` | `{'prior_mode': -1, 'removal_pct': 0.06, 'fixed_regression': -1}` | Removal-percent / prior-mode / regression config. Defaults to `DEFAULT_RAPM_CONFIG`; never mutated by this function (only `config["removal_pct"]`/`config["prior_mode"]` are read), matching the TS default parameter's own read-only usage. |

**Returns**

A `RapmPlayerContext`.

**Raises**

| Exception | Description |
|---|---|
| `ZeroDivisionError` | If `total_lineups` (derived from `players[0]`'s on/off possession totals) is exactly `0` -- see the module docstring's landmine 2. Not exercised by this task's oracle fixtures. |

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import lineup_to_team_report
from sportsdataverse.mbb.mbb_rapm import build_player_context, DEFAULT_RAPM_CONFIG

report = lineup_to_team_report({"lineups": buckets, "error_code": None})
ctx = build_player_context(
    report["players"], buckets, {}, {}, 100.0, "value", DEFAULT_RAPM_CONFIG
)
print(ctx["num_players"], ctx["team_info"]["off_poss"]["value"])

# Filtering lineups by side (the ``filtered_lineups`` closure)

off_lineups = ctx["filtered_lineups"]("off")
def_lineups = ctx["filtered_lineups"]("def")
```

### build_priors {#build_priors}

`build_priors(players_baseline: 'dict[PlayerId, IndivStatSet]', stats_averages: 'PureStatSet', avg_efficiency: 'float', col_to_player: 'list[str]', prior_mode: 'float', value_key: 'ValueKey' = 'value') -> 'RapmPriorInfo'`

Build strong/weak per-player RAPM priors for every column.

Faithful port of `RapmUtils.buildPriors` (`RapmUtils.ts:237-407`).
See the module docstring's landmine list, item 1, for the critical
Python-vs-JS `{}`-truthiness gotcha this function's implementation
deliberately avoids.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players_baseline` | `dict[PlayerId, IndivStatSet]` |  | `{player_id: IndivStatSet}` -- the most-general per-player baseline info (in production, sourced from `mbb_ratings.build_productivity`'s output; see the module docstring's "RAPM prior source" note). |
| `stats_averages` | `PureStatSet` |  | League/context average stat set, used by the (currently dead-code, see landmine 4) `get_prior_basis` fallback and by `with_avg_or_undef`'s nil-check gate. |
| `avg_efficiency` | `float` |  | League/context average efficiency. |
| `col_to_player` | `list[str]` |  | The player ids, in column order -- `playersStrong`/ `playersWeak` are index-aligned with this list. |
| `prior_mode` | `float` |  | `-1` for adaptive mode, `-2` (or lower) for no prior, `0`-`1` for a fixed strong-prior weight. |
| `value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- allows priors to be built from luck-adjusted parameters. |

**Returns**

A `RapmPriorInfo`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import build_priors

priors = build_priors({}, {}, 100.0, ["Wiggins, Aaron"], -1)
print(priors["players_weak"][0])
```

### build_productivity {#build_productivity}

`build_productivity(o_rtg: 'float', o_adj: 'float', usage: 'float', avg_efficiency: 'float') -> 'dict[str, float]'`

Public port of `RatingUtils.buildProductivity` (`RatingUtils.ts:963-990`).

Promoted to public -- see the module docstring's "Ported
behavior" section for the promotion rationale (RAPM needs to
import this across module boundaries).

Converts `ORtg` and a few other numbers into "productivity" using Dean
Oliver's PUE ("Player Usage Efficiency") formulation, SoS-adjusted via
`o_adj = avgEfficiency / Def_SOS`. **RAPM prior source:**
`Adj_ORtgPlus` is the value RAPM uses as an individual-offense prior.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `o_rtg` | `float` |  | The player's (possibly override-adjusted) `ORtg`. |
| `o_adj` | `float` |  | `avg_efficiency / Def_SOS` -- the strength-of-schedule adjustment factor. |
| `usage` | `float` |  | `100 * TotPoss / (Team_Poss or 1)` -- the player's possession-usage percentage. |
| `avg_efficiency` | `float` |  | The league/context average efficiency (`100` in every vendored jest call). |

**Returns**

`{"Adj_ORtg": float, "Adj_ORtgPlus": float, "Usage_Bonus": float, "SoS_Bonus": float}` -- keys kept TS-verbatim (see module docstring's naming-convention note).

### build_weak_prior_from_rapm {#build_weak_prior_from_rapm}

`build_weak_prior_from_rapm(rapm_results: 'list[float]', off_or_def: 'str') -> 'list[dict[str, float]]'`

Wrap a flat RAPM-estimate vector into `playersWeak`-shaped dicts.

Faithful port of `RapmUtils.buildWeakPriorFromRapm` (`RapmUtils.ts:410-419`),
used only by `pick_ridge_regression`'s `use_recursive_weak_prior`
branch to substitute the just-computed (pre-strong-prior) RAPM values as
the *weak* prior for a follow-up `apply_weak_priors` call -- "the
recursive prior" per the upstream `/** For "recursive" prior */` comment.

**Uncovered by the oracle** -- `semiRealRapmResults.testContext.priorInfo
.useRecursiveWeakPrior` is `false`, so `RapmUtils.test.ts`'s
`"pickRidgeRegression"` test never calls this function. Ported
faithfully from TS regardless (per "TS governs"); flagged as a documented
gap rather than backed by a synthetic test, matching this module's
existing convention for other upstream-untested branches.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rapm_results` | `list[float]` |  | A flat per-player RAPM estimate vector, e.g. `pick_ridge_regression`'s own `results_pre_prior`. |
| `off_or_def` | `str` |  | `"off"` or `"def"` -- selects the output key, `f"{off_or_def}_adj_ppp"`. |

**Returns**

One `{f"{off_or_def}_adj_ppp": rapm}` dict per input element, index-aligned with `rapm_results`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import build_weak_prior_from_rapm

weak_prior = build_weak_prior_from_rapm([5.0, 4.5], "off")
print(weak_prior[0])  # {"off_adj_ppp": 5.0}
```

### calc_collinearity_diag {#calc_collinearity_diag}

`calc_collinearity_diag(weight_matrix: 'NDArray[np.float64]', ctx: 'RapmPlayerContext') -> 'RapmPreProcDiagnostics'`

Multi-collinearity diagnostic between the players in an off/def design matrix.

Faithful port of `RapmUtils.calcCollinearityDiag` (`RapmUtils.ts:1629-1760`).
Runs an SVD of `weight_matrix`, builds condition indices ("lineup
combos") from the ratio of the largest to each singular value, and a
variance-decomposition-proportions ("VDP") matrix identifying which
players load onto which collinear combo -- the classic Belsley-Kuh-Welsch
collinearity-diagnostics recipe (see the upstream comment's
[colldiag.m](https://github.com/brian-lau/colldiag/blob/master/colldiag.m)
citation). Also builds a plain Pearson player/player correlation matrix
(`_calc_player_correlations`) and folds it into a possession
-weighted `adaptive_correl_weights` summary per player.

**`numpy.linalg.svd(weight_matrix, full_matrices=False)` replaces
`svd-js`'s `SVD(weightMatrix, false)`.** Both are the standard
Golub-Kahan-Reinsch decomposition (`A = U @ diag(S) @ Vᵀ`); numpy's
`Vh` return value already *is* `Vᵀ` (what the TS code separately
computes via `transpose(matrix(v))`), so this port skips that
transpose. The TS code (and this port) never reads `u`/the first SVD
return -- only `q`/`S` (singular values) and `v`/`Vᵀ`. Singular
-vector **sign is immaterial here**: every place `V` is used
(`phiMatrix`/`phi_matrix`) squares each entry (`val * val`), and a
per-singular-value sign flip on `U`/`V` together is a valid SVD
regardless -- so any `U`/`V` sign convention difference between
`svd-js` and LAPACK (numpy's backend) cannot change this function's
output. **Singular-value ordering is likewise immaterial**: both this
port and the TS source explicitly re-sort `q` (ascending, carrying the
original index along) before using it, so whichever order either SVD
implementation returns values in, the final result only depends on the
*values themselves* (up to the explicit resort), not on numpy's native
descending convention vs whatever order `svd-js` happens to return.

**`correl_matrix`/`poss_correl_matrix` stay `numpy.ndarray`** (see
the module docstring for why an `ndarray` here does not break deep `==`
comparisons).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `weight_matrix` | `NDArray[float64]` |  | An off/def design matrix, shape `(num_lineups, ctx["num_players"])` (e.g. `calc_player_weights`'s first return value, or a hand-built matrix for isolated testing). |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`. `ctx["num_players"]` sizes every per-player structure; `ctx["col_to_player"]` keys `player_combos`. |

**Returns**

A `RapmPreProcDiagnostics`.

**Raises**

| Exception | Description |
|---|---|
| `numpy.linalg.LinAlgError` | If the SVD fails to converge (not reachable via either oracle fixture). |

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calc_collinearity_diag, calc_player_weights

off_weights, _ = calc_player_weights(ctx)
diag = calc_collinearity_diag(off_weights, ctx)
print(diag["lineup_combos"][0])  # the worst-conditioned combo
```

### calc_lineup_outputs {#calc_lineup_outputs}

`calc_lineup_outputs(field: 'str', off_offset: 'float', def_offset: 'float', ctx: 'RapmPlayerContext', adaptive_correl_weights: 'list[float] | None' = None, use_old_val_if_possible: 'tuple[bool, bool]' = (False, False)) -> 'list[NDArray[np.float64]]'`

Build the off/def target vectors the RAPM design matrices are fit against.

Faithful port of `RapmUtils.calcLineupOutputs` (`RapmUtils.ts:598-751`).
For each filtered lineup, computes a possession-weighted residual: the
lineup's own stat value, plus any global luck adjustment, minus the
accumulated "prior offset" contributed by every player on the lineup
(a strong-prior blend for kept players -- see `_get_strong_weight`
-- or a fixed baseline contribution for removed players).

Upstream keeps this as a plain `Array<Array<number>>` (*not* a mathjs
`Matrix`, unlike `calc_player_weights`'s `offWeights`/
`defWeights` -- `RapmUtils.test.ts`'s own `tidyResults` helper for
this function has a visibly different shape, see the classification map
in `tests/fixtures/hoop_explorer/README.md`). This port still
materializes both output vectors as `numpy.ndarray` for consistency
with `calc_player_weights` at the same dict -> array boundary --
the ridge-regression solve consumes both as arrays regardless of
the upstream distinction.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `field` | `str` |  | The stat suffix to read off each lineup, e.g. `"adj_ppp"` (read as `{prefix}_{field}`, e.g. `"off_adj_ppp"`). |
| `off_offset` | `float` |  | The D1-average offensive value for `field` (the regression's starting/baseline value on the RHS). |
| `def_offset` | `float` |  | The D1-average defensive value for `field`. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`, e.g. from `build_player_context`. |
| `adaptive_correl_weights` | `list[float] \| None` | `None` | Optional per-player adaptive-correlation weights (index-aligned with `ctx["col_to_player"]`), used as the strong-prior blend fallback when `ctx["prior_info"] ["strong_weight"] < 0` -- see `_get_strong_weight`. |
| `use_old_val_if_possible` | `tuple[bool, bool]` | `(False, False)` | `(use_old_val_for_off, use_old_val_for_def)` -- whether to prefer each lineup/team stat's luck-adjusted `old_value` over its raw `value` when present. This is the luck-adjustment hook, an **inherited coverage gap**: the vendored oracle fixture has `old_value == value` on every field (via `insertOldValues`), so neither jest nor this port's replay test ever observes this flag change the resulting numbers -- only that passing it doesn't crash. See the module docstring's coverage-gap note. |

**Returns**

`[off_outputs, def_outputs]` -- two 1-D `numpy.ndarray` target vectors, index-aligned with `ctx["filtered_lineups"]("off"/"def")` (plus one extra element each when `ctx["unbias_weight"] > 0`, an "unbiasing observation" target -- always unreached in production, same as `calc_player_weights`'s extra row).

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calc_lineup_outputs

off_outputs, def_outputs = calc_lineup_outputs(
    "adj_ppp", 100.0, 100.0, ctx
)
print(off_outputs.shape)  # (num_off_lineups,)

# Luck-adjusted variant (reads ``old_value`` where present)

off_luck, def_luck = calc_lineup_outputs(
    "adj_ppp", 100.0, 100.0, ctx, use_old_val_if_possible=(True, True)
)
```

### calc_player_weights {#calc_player_weights}

`calc_player_weights(ctx: 'RapmPlayerContext') -> 'list[NDArray[np.float64]]'`

Build the off/def player-weight (design) matrices for the RAPM solve.

Faithful port of `RapmUtils.calcPlayerWeights` (`RapmUtils.ts:544-595`).
One row per (filtered) lineup, one column per remaining player; each
filled cell is `sqrt(lineup_possessions / total_side_possessions)` --
the possession-weighted design-matrix entry the ridge regression solves against. This is the first function in the module where a
`dict`-shaped `RapmPlayerContext` gets materialized into a
`numpy.ndarray` -- see the module docstring's "dict -> `numpy.ndarray`
boundary" note.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`, e.g. from `build_player_context`. |

**Returns**

`[off_weights, def_weights]` -- two `numpy.ndarray` matrices of shape `(num_{off,def}_lineups [+1 if ctx["unbias_weight"] > 0], ctx["num_players"])`. The optional extra row (only emitted when `ctx["unbias_weight"] > 0` -- always `0.0` in production per `build_player_context`'s hardcoded local, but settable directly on the returned context dict, as the oracle test does) holds each column's `unbias_weight`-scaled sum-of-squares, an "unbiasing observation" row (`RapmUtils.ts:578-593`).

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calc_player_weights

off_weights, def_weights = calc_player_weights(ctx)
print(off_weights.shape)  # (num_off_lineups, num_players)
```

### calc_slow_pseudo_inverse {#calc_slow_pseudo_inverse}

`calc_slow_pseudo_inverse(player_weight_matrix: 'NDArray[np.float64]', ridge_lambda: 'float', ctx: 'RapmPlayerContext') -> 'NDArray[np.float64]'`

Per-parameter variance terms for the ridge-regression standard errors.

Faithful port of the private `RapmUtils.calcSlowPseudoInverse`
(`RapmUtils.ts:1544-1557`): the same `(XᵀX + ridge_lambda·I)⁻¹` as
`slow_regression`'s `bottomInv`, but this function returns the
square root of its diagonal instead of the full solver matrix -- the
`paramErrs` term consumed by the standard-error formula (see
`calculate_sd_rapm`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_weight_matrix` | `NDArray[float64]` |  | The off/def design matrix, same shape as `slow_regression`'s. |
| `ridge_lambda` | `float` |  | The Tikhonov regularization strength (must match the `ridge_lambda` used to build the corresponding `slow_regression` solver, for the SEs to be meaningful). |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` -- only `ctx["num_players"]` is read. |

**Returns**

A length-`num_players` array, `sqrt(diag((XᵀX + λI)⁻¹))`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calc_slow_pseudo_inverse

param_errs = calc_slow_pseudo_inverse(x, 1.0, ctx)
```

### calculate_predicted_out {#calculate_predicted_out}

`calculate_predicted_out(player_weight_matrix: 'NDArray[np.float64]', regressed_players: 'list[float]', ctx: 'RapmPlayerContext') -> 'NDArray[np.float64]'`

Predict per-lineup outputs from fitted per-player RAPM values.

Faithful port of `RapmUtils.calculatePredictedOut` (`RapmUtils.ts:1559-1567`).
`ctx` is accepted for signature parity with the TS source but unused in
the body (ported verbatim -- upstream's own `ctx` param is likewise
dead in this function).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_weight_matrix` | `NDArray[float64]` |  | The off/def design matrix, shape `(num_lineups, num_players)`. |
| `regressed_players` | `list[float]` |  | The fitted per-player values (e.g. the final, strong-prior-blended RAPM from the `pickRidgeRegression`, or a raw `calculate_rapm` output), length `num_players`. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` (unused). |

**Returns**

The predicted per-lineup value, length `num_lineups` -- feed into `calculate_residual_error` alongside the actual lineup outputs.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calculate_predicted_out

predicted = calculate_predicted_out(x, [0.875, 1.375], ctx)
```

### calculate_rapm {#calculate_rapm}

`calculate_rapm(regression_matrix: 'NDArray[np.float64]', player_outputs: 'list[float]') -> 'NDArray[np.float64]'`

Apply a regression solver matrix to a target-outputs vector.

Faithful port of `RapmUtils.calculateRapm` (`RapmUtils.ts:772-775`).
Note the TS signature carries no `ctx` parameter (unlike its solve-layer
siblings) -- ported verbatim, param-for-param.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `regression_matrix` | `NDArray[float64]` |  | The `(num_players, num_lineups)` solver from `slow_regression`. |
| `player_outputs` | `list[float]` |  | The per-lineup target vector, length `num_lineups` (e.g. `calc_lineup_outputs`'s `off_outputs`/`def_outputs`). |

**Returns**

The per-player RAPM estimate, length `num_players`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calculate_rapm

rapm = calculate_rapm(solver, [1.0, 2.0, 3.0])
print(rapm.shape)  # (num_players,)
```

### calculate_residual_error {#calculate_residual_error}

`calculate_residual_error(player_outs: 'list[float]', regressed_outs: 'list[float]', ctx: 'RapmPlayerContext') -> 'float'`

Sum of squared residuals between actual and predicted lineup outputs.

Faithful port of `RapmUtils.calculateResidualError` (`RapmUtils.ts:1569-1579`).
`ctx` is accepted for signature parity but unused in the body (dead
upstream too).

**NaN/shape regime (landmine 7):** TS zips the two arrays via lodash
.zip` (pads the shorter side with `undefined`, so a length
mismatch silently contributes `NaN` to the running sum via
`undefined - number`) then reduces with plain `+`. This port instead
subtracts the two as `numpy` arrays: a length mismatch **raises**
`ValueError` (numpy broadcast rules), rather than the TS silent-NaN
behavior -- not reachable via either language's own call sites (both
arguments are always index-aligned to the same lineup count in
production), so this is a divergence in dead territory, not a fixed bug.
A `NaN` *value already present* inside either input (as opposed to a
length mismatch) propagates through the `numpy` subtraction/sum
exactly as it would through the JS arithmetic (both regimes:
numpy-propagate).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_outs` | `list[float]` |  | The actual per-lineup target values (e.g. `calc_lineup_outputs`'s output). |
| `regressed_outs` | `list[float]` |  | The predicted per-lineup values (e.g. `calculate_predicted_out`'s output). |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` (unused). |

**Returns**

`sum((player_outs[i] - regressed_outs[i]) ** 2)` -- the `errSq` term consumed by `calculate_sd_rapm`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `player_outs` and `regressed_outs` have different lengths -- see landmine 7 above. |

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calculate_residual_error

err_sq = calculate_residual_error([1.0, 2.0, 3.0], [0.875, 1.375, 2.25], ctx)
```

### calculate_sd_rapm {#calculate_sd_rapm}

`calculate_sd_rapm(param_errs: 'NDArray[np.float64]', err_sq: 'float', num_lineups: 'int', num_players: 'int') -> 'NDArray[np.float64]'`

Per-player RAPM standard errors.

Faithful port of the inline `sdRapm` computation in
`RapmUtils.pickRidgeRegression` (`RapmUtils.ts:1373-1390`, not itself
a named TS function -- promoted to a standalone, independently testable
helper here since the brief calls out the formula explicitly).
Cites [arXiv:1509.09169](https://arxiv.org/pdf/1509.09169.pdf).

**Two NaN/error regimes (landmines 8-9):**

8. `dof_inv = 1.0 / (num_lineups - num_players)` -- if
   `num_lineups == num_players` exactly, JS silently produces
   `Infinity` (float division by zero); this port instead **raises**
   `ZeroDivisionError` (Python float division by zero), matching this
   module's already-established landmine-2 convention (unguarded
   division, Python-raises vs JS-Infinity/NaN). Not reachable via the
   oracle fixtures (`num_off_lineups`/`num_def_lineups` always
   comfortably exceed `num_players` there).
9. `sqrt(sqrt(param_errs) * err_sq * dof_inv)` -- a negative
   `param_errs` entry (only possible if `XᵀX + λI` isn't actually
   positive-definite, e.g. `ridge_lambda < 0`) silently
   **numpy-propagates** to `NaN` (matching JS `Math.sqrt(negative)
   -> NaN`, with a `RuntimeWarning` rather than a raise) -- both
   language regimes agree here, unlike landmine 8.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `param_errs` | `NDArray[float64]` |  | Per-player variance terms from `calc_slow_pseudo_inverse`, length `num_players`. |
| `err_sq` | `float` |  | The residual sum of squares from `calculate_residual_error`. |
| `num_lineups` | `int` |  | `ctx["num_off_lineups"]` or `ctx["num_def_lineups"]` (whichever side `param_errs`/`err_sq` were computed for). |
| `num_players` | `int` |  | `ctx["num_players"]`. |

**Returns**

A length-`num_players` array of per-player RAPM standard errors.

**Raises**

| Exception | Description |
|---|---|
| `ZeroDivisionError` | If `num_lineups == num_players` -- see landmine 8 above. |

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calculate_sd_rapm

sd_rapm = calculate_sd_rapm(param_errs, err_sq, num_lineups=3, num_players=2)
```

### calibration_table {#calibration_table}

`calibration_table(y_true: 'np.ndarray', p_pred: 'np.ndarray', n_bins: 'int' = 10) -> 'pl.DataFrame'`

Bucket predicted probabilities into bins and compare to actual outcome rates.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |
| `n_bins` | `int` | `10` | Number of equal-width probability bins. |

**Returns**

A `polars.DataFrame` with columns `bin_mid`, `mean_pred`, `mean_actual`, `n` (one row per non-empty bin).

| col_name | type | description |
|---|---|---|
| `bin_mid` | Float64 |  |
| `mean_pred` | Float64 |  |
| `mean_actual` | Float64 |  |
| `n` | Int64 |  |

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import calibration_table
calibration_table(np.array([1, 0, 1, 0]), np.array([0.9, 0.1, 0.8, 0.2]))
```

### fit_shrinkage_k {#fit_shrinkage_k}

`fit_shrinkage_k(scored: 'pl.DataFrame', *, seed: 'int' = 0) -> 'float'`

Fit the talent shrinkage `k` split-half (see module docstring).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored` | `DataFrame` |  | `mbb_shot_quality` output. |
| `seed` | `int` | `0` | Split seed (deterministic fit). |

**Returns**

The `k` in `[1, 5000]` minimizing `talent_split_mse`.

**Example**

```python
from sportsdataverse.mbb.mbb_shooter_talent import fit_shrinkage_k
k = fit_shrinkage_k(scored)
```

### get_constants {#get_constants}

`get_constants(league: 'str') -> 'LeagueConstants'`

Return the `LeagueConstants` for a league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | Either `"mens"` or `"womens"`. |

**Returns**

The league's `LeagueConstants`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not a known key of `LEAGUE_CONSTANTS`. |

**Example**

```python
from sportsdataverse.mbb.mbb_prediction_constants import get_constants
get_constants("mens").hfa
```

### get_player_value_constants {#get_player_value_constants}

`get_player_value_constants(league: 'str') -> 'PlayerValueConstants'`

Return the `PlayerValueConstants` for a league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | `"mens"` or `"womens"`. |

**Returns**

The league's `PlayerValueConstants`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not a known key. |

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import get_player_value_constants
get_player_value_constants("mens").bundle_prefix
```

### in_game_features {#in_game_features}

`in_game_features(pbp: 'pl.DataFrame', pregame_home_prob: 'float') -> 'pl.DataFrame'`

Per-play in-game win-probability features from a `load_mbb_pbp` frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame with `start_game_seconds_remaining`, `home_score`, `away_score`, `team_id` (event team) and `home_team_id` (the `load_mbb_pbp` schema). |
| `pregame_home_prob` | `float` |  | The pregame home win probability (e.g. from `win_prob_from_margin`), encoded as a constant logit column. Clipped to `[1e-6, 1 - 1e-6]` so a saturated CDF (exact 0/1) cannot crash the logit. |

**Returns**

One row per input play: `score_diff` (home - away), `sec_left` (clipped at 0 -- overtime plays count as 0 seconds left; the game page gives overtime plays the period clock first), `sqrt_sec_left`, `pregame_logit`, `home_has_ball` (`Int8`; dead-ball / unknown-team plays are 0), `margin_per_sqrt_sec` (`score_diff / sqrt(sec_left + 1)`) and `sqrt_sec_left_60` (`sqrt(max(sec_left, 60))`). The same features as the NBA / WNBA model (`sportsdataverse.nba.nba_game_predict.in_game_features`).

| col_name | type | description |
|---|---|---|
| `score_diff` | Float64 |  |
| `sec_left` | Float64 |  |
| `sqrt_sec_left` | Float64 |  |
| `pregame_logit` | Float64 |  |
| `home_has_ball` | Int64 |  |
| `margin_per_sqrt_sec` | Float64 | Score margin over the time left, score_diff / sqrt(sec_left + 1): a fixed lead grows as the clock runs down. The basketball in-game boosters read the final minute only through this column. |
| `sqrt_sec_left_60` | Float64 | Square root of the seconds left, floored at one minute (sqrt(max(sec_left, 60))): the clock the basketball in-game boosters read outside the final minute. |

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import in_game_features
from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
pbp = load_mbb_pbp([2024]).filter(pl.col("game_id") == 401638643)
feats = in_game_features(pbp, 0.62)
```

### inject_rapm_into_players {#inject_rapm_into_players}

`inject_rapm_into_players(players: 'list[PlayerOnOffStats]', off_rapm_input: 'RapmProcessingInputs', def_rapm_input: 'RapmProcessingInputs', stats_averages: 'PureStatSet', ctx: 'RapmPlayerContext', adaptive_correl_weights: 'list[float] | None', read_value_keys: 'tuple[ValueKey, ValueKey]' = ('value', 'value'), write_value_key: 'ValueKey' = 'value') -> 'None'`

Write `pick_ridge_regression`'s RAPM predictions back onto each player.

Faithful port of `RapmUtils.injectRapmIntoPlayers` (`RapmUtils.ts:781-916`).
For every `onOffReportReplacement` field (minus the possession/title/
separator/`adj_opp` housekeeping keys -- see landmine 11 for the exact,
faithfully-ported omit-key quirk), re-derives that field's off/def target
vectors via `calc_lineup_outputs`, applies each side's
`calculate_rapm` solver, blends in the strong prior (mirroring
`pick_ridge_regression`'s own blend, except for `adj_ppp` which
reuses `off_rapm_input["rapm_adj_ppp"]`/`def_rapm_input["rapm_adj_ppp"]`
directly rather than recomputing), then writes `{playerId}.rapm[field]
= {write_value_key: result, "override": ...}` onto every player not in
`ctx["removed_players"]`.

**NOTE (upstream comment, verbatim): when `write_value_key ==
"old_value"`, this must be called *after* an initial `write_value_key
== "value"` call on the same `players` list** -- the `old_value`
pass .merge`s (`_lodash_merge`) its results into each player's
*existing* `rapm` dict rather than replacing it, so a player's
`rapm["field"]` ends up carrying both a `value` (from the first
call) and an `old_value` (from the second) side by side.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `list[PlayerOnOffStats]` |  | The players to write RAPM results onto (mutated in place -- each qualifying player gets a `"rapm"` key set/merged). |
| `off_rapm_input` | `RapmProcessingInputs` |  | `pick_ridge_regression`'s offensive output. |
| `def_rapm_input` | `RapmProcessingInputs` |  | `pick_ridge_regression`'s defensive output. |
| `stats_averages` | `PureStatSet` |  | League/context average stat set -- consulted for each field's off/def offset before `ctx["team_info"]`. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` (the same one `pick_ridge_regression` was called with). |
| `adaptive_correl_weights` | `list[float] \| None` |  | Optional per-player adaptive-correlation weights, forwarded to `calc_lineup_outputs` / `_get_strong_weight` exactly as `pick_ridge_regression` does. |
| `read_value_keys` | `tuple[ValueKey, ValueKey]` | `('value', 'value')` | `(off_key, def_key)` -- which key (`"value"`/`"old_value"`) to prefer when reading `stats_averages`/`ctx["team_info"]` offsets and when calling `calc_lineup_outputs` (forwarded as its `use_old_val_if_possible` flag). |
| `write_value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- which key each written field carries its result under. |

**Raises**

| Exception | Description |
|---|---|
| `TypeError` | If a player in `players` is absent from both `ctx["removed_players"]` and `ctx["player_to_col"]` -- see landmine 12 (a Python-vs-JS divergence in genuinely dead territory for the oracle). |

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import inject_rapm_into_players

inject_rapm_into_players(players, off_results, def_results, {}, ctx, None)
print(players[0]["rapm"]["off_adj_ppp"])  # {"value": ..., "override": None}

# Luck-adjusted two-call sequence (``"value"`` first, THEN ``"old_value"``)

inject_rapm_into_players(
    players, off_results, def_results, {}, ctx, None, ("value", "old_value"), "value"
)
inject_rapm_into_players(
    players, off_results, def_results, {}, ctx, None, ("old_value", "old_value"), "old_value"
)
```

### kmeans_fit {#kmeans_fit}

`kmeans_fit(X: 'np.ndarray', k: 'int', seed: 'int', n_init: 'int' = 10, max_iter: 'int' = 100) -> "'tuple[np.ndarray, np.ndarray]'"`

Seeded Lloyd's KMeans, best-of-`n_init` by inertia.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix `(n, d)` (standardize first). |
| `k` | `int` |  | Number of clusters. |
| `seed` | `int` |  | RNG seed (deterministic output). |
| `n_init` | `int` | `10` | Independent restarts. |
| `max_iter` | `int` | `100` | Lloyd iterations per restart. |

**Returns**

`(centers[k, d], labels[n])`.

**Example**

```python
centers, labels = kmeans_fit(Z, k=8, seed=0)
```

### load_artifact {#load_artifact}

`load_artifact(name: 'str') -> 'dict'`

Read a bundled player-value artifact (`mbb/models/<name>.json`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | Artifact stem, e.g. `"mbb_box_bpm"`. |

**Returns**

The parsed JSON dict.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import load_artifact
art = load_artifact("mbb_box_bpm")
```

### log_loss_score {#log_loss_score}

`log_loss_score(y_true: 'np.ndarray', p_pred: 'np.ndarray', eps: 'float' = 1e-15) -> 'float'`

Binary cross-entropy loss between predicted probabilities and outcomes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |
| `eps` | `float` | `1e-15` | Clipping bound to avoid `log(0)`. |

**Returns**

The mean log loss.

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import log_loss_score
log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
```

### logistic_fit {#logistic_fit}

`logistic_fit(X: 'np.ndarray', y: 'np.ndarray', lam: 'float' = 1.0) -> 'np.ndarray'`

L2-penalized logistic regression via L-BFGS (intercept unpenalized).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix `(n, d)`. |
| `y` | `ndarray` |  | Binary outcomes (0/1). |
| `lam` | `float` | `1.0` | L2 penalty on the non-intercept coefficients. |

**Returns**

Coefficient vector of length `d + 1` (intercept first).

**Example**

```python
coef = logistic_fit(X, drafted, lam=1.0)
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

### mbb_box_bpm {#mbb_box_bpm}

`mbb_box_bpm(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-player-season box Plus/Minus (offense, defense, total).

Aggregates the season's player boxscores, scores the per-100 features
through the bundled team-constrained coefficients, and applies the BPM
team adjustment so each team's minutes-weighted player scores sum to its
adjusted efficiency margin (points per 100 possessions above league
average; positive = good on both ends).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2025`) or list of seasons. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (player_id, season, team_id): `player_id:Utf8, player, season, team_id:Utf8, min, box_obpm, box_dbpm, box_bpm`. Empty input returns the schema with zero rows.

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `player` | String | Player name. |
| `season` | Int64 | Season year. |
| `team_id` | String | Unique team identifier. |
| `min` | Float64 | Minutes played. |
| `box_obpm` | Float64 |  |
| `box_dbpm` | Float64 |  |
| `box_bpm` | Float64 |  |

**Example**

```python
from sportsdataverse.mbb import mbb_box_bpm
bpm = mbb_box_bpm(2025)

# Pipeline next step (one line)

bpm.filter(pl.col("min") >= 400).sort("box_bpm", descending=True).head(15)
```

### mbb_bracket_sim {#mbb_bracket_sim}

`mbb_bracket_sim(seeded_field: 'pl.DataFrame', ratings: 'pl.DataFrame', *, n_sims: 'int' = 10000, seed: 'int' = 0, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Single-elimination Monte Carlo over a bracket-ordered field.

Rows of `seeded_field` are bracket slots: adjacent rows meet in round 1
and winners of adjacent games meet next round (the standard fold). All
games are neutral-site. Round columns are named from the END of a 64-team
bracket (`champion` back to `reach_r32`); with a smaller field the
early columns are 1.0 for everyone (trivially reached).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seeded_field` | `DataFrame` |  | Bracket-ordered rows with `team_id` (and typically `seed` for reference). |
| `ratings` | `DataFrame` |  | One row per team: `team_id, adj_em`. |
| `n_sims` | `int` | `10000` | Number of simulated brackets. |
| `seed` | `int` | `0` | Seed for `numpy.random.default_rng`. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per field team: `team_id, seed?, reach_r32, reach_s16, reach_e8, reach_f4, reach_final, champion` (probabilities).

No returns table is published for this function: no capture: it needs a bracket-ordered field whose size is a power of two, and no package function returns one.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the field size is not a power of two. |
| `KeyError` | If a field team is missing from `ratings`. |

**Example**

```python
from sportsdataverse.mbb.mbb_season_sim import mbb_bracket_sim
odds = mbb_bracket_sim(field_64, ratings, n_sims=20000, seed=42)
```

### mbb_draft_projection {#mbb_draft_projection}

`mbb_draft_projection(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Draft probability, projected pick, and pro tier per player-season.

`draft_prob` is the probability of being selected in the draft
immediately following the college season; `projected_pick` is the
expected overall pick conditional on being drafted (lower = better);
`pro_tier` buckets the pick through the bundled tier edges.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2025`, feeding the June 2025 draft) or list of seasons. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact; womens = WNBA draft). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per qualifying player-season: `player_id:Utf8, player, season, team_id:Utf8, draft_prob, projected_pick, pro_tier`. Empty input returns the schema with zero rows.

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `player` | String | Player name. |
| `season` | Int64 | Season year. |
| `team_id` | String | Unique team identifier. |
| `draft_prob` | Float64 |  |
| `projected_pick` | Float64 |  |
| `pro_tier` | String |  |

**Example**

```python
from sportsdataverse.mbb import mbb_draft_projection
board = mbb_draft_projection(2025)

# Pipeline next step (one line)

board.sort("draft_prob", descending=True).head(30)
```

### mbb_in_game_win_prob {#mbb_in_game_win_prob}

`mbb_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-play home win probability from the bundled in-game model.

Scores `in_game_features` through the committed artifact
(`sportsdataverse/mbb/models/mbb_in_game_wp.ubj` men's,
`wbb_in_game_wp.ubj` women's): a shallow monotone-constrained xgboost
booster fit by `tools/models/mbb_prediction/train_in_game_wp.py` on
2022, 2023 and 2025, held out on 2026 (2024, the pregame and decile gate
season, is in neither). It is the NBA / WNBA model's contract: within the
final minute the clock reaches the model only through
`margin_per_sqrt_sec`, so at a fixed lead the probability moves toward
the leader as time runs out. This is the raw model output: it never
applies the game result (`nba_game_page.score_win_prob` settles a
finished game's last play).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play for ONE game in the `load_mbb_pbp` schema (`start_game_seconds_remaining`, `home_score`, `away_score`, `team_id`, `home_team_id`). |
| `pregame_home_prob` | `float` |  | Pregame home win probability (e.g. from `win_prob_from_margin`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per play: the `in_game_features` columns plus `home_win_prob`.

| col_name | type | description |
|---|---|---|
| `score_diff` | Float64 |  |
| `sec_left` | Float64 |  |
| `sqrt_sec_left` | Float64 |  |
| `pregame_logit` | Float64 |  |
| `home_has_ball` | Int64 |  |
| `margin_per_sqrt_sec` | Float64 | Score margin over the time left, score_diff / sqrt(sec_left + 1): a fixed lead grows as the clock runs down. The basketball in-game boosters read the final minute only through this column. |
| `sqrt_sec_left_60` | Float64 | Square root of the seconds left, floored at one minute (sqrt(max(sec_left, 60))): the clock the basketball in-game boosters read outside the final minute. |
| `home_win_prob` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | If the bundled artifact is an xgboost tree (`.ubj`) and `xgboost` is not installed (`pip install sportsdataverse[models]`). |

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import mbb_in_game_win_prob
from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
pbp = load_mbb_pbp([2024]).filter(pl.col("game_id") == 401638643)
wp = mbb_in_game_win_prob(pbp, 0.62)
```

### mbb_predict_games {#mbb_predict_games}

`mbb_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Vectorized pregame predictions for a schedule of games.

Joins the ratings frame twice (home / away) and applies the closed-form
`predict_margin` / `win_prob_from_margin` /
`predict_total` math column-wise.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game with `game_id`, `home_team_id`, `away_team_id` and optionally `neutral_site` (missing column means every game is a true home game). Team-id dtypes must match `ratings['team_id']` exactly. |
| `ratings` | `DataFrame` |  | One row per team with `team_id, adj_o, adj_d, adj_em, adj_tempo` (the `mbb_team_ratings` output for one season / as-of date). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per input game: `game_id, home_team_id, away_team_id, exp_margin, home_win_prob, exp_total`. Games whose teams are missing from `ratings` carry nulls.

No returns table is published for this function: no capture: it needs a schedule with home_team_id / away_team_id columns, and no package function returns one (load_mbb_schedule ships home_id / away_id).

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a join-key dtype mismatches `ratings['team_id']`, or if `ratings` has duplicate `team_id` rows (e.g. multiple seasons passed at once). |

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import mbb_predict_games
from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings
preds = mbb_predict_games(games, mbb_team_ratings([2024]))
```

### mbb_recruiting_projection {#mbb_recruiting_projection}

`mbb_recruiting_projection(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Expected freshman box-BPM per recruit + over/under-performance residual.

Scores each recruit of the season's incoming class through the bundled
recruiting ridge (composite grade + log national rank; missing values
imputed with the class median / the bubble rank). When the freshman
season is already observable, `resume_residual = realized box_bpm -
exp_box_bpm` (null otherwise, and `player_id` carries the matched
college athlete id).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | Freshman college season(s) (e.g. `2025` = the class arriving for 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per recruit: `recruit_id:Utf8, player_id:Utf8 (nullable), player, season, team_id:Utf8, composite, rank_nat, exp_box_bpm, resume_residual`. Empty input returns the schema with zero rows.

| col_name | type | description |
|---|---|---|
| `recruit_id` | String |  |
| `player_id` | String | Unique player identifier. |
| `player` | String | Player name. |
| `season` | Int64 | Season year. |
| `team_id` | String | Unique team identifier. |
| `composite` | Float64 |  |
| `rank_nat` | Int64 |  |
| `exp_box_bpm` | Float64 |  |
| `resume_residual` | Float64 |  |

**Example**

```python
from sportsdataverse.mbb import mbb_recruiting_projection
proj = mbb_recruiting_projection(2026)

# Pipeline next step (one line)

proj.sort("exp_box_bpm", descending=True).head(15)
```

### mbb_season_projections {#mbb_season_projections}

`mbb_season_projections(season: 'int', *, as_of_date: 'datetime.date | None' = None, n_sims: 'int' = 10000, seed: 'int' = 0, league: 'str' = 'mens', ineligible: 'Iterable[str]' = (), return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The daily `season_sim` table: projected W-L, conference standings, title and bid odds.

Loads the released ESPN schedule and team box, rates every team on the games strictly
before `as_of_date`, and runs
`~sportsdataverse.mbb.mbb_college_season.simulate_season`: the remaining regular
season, the regular-season conference titles, every unfinished conference tournament
(the auto-bid), and the 68-team field and its seeds, `n_sims` times from
`numpy.random.default_rng(seed)`. The same arguments give the same table.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season to project (e.g. `2024`). |
| `as_of_date` | `date \| None` | `None` | Only use games strictly before this date; `None` uses every completed game (today's table). |
| `n_sims` | `int` | `10000` | Simulated seasons. |
| `seed` | `int` | `0` | RNG seed. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `ineligible` | `Iterable[str]` | `()` | Team ids barred from the NCAA tournament (no bid; see `~sportsdataverse.mbb.mbb_bracketology.mbb_bracketology`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per Division I team: current and projected overall and conference W-L, the projected conference standing (`proj_conf_win_pct_rank`, 1 best within the conference), `reg_season_title_prob` (co-champions count), `conf_tourney_title_prob`, `auto_bid_prob` (the champion's, or the runner-up's when the champion is ineligible), `at_large_prob`, `bid_prob`, `seed_mean` (over sims with a bid) and `seed_1_prob` .. `seed_16_prob`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by its end year (2024 is 2023-24). |
| `as_of_date` | Date | The as-of date: only games strictly before it count as played. Null when every completed game is used. |
| `team_id` | String | ESPN team id (Utf8). |
| `team` | String | Team (ESPN location name). |
| `conference_id` | String | ESPN conference id, read off the schedule (Utf8). |
| `conference` | String | Conference short name. Null for independents, which get no auto-bid. |
| `wins` | Int64 | Wins as of the date (every opponent). |
| `losses` | Int64 | Losses as of the date. |
| `conf_wins` | Int64 | Regular-season conference wins as of the date. |
| `conf_losses` | Int64 | Regular-season conference losses as of the date. |
| `proj_wins` | Float64 | Projected wins: played wins plus the mean simulated wins in the remaining regular-season games (conference tournaments not counted). |
| `proj_losses` | Float64 | Projected losses, on the same games. |
| `proj_conf_wins` | Float64 | Projected regular-season conference wins. |
| `proj_conf_losses` | Float64 | Projected regular-season conference losses. |
| `proj_conf_win_pct` | Float64 | proj_conf_wins / (proj_conf_wins + proj_conf_losses). |
| `proj_conf_win_pct_rank` | Float64 | Projected conference standing: rank of proj_conf_win_pct within the conference, 1 is best, average ties. Null for independents. |
| `proj_conf_win_pct_pct` | Float64 | Percentile of proj_conf_win_pct within the conference. |
| `reg_season_title_prob` | Float64 | Share of sims with the conference's best regular-season conference winning percentage; co-champions all count, so a conference can sum past 1. |
| `conf_tourney_title_prob` | Float64 | Share of sims winning the conference tournament (a played final is certain). Ineligible teams play it too, so this can differ from auto_bid_prob. |
| `auto_bid_prob` | Float64 | Share of sims holding the conference's auto-bid: the tournament champion, or the runner-up when the champion is ineligible. |
| `at_large_prob` | Float64 | Share of sims with an at-large bid. |
| `bid_prob` | Float64 | Share of sims in the 68-team field (auto_bid_prob + at_large_prob). |
| `seed_mean` | Float64 | Mean seed line over the sims with a bid; null when never in the field. |
| `seed_1_prob` | Float64 | Share of all sims with a 1 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_2_prob` | Float64 | Share of all sims with a 2 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_3_prob` | Float64 | Share of all sims with a 3 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_4_prob` | Float64 | Share of all sims with a 4 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_5_prob` | Float64 | Share of all sims with a 5 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_6_prob` | Float64 | Share of all sims with a 6 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_7_prob` | Float64 | Share of all sims with a 7 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_8_prob` | Float64 | Share of all sims with a 8 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_9_prob` | Float64 | Share of all sims with a 9 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_10_prob` | Float64 | Share of all sims with a 10 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_11_prob` | Float64 | Share of all sims with a 11 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_12_prob` | Float64 | Share of all sims with a 12 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_13_prob` | Float64 | Share of all sims with a 13 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_14_prob` | Float64 | Share of all sims with a 14 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_15_prob` | Float64 | Share of all sims with a 15 seed (unconditional; the 16 lines sum to bid_prob). |
| `seed_16_prob` | Float64 | Share of all sims with a 16 seed (unconditional; the 16 lines sum to bid_prob). |
| `n_sims` | Int64 | Simulated seasons. |

**Example**

```python
import datetime
from sportsdataverse.mbb import mbb_season_projections
odds = mbb_season_projections(2024, as_of_date=datetime.date(2024, 2, 1), n_sims=2000)

# Pipeline next step (one line)

odds.sort("bid_prob", descending=True).head(68)
```

### mbb_season_sim {#mbb_season_sim}

`mbb_season_sim(ratings: 'pl.DataFrame', remaining_schedule: 'pl.DataFrame', *, n_sims: 'int' = 10000, seed: 'int' = 0, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Monte Carlo the remaining schedule: expected wins + title odds.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | One row per team: `season, team_id, adj_em` and optionally `conference` (enables `conf_title_prob`) and `current_wins` (added to the simulated remaining wins). |
| `remaining_schedule` | `DataFrame` |  | Games to simulate: `home_team_id, away_team_id, neutral_site`. |
| `n_sims` | `int` | `10000` | Number of simulated seasons. |
| `seed` | `int` | `0` | Seed for `numpy.random.default_rng` (deterministic output). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `season, team_id, exp_wins` (mean simulated total wins), `playoff_prob` (share of sims finishing in the top 68 win totals -- a field-size proxy, ties broken by `adj_em`) and `conf_title_prob` (share of sims with the most wins among conference members; ties count for every tied team; null without a `conference` column).

No returns table is published for this function: no capture: it needs a schedule with home_team_id / away_team_id columns, and no package function returns one (load_mbb_schedule ships home_id / away_id).

**Raises**

| Exception | Description |
|---|---|
| `KeyError` | If a scheduled team is missing from `ratings` -- every `home_team_id` / `away_team_id` must have a ratings row. |

**Example**

```python
from sportsdataverse.mbb.mbb_season_sim import mbb_season_sim
odds = mbb_season_sim(ratings, remaining, n_sims=5000, seed=42)
```

### mbb_shooter_talent {#mbb_shooter_talent}

`mbb_shooter_talent(scored: 'pl.DataFrame', *, league: 'str' = 'mens', k: "'float | None'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-shooter EB-regressed make% over expected + points over expected.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored` | `DataFrame` |  | `mbb_shot_quality` output (needs `shooter_id, made, point_value, xmake, xpoints`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (default `k` source). |
| `k` | `float \| None` | `None` | Shrinkage pseudo-shots; `None` uses `get_constants(league).shrink_k_talent` (fitted split-half). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per shooter: `shooter_id:Utf8, n_shots, make_rate, xmake_mean, oe_pct, oe_pct_regressed, points_over_expected, poe_per_100`. Empty input returns the zero-row schema.

| col_name | type | description |
|---|---|---|
| `shooter_id` | String | Unique identifier for shooter. |
| `n_shots` | Int64 |  |
| `make_rate` | Float64 |  |
| `xmake_mean` | Float64 |  |
| `oe_pct` | Float64 |  |
| `oe_pct_regressed` | Float64 |  |
| `points_over_expected` | Float64 |  |
| `poe_per_100` | Float64 |  |

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality, mbb_shooter_talent
talent = mbb_shooter_talent(mbb_shot_quality(mbb_shot_data(2025)))

# Pipeline next step (one line)

talent.filter(pl.col("n_shots") >= 200).sort("oe_pct_regressed", descending=True).head(15)
```

### mbb_shot_quality {#mbb_shot_quality}

`mbb_shot_quality(shots: 'pl.DataFrame', *, model: "'pl.DataFrame | None'" = None, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Score each shot with `xmake` / `xpoints` from the cell table.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots` | `DataFrame` |  | Canonical shot frame. |
| `model` | `DataFrame \| None` | `None` | A `mbb_shot_quality_model` table. When `None` it is built from `shots` itself -- convenient, but leakage-safe evaluation should pass a model fit on PRIOR data. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`shots`'s columns plus `xmake:Float64, xpoints:Float64` (null for cells absent from the model). Empty input returns the input schema plus the two columns, zero rows.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `season` | Int64 | Season year. |
| `team_id` | String | Unique team identifier. |
| `shooter_id` | String | Unique identifier for shooter. |
| `shot_x` | Float64 |  |
| `shot_y` | Float64 |  |
| `dist_ft` | Float64 |  |
| `shot_zone` | String |  |
| `shot_type` | String | Shot type label (e.g. 'Jump Shot', 'Layup'). |
| `made` | Boolean |  |
| `point_value` | Int64 |  |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `sec_left` | Float64 |  |
| `source` | String |  |
| `xmake` | Float64 |  |
| `xpoints` | Float64 |  |

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality
scored = mbb_shot_quality(mbb_shot_data(2025))

# Pipeline next step (one line)

scored.group_by("team_id").agg(pl.col("xpoints").sum()).sort("xpoints", descending=True)
```

### mbb_shot_quality_model {#mbb_shot_quality_model}

`mbb_shot_quality_model(shots: 'pl.DataFrame', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Empirical-Bayes `zone x type` make-rate / xPoints table.

Each cell's raw make rate is shrunk toward its PARENT-ZONE mean by
`n / (n + k)` with `k = get_constants(league).shrink_k_zone`
pseudo-attempts, so sparse cells (e.g. tip-ins in the mid zone) borrow
strength from their zone; `xpoints = make_rate_shrunk * point_value`
(the cell's modal point value).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots` | `DataFrame` |  | Canonical shot frame (needs `shot_zone, shot_type, made, point_value`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the shrinkage `k`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `(shot_zone, shot_type)`: `shot_zone, shot_type, n, make_rate_raw, make_rate_shrunk, point_value, xpoints`. Empty input returns the zero-row schema.

| col_name | type | description |
|---|---|---|
| `shot_zone` | String |  |
| `shot_type` | String | Shot type label (e.g. 'Jump Shot', 'Layup'). |
| `n` | Int64 |  |
| `make_rate_raw` | Float64 |  |
| `make_rate_shrunk` | Float64 |  |
| `point_value` | Float64 |  |
| `xpoints` | Float64 |  |

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality_model
model = mbb_shot_quality_model(mbb_shot_data(2025))

# Pipeline next step (one line)

model.sort("xpoints", descending=True).head(5)
```

### mbb_team_ratings {#mbb_team_ratings}

`mbb_team_ratings(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Opponent-adjusted team ratings (AdjO/AdjD/AdjEM/AdjTempo) per team-season.

Loads schedule + team boxscore for `seasons`, computes per-game efficiency,
runs the opponent-adjustment fixed points, and adds a per-season dense
`rank` (on `adj_em` descending) and `adj_em_z` (z-score of `adj_em`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season (e.g. `2024`) or list of seasons. |
| `league` | `str` | `'mens'` | `"mens"` / `"womens"` -- selects the constants. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per (season, team_id) with columns `season, team_id, adj_o, adj_d, adj_em, adj_tempo, raw_o, raw_d, games, rank, adj_em_z`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year. |
| `team_id` | String | Unique team identifier. |
| `adj_o` | Float64 | Adj o. |
| `adj_d` | Float64 | Adj d. |
| `adj_em` | Float64 | Adj em. |
| `adj_tempo` | Float64 |  |
| `raw_o` | Float64 | Raw o. |
| `raw_d` | Float64 | Raw d. |
| `games` | Int64 | Games played. |
| `rank` | Int64 | Rank. |
| `adj_em_z` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `InsufficientInputError` | When more than 10% of a requested season's games have no turnovers under any turnover key (ESPN's pre-2009 women's box schema), so its possession estimate -- and every rating derived from it -- would be wrong rather than merely missing. |

**Example**

```python
from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings
ratings = mbb_team_ratings(2024)
ratings.sort("rank").head()
```

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
