---
title: "WBB — additional Python functions — Calc"
sidebar_label: "Calc"
sidebar_position: 2
description: "WBB — additional Python functions — Calc — function reference in sdv-py, the SportsDataverse Python package."
---
# WBB — additional Python functions — Calc

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
(calc_player_correlations`) and folds it into a possession
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
the module docstring's "Task 3.6 notes" for why this doesn't hit the
Task 3.5 "`ndarray` breaks deep `==`" concern).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `weight_matrix` | `NDArray[float64]` |  | An off/def design matrix, shape `(num_lineups, ctx["num_players"])` (e.g. `calc_player_weights`'s first return value, or a hand-built matrix for isolated testing). |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`. `ctx["num_players"]` sizes every per-player structure; `ctx["col_to_player"]` keys `player_combos`. |

**Returns**

A `RapmPreProcDiagnostics`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calc_collinearity_diag, calc_player_weights

off_weights, _ = calc_player_weights(ctx)
diag = calc_collinearity_diag(off_weights, ctx)
print(diag["lineup_combos"][0])  # the worst-conditioned combo
```

### calc_def_player_luck_adj {#calc_def_player_luck_adj}

`calc_def_player_luck_adj(sample: 'LineupStatSet', base: 'LineupStatSet', avg_eff: 'float') -> 'DefLuckAdjustmentDiags'`

Defensive 3P-luck adjustment for a single player.

Faithful port of `LuckUtils.calcDefPlayerLuckAdj` (`LuckUtils.ts:402-426`).
Unlike `calc_off_player_luck_adj`, this is **not** a pure
delegation -- see the module docstring's `calc_def_player_luck_adj`
note for the `translate()` remap this wraps around
`calc_def_team_luck_adj`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sample` | `LineupStatSet` |  | The player's stat dict for the period being luck-adjusted (must carry `oppo_total_def_3p_made`/`oppo_total_def_3p_attempts` -- there is no player-level `def_3p` field upstream, hence the remap). |
| `base` | `LineupStatSet` |  | The player's stat dict for the baseline/reference period. |
| `avg_eff` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |

**Returns**

Same shape as `calc_def_team_luck_adj`, computed against the translated (`oppo_*` -> `def_*`) player stat dicts.

**Example**

```python
from sportsdataverse.mbb.mbb_luck import calc_def_player_luck_adj

diags = calc_def_player_luck_adj(sample_player, base_player, 100.0)
print(diags["deltaDefAdjEff"])
```

### calc_def_team_luck_adj {#calc_def_team_luck_adj}

`calc_def_team_luck_adj(sample: 'LineupStatSet', base: 'LineupStatSet', avg_eff: 'float', sample_def_3pa_override: 'float | None' = None) -> 'DefLuckAdjustmentDiags'`

Defensive 3P-luck adjustment for a team (or lineup).

Faithful port of `LuckUtils.calcDefTeamLuckAdj` (`LuckUtils.ts:429-531`).
See the module docstring for the SoS-vs-luck-split formula (`LUCK_PCT`)
and the shared unguarded-division landmine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sample` | `LineupStatSet` |  | The team/lineup/player stat dict for the period being luck-adjusted (e.g. an on/off split or a single lineup). |
| `base` | `LineupStatSet` |  | The team/lineup/player stat dict for the baseline/reference period. |
| `avg_eff` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |
| `sample_def_3pa_override` | `float \| None` | `None` | When given, used as `sampleDef3PA` instead of `sample["total_def_3p_attempts"]` -- see `calc_off_team_luck_adj`'s `sample_3pa_override` docstring for the shared "lineup regression" rationale (`LuckUtils.ts:433-434`, verbatim comment). |

**Returns**

A `DefLuckAdjustmentDiags` dict -- TS-verbatim keys (`avgEff`, `luckPct`, `baseDef3P`, `baseDef3PSos`, `baseDef3PA`, `basePoss`, `base3PSosAdj`, `sampleDef3P`, `sampleDef3PSos`, `sampleDef3PA`, `samplePoss`, `sample3PSosAdj`, `sampleDefEfg`, `sampleDefPpp`, `sampleOffSos`, `sampleDef3PRate`, `sampleDefFGA`, `sampleDefOrb`, `avg3PSosAdj`, `adjDef3P`, `delta3P`, `deltaDefEfg`, `deltaDefPppNoOrb`, `deltaMissesPct`, `deltaDefOrbFactor`, `deltaPtsOffMisses`, `deltaDefPpp`, `deltaDefAdjEff`).

**Example**

```python
from sportsdataverse.mbb.mbb_luck import calc_def_team_luck_adj

diags = calc_def_team_luck_adj(sample_team_off, base_team, 100.0)
print(diags["deltaDefAdjEff"])
```

### calc_lineup_outputs {#calc_lineup_outputs}

`calc_lineup_outputs(field: 'str', off_offset: 'float', def_offset: 'float', ctx: 'RapmPlayerContext', adaptive_correl_weights: 'list[float] | None' = None, use_old_val_if_possible: 'tuple[bool, bool]' = (False, False)) -> 'list[NDArray[np.float64]]'`

Build the off/def target vectors the RAPM design matrices are fit against.

Faithful port of `RapmUtils.calcLineupOutputs` (`RapmUtils.ts:598-751`).
For each filtered lineup, computes a possession-weighted residual: the
lineup's own stat value, plus any global luck adjustment, minus the
accumulated "prior offset" contributed by every player on the lineup
(a strong-prior blend for kept players -- see get_strong_weight`
-- or a fixed baseline contribution for removed players).

Upstream keeps this as a plain `Array<Array<number>>` (*not* a mathjs
`Matrix`, unlike `calc_player_weights`'s `offWeights`/
`defWeights` -- `RapmUtils.test.ts`'s own `tidyResults` helper for
this function has a visibly different shape, see the classification map
in `tests/fixtures/hoop_explorer/README.md`). This port still
materializes both output vectors as `numpy.ndarray` for consistency
with `calc_player_weights` at the same dict -> array boundary --
Task 3.4's ridge-regression solve consumes both as arrays regardless of
the upstream distinction.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `field` | `str` |  | The stat suffix to read off each lineup, e.g. `"adj_ppp"` (read as `{prefix}_{field}`, e.g. `"off_adj_ppp"`). |
| `off_offset` | `float` |  | The D1-average offensive value for `field` (the regression's starting/baseline value on the RHS). |
| `def_offset` | `float` |  | The D1-average defensive value for `field`. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`, e.g. from `build_player_context`. |
| `adaptive_correl_weights` | `list[float] \| None` | `None` | Optional per-player adaptive-correlation weights (index-aligned with `ctx["col_to_player"]`), used as the strong-prior blend fallback when `ctx["prior_info"] ["strong_weight"] < 0` -- see get_strong_weight`. |
| `use_old_val_if_possible` | `tuple[bool, bool]` | `(False, False)` | `(use_old_val_for_off, use_old_val_for_def)` -- whether to prefer each lineup/team stat's luck-adjusted `old_value` over its raw `value` when present. This is the luck-adjustment hook Task 3.1's classification map flags as an **inherited coverage gap**: the vendored oracle fixture has `old_value == value` on every field (via `insertOldValues`), so neither jest nor this port's replay test ever observes this flag change the resulting numbers -- only that passing it doesn't crash. See the module docstring's "Task 3.3 coverage gap" note. |

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

### calc_off_player_luck_adj {#calc_off_player_luck_adj}

`calc_off_player_luck_adj(sample_player: 'LineupStatSet', base_player: 'LineupStatSet', avg_eff: 'float') -> 'OffLuckAdjustmentDiags'`

Offensive 3P-luck adjustment for a single player.

Faithful port of `LuckUtils.calcOffPlayerLuckAdj` (`LuckUtils.ts:174-187`).
Per Task 2.1's surprise #4, this is a literal 1-player-team delegation
to `calc_off_team_luck_adj` -- ORB effects are ignored for an
individual player (the upstream comment: "the team calc basically
works fine here, apart from ORBs, which we'll ignore").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sample_player` | `LineupStatSet` |  | The player's stat dict for the period being luck-adjusted. |
| `base_player` | `LineupStatSet` |  | The player's stat dict for the baseline/reference period. |
| `avg_eff` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |

**Returns**

Same shape as `calc_off_team_luck_adj` -- identical to calling that function with `sample_players=[sample_player]`, `base_players_map={base_player["key"]: base_player}`.

**Example**

```python
from sportsdataverse.mbb.mbb_luck import calc_off_player_luck_adj

diags = calc_off_player_luck_adj(sample_player, base_player, 100.0)
print(diags["deltaOffAdjEff"])
```

### calc_off_team_luck_adj {#calc_off_team_luck_adj}

`calc_off_team_luck_adj(sample_team: 'LineupStatSet', sample_players: 'list[LineupStatSet]', base_team: 'LineupStatSet', base_players_map: 'dict[str, LineupStatSet]', avg_eff: 'float', sample_3pa_override: 'float | None' = None, manual_overrides: 'list[ManualOverride] | None' = None) -> 'OffLuckAdjustmentDiags'`

Offensive 3P-luck adjustment for a team (or lineup).

Faithful port of `LuckUtils.calcOffTeamLuckAdj` (`LuckUtils.ts:190-399`).
See the module docstring for the Bayesian-shrink formula, the JS-array-
truthiness / object-selection landmines, and the one unguarded-division
landmine this function carries.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sample_team` | `LineupStatSet` |  | The team/lineup stat dict for the period being luck-adjusted (e.g. an on/off split or a single lineup). |
| `sample_players` | `list[LineupStatSet]` |  | The roster of per-player stat dicts backing `sample_team` (`samplePlayers == players.map(on/off/baseline)` per the upstream comment). |
| `base_team` | `LineupStatSet` |  | The team stat dict for the baseline/reference period (typically full-season). |
| `base_players_map` | `dict[str, LineupStatSet]` |  | `{player_key: base_period_player_stat_dict}`. |
| `avg_eff` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |
| `sample_3pa_override` | `float \| None` | `None` | When given, used as `sample3PA` instead of `sample_team["total_off_3p_attempts"]`. Per the upstream comment (`LuckUtils.ts:196-198`, shared verbatim with `calc_def_team_luck_adj`'s `sample_def_3pa_override`): "when calc'ing luck on lineups, each lineup gets the total sample as its regression so its average is right over the set" -- i.e. this lets every lineup in a sweep share one common 3PA denominator (the team's) for its regression target, rather than each lineup regressing against its own much smaller, noisier 3PA count. Note that `calc_off_player_luck_adj` itself does *not* pass this (its delegation call omits it entirely) -- the jest oracle's own "3P override" cross-check (`LuckUtils.test.ts:100-115`) instead calls `calc_off_team_luck_adj` directly with the player's own 3PA as this override, purely to demonstrate the parameter's effect in isolation. |
| `manual_overrides` | `list[ManualOverride] \| None` | `None` | Per-player 3P%-expectation overrides from the UI. **A non-`None` empty list still activates the team-level override-delta branch** (JS array truthiness) -- see the module docstring's landmine note. `None` (the default) is the "no overrides at all" case. |

**Returns**

An `OffLuckAdjustmentDiags` dict -- TS-verbatim keys (`avgEff`, `samplePoss`, `sample3P`, `sample3PA`, `base3PA`, `player3PInfo` (per-player detail, sorted by descending `shot_info_total_3p`), `sampleBase3P`, `regress3P`, `sampleOff3PRate`, `sampleOffFGA`, `sampleOffOrb`, `sampleOffEfg`, `sampleOffPpp`, `sampleDefSos`, `delta3P`, `deltaOffEfg`, `deltaMissesPct`, `deltaOffPppNoOrb`, `deltaOffOrbFactor`, `deltaPtsOffMisses`, `deltaOffPpp`, `deltaOffAdjEff`).

**Example**

```python
from sportsdataverse.mbb.mbb_luck import calc_off_team_luck_adj

diags = calc_off_team_luck_adj(
    sample_team_on, sample_players_on, base_team, base_players_map, 100.0,
)
print(diags["deltaOffAdjEff"])

# With per-player manual 3P% overrides

diags = calc_off_team_luck_adj(
    sample_team_on, sample_players_on, base_team, base_players_map, 100.0,
    manual_overrides=[
        {"rowId": "Cowan, Anthony", "statName": "off_3p", "newVal": 0.5, "use": True},
    ],
)
```

### calc_player_weights {#calc_player_weights}

`calc_player_weights(ctx: 'RapmPlayerContext') -> 'list[NDArray[np.float64]]'`

Build the off/def player-weight (design) matrices for the RAPM solve.

Faithful port of `RapmUtils.calcPlayerWeights` (`RapmUtils.ts:544-595`).
One row per (filtered) lineup, one column per remaining player; each
filled cell is `sqrt(lineup_possessions / total_side_possessions)` --
the possession-weighted design-matrix entry the ridge regression (Task
3.4) solves against. This is the first function in the module where a
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
