---
title: "MBB — additional Python functions — Calculate"
sidebar_label: "Calculate"
sidebar_position: 3
description: "MBB — additional Python functions — Calculate — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — Calculate

### calculate_aggregated_lineup_stats {#calculate_aggregated_lineup_stats}

`calculate_aggregated_lineup_stats(lineups: 'list[LineupStatSet] | None') -> 'LineupStatSet'`

Combine all lineups into a single team stat set.

Faithful port of `LineupUtils.calculateAggregatedLineupStats`
(`LineupUtils.ts:106`). Seeds an accumulator from
`StatModels.emptyLineup()` (`{"key": "empty", "doc_count": 0}`) plus
an `all_lineups` sub-accumulator of the same shape, then merges every
lineup via `weighted_avg`: lineups without a truthy `rapmRemove`
key merge into the main accumulator, while `rapmRemove` lineups merge
into `all_lineups` instead (their contribution is folded back in
afterward). Calls `complete_weighted_avg` to turn the main
accumulator's weighted sums into weighted averages, then -- because
`StatModels.emptyLineup()` always carries `key`/`doc_count` and so
is never considered "empty" by the upstream `lodash.isEmpty` check --
unconditionally re-merges the (now-averaged) team totals into
`all_lineups` and finishes that sub-accumulator too. Finally rebuilds
`off_net` / `off_raw_net` via `build_efficiency_margins`
(value-key always; old-value-key too when the team is in luck-adjusted
mode, i.e. `off_ppp.old_value` is present) -- but only on the top-level
result, matching upstream's "don't bother for all_lineups" comment.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `list[LineupStatSet] \| None` |  | The per-lineup `LineupStatSet` docs to fold together (e.g. the ES aggregation buckets under `responses[0].aggregations.lineups.buckets`). `None` or an empty list yields an all-zero/empty team stat set (mirrors the upstream `lineups \|\| []` guard). |

**Returns**

The aggregated team-total `LineupStatSet`, including a nested `all_lineups` key holding the `rapmRemove`-lineups-plus-team-total composite sub-aggregate.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import calculate_aggregated_lineup_stats

buckets = raw_response["responses"][0]["aggregations"]["lineups"]["buckets"]
team_info = calculate_aggregated_lineup_stats(buckets)
print(team_info["off_ppp"]["value"], team_info["off_poss"]["value"])

# RAPM-exclusion flag

buckets[1]["rapmRemove"] = True  # divert into all_lineups instead
team_info = calculate_aggregated_lineup_stats(buckets)
```

### calculate_possessions {#calculate_possessions}

`calculate_possessions(lineup_events: 'Iterable[LineupEvent]') -> 'list[LineupEvent]'`

Top-level entry point: calculate team/opponent possessions for a

sequence of lineup events (`PossessionUtils.calculate_possessions`,
`PossessionUtils.scala:371-379`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_events` | `Iterable[LineupEvent]` |  | The lineups to enrich, in chronological order. |

**Returns**

The lineups, each enriched with possession counts.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_possessions import calculate_possessions

enriched = calculate_possessions(lineups)
enriched[0].team_stats.num_possessions
```

### calculate_possessions_by_event {#calculate_possessions_by_event}

`calculate_possessions_by_event(raw_events_as_clumps: 'Iterable[ConcurrentClump]') -> 'list[LineupEvent]'`

Drive the batch loop + per-clump scoring over an already-flattened

clump stream (`PossessionUtils.calculate_possessions_by_event`,
`PossessionUtils.scala:521-573`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `raw_events_as_clumps` | `Iterable[ConcurrentClump]` |  | The unbatched clump stream, e.g. from flat-mapping `lineup_as_raw_clumps` over several lineups. |

**Returns**

The lineups, each enriched with possession counts, in original order.

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
| `regressed_players` | `list[float]` |  | The fitted per-player values (e.g. the final, strong-prior-blended RAPM from Task 3.5's `pickRidgeRegression`, or a raw `calculate_rapm` output), length `num_players`. |
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
helper here since Task 3.4's brief calls out the formula explicitly).
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

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calculate_sd_rapm

sd_rapm = calculate_sd_rapm(param_errs, err_sq, num_lineups=3, num_players=2)
```

### calculate_stats {#calculate_stats}

`calculate_stats(clump: 'ConcurrentClump', prev: 'ConcurrentClump', dir: 'Direction') -> 'PossCalcFragment'`

Calculate one direction's possession-fragment for one merged clump

(`PossessionUtils.calculate_stats`, `PossessionUtils.scala:170-369`).

See the upstream source's inline worked examples (and-one detection,
technical/flagrant offsetting, the deadball-rebound heuristic) for the
hand-annotated NCAA play-by-play snippets that motivate each step; this
port reproduces every step in the same order.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `ConcurrentClump` |  | The merged clump to score. |
| `prev` | `ConcurrentClump` |  | The previously-processed merged clump (feeds the and-one and deadball-rebound heuristics -- see below). |
| `dir` | `Direction` |  | Which side (`Direction.TEAM`/`Direction.OPPONENT`) is "attacking" for this calculation. Named to match the Scala (shadows the `dir` builtin -- consistent with this port's existing precedent of naming params after their Scala originals, e.g. `RawGameEvent.for_team`'s `min`). |

**Returns**

A `~sportsdataverse.mbb.mbb_ncaa_models.PossCalcFragment` for this clump/direction.
