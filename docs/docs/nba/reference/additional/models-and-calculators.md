---
title: "NBA — additional Python functions — Models and calculators: AdjRapmModel–game_page"
sidebar_label: "Models and calculators: AdjRapmModel–game_page"
sidebar_position: 11
description: "NBA — additional Python functions — Models and calculators: AdjRapmModel–game_page — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Models and calculators: AdjRapmModel–game_page

### AdjRapmModel {#AdjRapmModel}

`AdjRapmModel(prior: 'Dict[int, Tuple[float, float]]', alphas: 'np.ndarray' = <factory>, n_samples: 'int' = 200, seed: 'int' = 0) -> None`

Prior-informed RAPM: ridge toward a per-player box prior with an RTO posterior.

Implements the `~sportsdataverse.nba.nba_model_validation.PriorModel`
protocol so the validation harness routes through `fit_with_prior` and the
resulting `~sportsdataverse.nba.nba_model_validation.FitResult` carries
a posterior — enabling Oracle ④ (interval calibration).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `prior` | `Dict[int, Tuple[float, float]]` |  | Per-player `{player_id: (o_prior, d_prior)}` in per-100 units. |
| `alphas` | `ndarray` | `<factory>` | RidgeCV alpha grid forwarded to `_fit_prior_ridge`. |
| `n_samples` | `int` | `200` | Number of RTO posterior samples. |
| `seed` | `int` | `0` | RNG seed for the RTO sampler. |

**Example**

```python
from sportsdataverse.nba import AdjRapmModel, nba_spm
from sportsdataverse.nba.nba_model_validation import validate_model
prior = AdjRapmModel.from_spm(nba_spm(box_feats, coef))
report = validate_model(prior, season_frames, model_name="adj_rapm")
print(report.calibration.coverage)      # non-None: the prior model has a posterior
```

**Methods**

#### AdjRapmModel.fit_with_prior

`AdjRapmModel.fit_with_prior(X: 'csr_matrix', y: 'np.ndarray', prior_mean: 'np.ndarray') -> 'FitResult'`

Delegate to `_fit_prior_ridge` using this model's hyperparameters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `csr_matrix` |  | Sparse design `(n, 2P)` from `~sportsdataverse.nba.nba_rapm.build_rapm_design`. |
| `y` | `ndarray` |  | Possession points `(n,)`. |
| `prior_mean` | `ndarray` |  | Per-possession prior mean `(2P,)` built by the harness. |

**Returns**

`~sportsdataverse.nba.nba_model_validation.FitResult` with posterior of shape `(n_samples, 2P)`.

#### AdjRapmModel.from_bpm

`AdjRapmModel.from_bpm(bpm: 'pl.DataFrame', **kw: 'Any') -> "'AdjRapmModel'"`

Construct from a BPM output frame (`obpm` / `dbpm` columns).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `bpm` | `DataFrame` |  | Frame with `player_id`, `obpm`, `dbpm` columns (per-100 units). |

**Returns**

`AdjRapmModel` whose `prior` maps each player_id to `(obpm, dbpm)`.

#### AdjRapmModel.from_spm

`AdjRapmModel.from_spm(spm: 'pl.DataFrame', **kw: 'Any') -> "'AdjRapmModel'"`

Construct from an SPM output frame (`ospm` / `dspm` columns).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `spm` | `DataFrame` |  | Frame with `player_id`, `ospm`, `dspm` columns (per-100 units). |

**Returns**

`AdjRapmModel` whose `prior` maps each player_id to `(ospm, dspm)`.

### AgingCurve {#AgingCurve}

`AgingCurve(delta_by_age: 'Dict[int, float]' = <factory>) -> None`

Empirical aging deltas: `delta_by_age[a]` = expected rating change aging a -> a+1.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `delta_by_age` | `Dict[int, float]` | `<factory>` |  |

**Methods**

#### AgingCurve.delta

`AgingCurve.delta(age: 'float') -> 'float'`

Aging drift for a player of (rounded) `age`; 0.0 outside the fitted range.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `age` | `float` |  |  |

### BoxPriorModel {#BoxPriorModel}

`BoxPriorModel(feature_names: 'list[str]', o_coef: 'np.ndarray', o_intercept: 'np.ndarray', d_coef: 'np.ndarray', d_intercept: 'np.ndarray', season: 'int', train_seasons: 'list[int]') -> None`

A cross-fitted box prior: one offense and one defense linear rule per player fold.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `feature_names` | `list[str]` |  | Ordered feature columns (`BOX_PRIOR_FEATURES`). |
| `o_coef` | `ndarray` |  | `(folds, F)` offense coefficients on the raw features. |
| `o_intercept` | `ndarray` |  | `(folds,)`. |
| `d_coef` | `ndarray` |  | `(folds, F)` defense coefficients (`d` positive = good defense). |
| `d_intercept` | `ndarray` |  | `(folds,)`. |
| `season` | `int` |  | The season the prior is for. |
| `train_seasons` | `list[int]` |  | The seasons it learned from. |

### CurrentImpactFilter {#CurrentImpactFilter}

`CurrentImpactFilter(params: 'CurrentImpactParams | None' = None) -> 'None'`

The joint Kalman filter of `nba_current_impact`, fed one season and one game at a time.

State, per possession and in the RAPM design's sign: each player's offense deviation, defense
deviation (negative = good defense) and the league level. Within a season the covariance is joint
over the season's players; across seasons each player's mean and variance carry (the cross terms do
not: ponytail, carry the block if a gate ever wants it).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `params` | `CurrentImpactParams \| None` | `None` | The shock and decay SDs. |

**Methods**

#### CurrentImpactFilter.drift

`CurrentImpactFilter.drift(days: 'float') -> 'None'`

Add `days` of drift variance to every player's deviation (the time decay).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `days` | `float` |  |  |

#### CurrentImpactFilter.end_season

`CurrentImpactFilter.end_season() -> 'None'`

Store each seen player's mean and variance for later seasons.

#### CurrentImpactFilter.ratings

`CurrentImpactFilter.ratings() -> 'dict[int, tuple[float, float, float, float, int, int, dt.date | None]]'`

Every seen player's `(o, d, o_prior, d_prior, off_poss, def_poss, last_game)`.

Ratings and priors per 100 (`d` positive = good defense): the prior in force plus the deviation.
A player carried from an earlier season who is not in this one's matrix gets his carried deviation
plus the prior in force for him (0 when he has none). Possessions count this season's games.

#### CurrentImpactFilter.set_prior

`CurrentImpactFilter.set_prior(prior: 'dict[int, tuple[float, float]]') -> 'None'`

Set the expectation `{pid: (o_prior, d_prior)}` per 100; absent players get 0 (league average).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `prior` | `dict[int, tuple[float, float]]` |  |  |

#### CurrentImpactFilter.start_season

`CurrentImpactFilter.start_season(pids: 'Sequence[int]', noise_var: 'float | None' = None) -> 'None'`

Open a season for `pids`: carried players get `season_sd^2` more variance, the level resets.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pids` | `Sequence[int]` |  |  |
| `noise_var` | `float \| None` | `None` |  |

#### CurrentImpactFilter.symmetrize

`CurrentImpactFilter.symmetrize() -> 'None'`

Average the covariance with its transpose (rounding drift; `run_current_impact` calls it daily).

#### CurrentImpactFilter.update_game

`CurrentImpactFilter.update_game(off: 'np.ndarray', de: 'np.ndarray', points: 'np.ndarray', teams: 'dict[int, int]', day: 'dt.date') -> 'None'`

Update on one game's possessions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `off` | `ndarray` |  | `(n, 5)` offensive player ids. |
| `de` | `ndarray` |  | `(n, 5)` defensive player ids. |
| `points` | `ndarray` |  | `(n,)` possession points. |
| `teams` | `dict[int, int]` |  | `{pid: team_id}` for every player in the game (for the team-change shock). |
| `day` | `date` |  | The game date. |

### CurrentImpactParams {#CurrentImpactParams}

`CurrentImpactParams(entry_sd: 'float' = 0.983, season_sd: 'float' = 0.417, team_sd: 'float' = 0.026, drift_sd: 'float' = 0.0115, level_sd: 'float' = 0.05) -> None`

The filter's shock and decay SDs, per 100 possessions (`level_sd` per possession).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `entry_sd` | `float` | `0.983` | SD of a new player's deviation. |
| `season_sd` | `float` | `0.417` | SD added at each new season. |
| `team_sd` | `float` | `0.026` | SD added at a team change. |
| `drift_sd` | `float` | `0.0115` | SD of the daily drift, per square-root day. |
| `level_sd` | `float` | `0.05` | SD of the league level at a season start. |

### ExternalValidityResult {#ExternalValidityResult}

`ExternalValidityResult(corr: 'float', n_matched: 'int', coverage_pct: 'float', permutation_p95: 'float', join: 'str') -> None`

Concurrent-validity correlation of model ratings against a published oracle metric.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `corr` | `float` |  | Pearson correlation of the model's rating column against the oracle's value column, over matched rows. `nan` when fewer than 3 rows matched. |
| `n_matched` | `int` |  | Number of rows successfully joined (ratings <-> oracle). |
| `coverage_pct` | `float` |  | `100 * n_matched / len(ratings)` -- how much of the model's player population the oracle covers. `0.0` when `ratings` is empty. |
| `permutation_p95` | `float` |  | 95th percentile of `\|corr\|` over `n_permutations` random shuffles of the oracle's matched value column -- a self-computed null-correlation ceiling. The spec deliberately avoids a hardcoded floor constant; compare `corr` against this instead. `nan` when fewer than 3 rows matched. |
| `join` | `str` |  | The join strategy used (`"id"` or `"name"`). |

### ForecastResult {#ForecastResult}

`ForecastResult(forecast_rmse: 'float', forecast_corr: 'float', baseline_rmse: 'float', n_forecasts: 'int') -> None`

Forecast-accuracy metrics: predicted-vs-actual next-season rating over held-out transitions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `forecast_rmse` | `float` |  |  |
| `forecast_corr` | `float` |  |  |
| `baseline_rmse` | `float` |  |  |
| `n_forecasts` | `int` |  |  |

### LeagueConstants {#LeagueConstants}

`LeagueConstants(hfa: 'float', margin_sd: 'float', avg_pace: 'float', avg_off_rtg: 'float', game_minutes: 'int', in_game_wp_artifact: 'str' = 'nba_in_game_wp.ubj') -> None`

Per-`league_id` fitted constants for the NBA prediction & market stack.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `hfa` | `float` |  | Home-court advantage in points (fitted on the as-of-date backtest for the league; see `tools/models/nba_prediction/fit_pregame.py`). |
| `margin_sd` | `float` |  | Std. dev. of the game-margin residual (fitted jointly with `hfa`; the Brier-minimizing sigma agrees to within a documented tolerance). |
| `avg_pace` | `float` |  | League baseline possessions per team per game (adjusted- pace anchor for `~sportsdataverse.nba.nba_team_ratings.adjust_pace`). |
| `avg_off_rtg` | `float` |  | League baseline points per 100 possessions. |
| `game_minutes` | `int` |  | Regulation game length in minutes (NBA/G-League 48, WNBA 40) -- structurally different, not a fitted number. |
| `in_game_wp_artifact` | `str` | `'nba_in_game_wp.ubj'` | Filename of the bundled in-game-WP coefficients under `sportsdataverse/nba/models` (committed in Phase 3). |

### MeasureSpec {#MeasureSpec}

`MeasureSpec(measure: 'str', actual: 'str', denom: 'str', out_prefix: 'str', extra_denoms: 'dict[str, tuple[str, str]]' = <factory>) -> None`

Per-model column map for a `leaguedashptstats` measure.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `measure` | `str` |  | `pt_measure_type` sent to `nba_stats_leaguedashptstats`. |
| `actual` | `str` |  | Realized-outcome column (snake_case). |
| `denom` | `str` |  | Opportunity column (snake_case). |
| `out_prefix` | `str` |  | Output-column prefix, e.g. `"reb"` -> `reb_oe`. |
| `extra_denoms` | `dict[str, tuple[str, str]]` | `<factory>` | Difficulty buckets: `label -> (actual_col, denom_col)`. |

### NbaBpmModel {#NbaBpmModel}

`NbaBpmModel(player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame', positions: 'pl.DataFrame', *, team_adjust: 'bool' = True) -> 'None'`

A `RatingsModel` scoring a fold via faithful BPM 2.0.

Scores a fold via faithful BPM 2.0; position/role are estimated **fold-native**
(recomputed over the fold's games) in v1 — a full-season-position refinement is a
documented follow-up. `fit_ratings` restricts the box rate + team margin to the
fold's games (the leakage guard).

Design note: position/role are recomputed inside `nba_bpm` over the fold in v1 for
simplicity (fold-native); the spec's "position over full season" refinement is a
documented follow-up if faithfulness testing shows fold-position drift matters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | Per-player-per-game box lines (same schema as `nba_bpm`). |
| `team_logs` | `DataFrame` |  | Per-team-per-game lines including `plus_minus`. |
| `positions` | `DataFrame` |  | Listed positions (`player_id`, `position_num`) from `nba_player_positions`. |
| `team_adjust` | `bool` | `True` | Apply the team adjustment (`True`) or return raw box-BPM (`False`). |

**Example**

```python
from sportsdataverse.nba import NbaBpmModel
from sportsdataverse.nba.nba_model_validation import validate_model
model = NbaBpmModel(logs["player"], logs["team"], positions)
report = validate_model(model, season_frames, model_name="bpm")
```

**Methods**

#### NbaBpmModel.fit_ratings

`NbaBpmModel.fit_ratings(possessions: 'pl.DataFrame') -> 'RatingsFit'`

Score the fold's players via BPM 2.0, restricted to the fold's game_ids.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | The fold's possession+lineup frame. Only the `game_id` values it contains are used to filter `player_logs` and `team_logs` (the leakage guard). |

**Returns**

`RatingsFit` with `o_ratings` (OBPM) and `d_ratings` (DBPM) keyed by player_id. Returns empty dicts when no box data covers the fold's games.

### NbaSpmModel {#NbaSpmModel}

`NbaSpmModel(coefficients: 'SpmCoefficients', player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame') -> 'None'`

A `RatingsModel` that scores a fold via fitted SPM coefficients.

Restricts its box aggregation to the fold's `game_id` (the leakage guard),
then applies the (globally pre-fit) SPM coefficients.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `coefficients` | `SpmCoefficients` |  | A `SpmCoefficients` instance from `train_spm`. |
| `player_logs` | `DataFrame` |  | Per-player-per-game box lines used to build fold features. |
| `team_logs` | `DataFrame` |  | Per-team-per-game lines used to estimate per-game possessions. |

**Example**

```python
from sportsdataverse.nba import NbaSpmModel, train_spm
from sportsdataverse.nba.nba_model_validation import validate_model
model = NbaSpmModel(coef, logs["player"], logs["team"])
report = validate_model(model, season_frames, model_name="spm")
```

**Methods**

#### NbaSpmModel.fit_ratings

`NbaSpmModel.fit_ratings(possessions: 'pl.DataFrame') -> 'RatingsFit'`

Aggregate the fold's box (restricted to its game_ids) and apply SPM coeffs.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | The fold's possession+lineup frame. Only the `game_id` values it contains are used to filter `player_logs` and `team_logs` (the leakage guard). |

**Returns**

`RatingsFit` with `o_ratings` and `d_ratings` dicts mapping player_id to per-100 OSPM/DSPM. Returns empty dicts when no box features can be built from the fold's games.

### RemlFit {#RemlFit}

`RemlFit(coef: 'np.ndarray', intercept: 'float', lambda_o: 'float', lambda_d: 'float', sigma2: 'float', prior_weight: 'np.ndarray', reml: 'float', converged: 'bool') -> None`

A prior-informed ridge fit with separate offense / defense penalties.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `coef` | `ndarray` |  | Posterior mean `(2P,)` per possession (offense then defense, as the design). |
| `intercept` | `float` |  | Points per possession at the prior (unpenalized). |
| `lambda_o` | `float` |  | Offense penalty (noise variance over the offense prior variance). |
| `lambda_d` | `float` |  | Defense penalty. |
| `sigma2` | `float` |  | Residual variance per possession. |
| `prior_weight` | `ndarray` |  | `(2P,)` weight of a player's own prior in his posterior mean, the diagonal of `M^-1 diag(lambda)` (1 for a player with no possessions, toward 0 as his possessions swamp the prior). His teammates' and opponents' priors also reach him through the off-diagonal terms. |
| `reml` | `float` |  | The minimized `-2` restricted log likelihood (up to a constant). |
| `converged` | `bool` |  | `False` when the REML search stopped without converging (a warning is raised too); `True` for given penalties. |

### RidgeRapmModel {#RidgeRapmModel}

`RidgeRapmModel(alphas: 'np.ndarray' = array([   100.        ,    268.26957953,    719.685673  ,   1930.69772888,
         5179.47467923,  13894.95494373,  37275.93720315, 100000.        ])) -> 'None'`

Reference model: the merged plain-RAPM RidgeCV fit, adapted to `RapmModel`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `alphas` | `ndarray` | `array([   100.        ,    268.26957953,    719.685673  ,   1930.69772888,
         5179.47467923,  13894.95494373,  37275.93720315, 100000.        ])` | Ridge penalty grid for cross-validation. Defaults to the merged `DEFAULT_RAPM_ALPHAS`. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_rapm import build_rapm_design
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel

rows = {
    "off_player_1": [1, 6], "off_player_2": [2, 7],
    "off_player_3": [3, 8], "off_player_4": [4, 9],
    "off_player_5": [5, 10],
    "def_player_1": [6, 1], "def_player_2": [7, 2],
    "def_player_3": [8, 3], "def_player_4": [9, 4],
    "def_player_5": [10, 5],
    "points": [2, 0],
}
poss = pl.DataFrame(rows)
X, y, pids = build_rapm_design(poss)
fit = RidgeRapmModel().fit(X, y)
print(fit.coef.shape)    # (20,) — 10 players × 2 sides
print(fit.posterior)     # None — point estimator
```

**Methods**

#### RidgeRapmModel.fit

`RidgeRapmModel.fit(X: 'csr_matrix', y: 'np.ndarray') -> 'FitResult'`

Fit RidgeCV and return coefficients + intercept (no posterior).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `csr_matrix` |  | Sparse design matrix of shape `(n_possessions, 2P)`. |
| `y` | `ndarray` |  | Target points per possession, shape `(n_possessions,)`. |

**Returns**

FitResult with `coef` shape `(2P,)`, scalar `intercept`, and `posterior=None`.

### SeasonGram {#SeasonGram}

`SeasonGram(pids: 'np.ndarray', gram: 'np.ndarray', xty: 'np.ndarray', counts: 'np.ndarray', n: 'int', ysum: 'float') -> None`

One season's RAPM sufficient statistics (the design of `build_rapm_design`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pids` | `ndarray` |  | The season's player ids, in design order. |
| `gram` | `ndarray` |  | `X'X` dense `(2P, 2P)`. |
| `xty` | `ndarray` |  | `X'y` `(2P,)`. |
| `counts` | `ndarray` |  | `X'1` `(2P,)`: offensive then defensive possessions per player. |
| `n` | `int` |  | Possessions. |
| `ysum` | `float` |  | Total points. |

### SpmCoefficients {#SpmCoefficients}

`SpmCoefficients(o_coef: 'np.ndarray', d_coef: 'np.ndarray', o_intercept: 'float', d_intercept: 'float', feature_names: 'List[str]') -> None`

Fitted SPM coefficients (box features -> offense/defense RAPM, per-100).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `o_coef` | `ndarray` |  | Coefficient vector for the offense regression (shape `[n_features]`). |
| `d_coef` | `ndarray` |  | Coefficient vector for the defense regression (shape `[n_features]`). |
| `o_intercept` | `float` |  | Intercept for the offense regression. |
| `d_intercept` | `float` |  | Intercept for the defense regression. |
| `feature_names` | `List[str]` |  | Ordered list of feature column names corresponding to the coefficient vectors. |

### ValidationReport {#ValidationReport}

`ValidationReport(model_name: 'str', n_seasons: 'int', retrodiction: 'Optional[RetrodictionResult]' = None, reliability: 'Optional[ReliabilityResult]' = None, cross_season: 'Optional[CrossSeasonResult]' = None, calibration: 'Optional[CalibrationResult]' = None, external: 'Optional[ExternalValidityResult]' = None, walk_forward: 'Optional[WalkForwardResult]' = None) -> None`

Holds all oracle results for a single model evaluation run.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model_name` | `str` |  | Human-readable label for the model being evaluated. |
| `n_seasons` | `int` |  | Number of season frames supplied to `validate_model`. |
| `retrodiction` | `Optional[RetrodictionResult]` | `None` | Result from Oracle 1, or `None` if not selected. |
| `reliability` | `Optional[ReliabilityResult]` | `None` | Result from Oracle 2, or `None` if not selected. |
| `cross_season` | `Optional[CrossSeasonResult]` | `None` | Result from Oracle 3, or `None` if not selected. |
| `calibration` | `Optional[CalibrationResult]` | `None` | Result from Oracle 4, or `None` if not selected or the model is a point estimator. |
| `external` | `Optional[ExternalValidityResult]` | `None` | Result from Oracle 5 (`external_validity`), or `None` if not selected. |
| `walk_forward` | `Optional[WalkForwardResult]` | `None` | Result from Oracle 6 (`walk_forward`), or `None` if not selected. |

**Example**

```python
from sportsdataverse.nba.nba_model_validation import (
    RidgeRapmModel, validate_model,
)

# season_frames is a list[pl.DataFrame] of possession stints per season
rep = validate_model(RidgeRapmModel(), season_frames, model_name="plain_rapm")
print(rep.model_name)                        # "plain_rapm"
print(rep.n_seasons)                         # len(season_frames)
print(rep.retrodiction.game_margin_rmse)     # float
print(rep.reliability.spearman_brown)        # float
print(rep.calibration)                       # None — point estimator
```

### WalkForwardResult {#WalkForwardResult}

`WalkForwardResult(game_margin_rmse: 'float', game_margin_corr: 'float', carry_forward_rmse: 'float', random_fold_rmse: 'float', n_checkpoints: 'int', n_test_games: 'int') -> None`

Oracle 6: walk-forward ("predict tomorrow") retrodiction over a season timeline.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_margin_rmse` | `float` |  | RMSE of predicted vs actual per-(game, team) margins, pooled across all checkpoints -- the model refit through each checkpoint date, predicting the following `horizon_days` window. |
| `game_margin_corr` | `float` |  | Pearson correlation of the same pooled predictions. |
| `carry_forward_rmse` | `float` |  | RMSE using the PRIOR checkpoint's fit (no refit) applied to the current window -- isolates whether refitting through each checkpoint actually helps. `nan` when fewer than 2 checkpoints produce a valid window. |
| `random_fold_rmse` | `float` |  | Oracle 1's (`retrodiction`) pooled game-margin RMSE on the same possessions -- the non-time-ordered baseline. |
| `n_checkpoints` | `int` |  | Number of checkpoint dates that produced a non-degenerate (train, test) split. |
| `n_test_games` | `int` |  | Total distinct game_ids evaluated across all checkpoints. |

### adjust_efficiency {#adjust_efficiency}

`adjust_efficiency(game_eff: 'pl.DataFrame', *, league_id: 'str' = '00', max_iter: 'int' = 100, tol: 'float' = 0.0001) -> 'pl.DataFrame'`

Iterative opponent-adjusted rating -> AdjOffRtg / AdjDefRtg / AdjNet per team-season.

KenPom-style fixed point: initialize `adj_off = raw_off` /
`adj_def = raw_def`, then repeatedly recompute each team's rating from
its games with the opponent's *current* adjusted rating and a
home-court adjustment removed, until the largest change is below `tol`.
Ratings are computed independently per season.

The per-game offensive update is
`off_rtg - (adj_def_opp - avg) - loc_o` where `loc_o` is `+hfa/2` at
home, `-hfa/2` away, `0` neutral (defense is symmetric with the
opposite sign); `avg` is the league baseline off rating and `hfa`
comes from `~sportsdataverse.nba.nba_prediction_constants.get_constants`.

*(T7.2-shared algorithm)* -- identical fixed point to the MBB
`mbb_team_ratings.adjust_efficiency` / CFB ratings cores.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_eff` | `DataFrame` |  | Output of `raw_game_efficiency`. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League -- selects the HFA + baseline off-rating constants. |
| `max_iter` | `int` | `100` | Maximum fixed-point iterations. |
| `tol` | `float` | `0.0001` | Convergence tolerance on the largest rating change. |

**Returns**

One row per (season, team_id): `season, team_id, adj_off_rtg, adj_def_rtg, adj_net_rtg, raw_off_rtg, raw_def_rtg, games`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year. |
| `team_id` | String | Unique team identifier. |
| `adj_off_rtg` | Float64 |  |
| `adj_def_rtg` | Float64 |  |
| `adj_net_rtg` | Float64 |  |
| `raw_off_rtg` | Float64 |  |
| `raw_def_rtg` | Float64 |  |
| `games` | Int64 | Games played. |

**Example**

```python
from sportsdataverse.nba.nba_team_ratings import adjust_efficiency, raw_game_efficiency
ratings = adjust_efficiency(raw_game_efficiency(sched, box))
```

### adjust_pace {#adjust_pace}

`adjust_pace(game_eff: 'pl.DataFrame', *, league_id: 'str' = '00', max_iter: 'int' = 100, tol: 'float' = 0.0001) -> 'pl.DataFrame'`

Opponent-adjusted pace (possessions/game) per team-season.

Same fixed point as `adjust_efficiency`, applied to game
possessions under the additive model `poss = pace_i + pace_j - avg`: a
team's pace is recovered by removing its opponents' current adjusted
pace. `avg` is the league baseline pace from
`~sportsdataverse.nba.nba_prediction_constants.get_constants`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_eff` | `DataFrame` |  | Output of `raw_game_efficiency`. |
| `league_id` | `str` | `'00'` | `"00"` / `"10"` / `"20"` -- selects the pace baseline. |
| `max_iter` | `int` | `100` | Maximum fixed-point iterations. |
| `tol` | `float` | `0.0001` | Convergence tolerance on the largest pace change. |

**Returns**

One row per (season, team_id): `season, team_id, adj_pace, raw_pace`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year. |
| `team_id` | String | Unique team identifier. |
| `adj_pace` | Float64 |  |
| `raw_pace` | Float64 |  |

**Example**

```python
from sportsdataverse.nba.nba_team_ratings import adjust_pace, raw_game_efficiency
pace = adjust_pace(raw_game_efficiency(sched, box))
```

### apply_box_prior {#apply_box_prior}

`apply_box_prior(features: 'pl.DataFrame', model: 'BoxPriorModel', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Score box-prior features with each player's out-of-fold rule.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `features` | `DataFrame` |  | Output of `box_prior_features` (`player_id` and `BOX_PRIOR_FEATURES`). |
| `model` | `BoxPriorModel` |  | A `BoxPriorModel` from `train_box_prior`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`player_id` (Int64), `o_prior`, `d_prior`, `box_prior` (Float64, per 100; `d` positive = good defense; `box_prior = o_prior + d_prior`).

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | NBA stats person id. |
| `o_prior` | Float64 | Box prior v2 offense per 100, from the player's out-of-fold rule. |
| `d_prior` | Float64 | Box prior v2 defense per 100 (positive = good defense), from the player's out-of-fold rule. |
| `box_prior` | Float64 | o_prior + d_prior. |

**Example**

```python
prior = apply_box_prior(feats, model)
prior_dict = {r[0]: (r[1], r[2]) for r in prior.select("player_id", "o_prior", "d_prior").iter_rows()}
```

### as_of_ratings_split {#as_of_ratings_split}

`as_of_ratings_split(results: 'pl.DataFrame', cutoff_date: 'datetime.date') -> 'pl.DataFrame'`

Filter a results frame to games strictly before a cutoff date (leakage boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | A `polars.DataFrame` with a `date` column. |
| `cutoff_date` | `date` |  | Games on or after this date are excluded. |

**Returns**

A `polars.DataFrame` containing only rows with `date < cutoff_date`.

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
| `off_rtg` | Float64 |  |
| `def_rtg` | Float64 |  |

**Example**

```python
import datetime as dt
from sportsdataverse._common.metrics import as_of_ratings_split
as_of_ratings_split(results, dt.date(2023, 9, 8))
```

### box_prior_features {#box_prior_features}

`box_prior_features(player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame', possessions: 'pl.DataFrame', bio: 'pl.DataFrame', *, game_ids: 'Sequence[str] | None' = None, pad_poss: 'float' = 1000.0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

One season's box-prior features: league-relative, padded box rates plus player and lineup context.

Per-100 rates come from `~sportsdataverse.nba.nba_box_logs.box_features`. Each rate (and
minutes per game) is taken relative to the season's on-court-possession-weighted league mean and
padded toward it: `(x - league) * poss / (poss + pad_poss)`, `poss` the player's offensive
on-court possessions. Context: age (centred at 27, and squared), height relative to the league mean,
listed position (1-5, centred at 3), and the mean over his offensive possessions of his four
teammates' and five opponents' padded points, assists and rebounds per 100.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | Per-player-game box lines (`leaguegamelog` `P`). |
| `team_logs` | `DataFrame` |  | Per-team-game lines (`leaguegamelog` `T`). |
| `possessions` | `DataFrame` |  | The season's possession+lineup rows. |
| `bio` | `DataFrame` |  | `player_id` (Int64), `age`, `height_in`, `position_num` (Float64; nulls allowed). |
| `game_ids` | `Sequence[str] \| None` | `None` | Restrict logs and possessions to these games (a fold's training games: the leakage guard of the held-out and chronological tests). |
| `pad_poss` | `float` | `1000.0` | Padding possessions. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per player on court in `possessions`: `player_id` (Int64), `poss` / `def_poss` (Float64, on-court possessions), and `BOX_PRIOR_FEATURES` (Float64, no nulls).

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | NBA stats person id. |
| `poss` | Float64 | Offensive on-court possessions in the games used. |
| `def_poss` | Float64 | Defensive on-court possessions in the games used. |
| `pts_rel` | Float64 | pts per 100 possessions minus the season's on-court-possession-weighted league mean, padded toward it: (x - league) * poss / (poss + pad_poss), pad_poss 1,000 by default. |
| `fg3m_rel` | Float64 | fg3m per 100 possessions minus the season's on-court-possession-weighted league mean, padded toward it: (x - league) * poss / (poss + pad_poss), pad_poss 1,000 by default. |
| `fga_rel` | Float64 | fga per 100 possessions minus the season's on-court-possession-weighted league mean, padded toward it: (x - league) * poss / (poss + pad_poss), pad_poss 1,000 by default. |
| `fta_rel` | Float64 | fta per 100 possessions minus the season's on-court-possession-weighted league mean, padded toward it: (x - league) * poss / (poss + pad_poss), pad_poss 1,000 by default. |
| `ast_rel` | Float64 | ast per 100 possessions minus the season's on-court-possession-weighted league mean, padded toward it: (x - league) * poss / (poss + pad_poss), pad_poss 1,000 by default. |
| `oreb_rel` | Float64 | oreb per 100 possessions minus the season's on-court-possession-weighted league mean, padded toward it: (x - league) * poss / (poss + pad_poss), pad_poss 1,000 by default. |
| `dreb_rel` | Float64 | dreb per 100 possessions minus the season's on-court-possession-weighted league mean, padded toward it: (x - league) * poss / (poss + pad_poss), pad_poss 1,000 by default. |
| `stl_rel` | Float64 | stl per 100 possessions minus the season's on-court-possession-weighted league mean, padded toward it: (x - league) * poss / (poss + pad_poss), pad_poss 1,000 by default. |
| `blk_rel` | Float64 | blk per 100 possessions minus the season's on-court-possession-weighted league mean, padded toward it: (x - league) * poss / (poss + pad_poss), pad_poss 1,000 by default. |
| `tov_rel` | Float64 | tov per 100 possessions minus the season's on-court-possession-weighted league mean, padded toward it: (x - league) * poss / (poss + pad_poss), pad_poss 1,000 by default. |
| `pf_rel` | Float64 | pf per 100 possessions minus the season's on-court-possession-weighted league mean, padded toward it: (x - league) * poss / (poss + pad_poss), pad_poss 1,000 by default. |
| `mpg_rel` | Float64 | Minutes per game minus the league mean, padded as the rates are. |
| `age_c` | Float64 | (Age - 27) / 10, age from leaguedashplayerbiostats; 0 when unknown. |
| `age_c2` | Float64 | Square of age_c: the quadratic term of the aging curve. |
| `height_rel` | Float64 | Height in inches minus the season's possession-weighted league mean (a missing height takes the mean). |
| `position_c` | Float64 | Listed position on the 1-5 scale minus 3 (unknown = 0). |
| `tm_pts` | Float64 | Mean over the player's offensive possessions of his four teammates' padded points per 100 (pts_rel). |
| `tm_ast` | Float64 | Mean over the player's offensive possessions of his four teammates' padded assists per 100. |
| `tm_reb` | Float64 | Mean over the player's offensive possessions of his four teammates' padded rebounds per 100. |
| `opp_pts` | Float64 | Mean over the player's offensive possessions of the five defenders' padded points per 100. |
| `opp_ast` | Float64 | Mean over the player's offensive possessions of the five defenders' padded assists per 100. |
| `opp_reb` | Float64 | Mean over the player's offensive possessions of the five defenders' padded rebounds per 100. |

**Example**

```python
from sportsdataverse.nba.nba_box_prior import box_prior_features
feats = box_prior_features(logs["player"], logs["team"], possessions, bio)
```

### box_prior_training_seasons {#box_prior_training_seasons}

`box_prior_training_seasons(season: 'int', available: 'Iterable[int]') -> 'list[int]'`

The seasons a prior for `season` may learn from: earlier ones, or every other one through 2005.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | The season (end year) the prior is for. |
| `available` | `Iterable[int]` |  | Seasons with a complete career-target fit (complete regular seasons). |

**Returns**

Sorted season list, never containing `season`.

### calibrate_pts_per_win {#calibrate_pts_per_win}

`calibrate_pts_per_win(team_season: 'pl.DataFrame') -> 'float'`

Regress team wins on season point margin; return points-per-marginal-win.

Fits `wins ~ total_margin` via ordinary least squares over one (or more,
pooled) season's team-level rows and returns `1 / slope` — the amount of
full-season point differential associated with one additional win. This is
`nba_war`'s `pts_per_win` input.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_season` | `DataFrame` |  | One row per team-season with `team_id` (any dtype), `wins` (numeric), and `total_margin` (numeric — the team's full-season point differential: points scored minus points allowed across all its games, NOT a per-game average). |

**Returns**

`float` points of season margin per marginal win.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `team_season` has fewer than 3 rows (a degenerate regression), if `total_margin` has zero variance, or if the fitted slope is exactly zero (undefined points-per-win). |

**Example**

```python
from sportsdataverse.nba.nba_war import calibrate_pts_per_win
pts_per_win = calibrate_pts_per_win(team_standings)  # team_id/wins/total_margin
print(pts_per_win)
```

### calibrate_replacement_level {#calibrate_replacement_level}

`calibrate_replacement_level(ratings: 'pl.DataFrame', poss: 'pl.DataFrame', *, pts_per_win: 'float', target_total_war: 'float', rating_col: 'str' = 'rating', poss_col: 'str' = 'poss') -> 'float'`

Solve for the `replacement_level` that makes summed league WAR hit a target.

WAR is affine in `replacement_level`:
`war_i = (rating_i - replacement) * poss_i / 100 / pts_per_win`. Summed
over all players this is a single linear equation in `replacement_level`;
this function solves it in closed form (not an iterative search) for the
`replacement_level` that makes `sum(war_i) == target_total_war`.

`target_total_war` is a value the CALLER computes from real standings
(e.g. total league wins above a chosen replacement-team win percentage) —
this function does not assume or invent any such win-percentage convention.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | Frame with `player_id` and `rating_col`. |
| `poss` | `DataFrame` |  | Frame with `player_id` and `poss_col` (total possessions played). |
| `pts_per_win` | `float` |  | Points of season margin per marginal win (`calibrate_pts_per_win`'s output). |
| `target_total_war` | `float` |  | The desired sum of every player's WAR. |
| `rating_col` | `str` | `'rating'` | Column in `ratings` holding the per-100-possession rating. |
| `poss_col` | `str` | `'poss'` | Column in `poss` holding total possessions played. |

**Returns**

`float` replacement_level solving the equation exactly.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `ratings` and `poss` share no `player_id` (an inner join on two non-empty frames producing zero rows — a probable id-source/dtype mismatch), or if the resulting total possession weight sums to zero (the equation is then degenerate). |

**Example**

```python
from sportsdataverse.nba.nba_war import calibrate_replacement_level
repl = calibrate_replacement_level(
    ratings, poss, pts_per_win=250.0, target_total_war=300.0,
)
```

### career_rapm_target {#career_rapm_target}

`career_rapm_target(grams: 'Mapping[int, SeasonGram]', ages: 'pl.DataFrame', *, seasons: 'Sequence[int]', center: 'int', half_life: 'float' = 4.0, penalty: 'float | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Age-adjusted, time-weighted career RAPM over `seasons`: the box prior's training target.

One ridge fit over every possession of `seasons`. Each player has one offensive and one defensive
career coefficient (penalty `penalty`, the plain-RAPM per-possession penalty by default); the
on-court players' ages enter through a quadratic aging curve per side, and each season has its own
points-per-possession level (both unpenalized). A season's possessions are weighted
`0.5 ** (|t - center| / half_life)`, so the career rating leans toward the seasons near
`center`. The fit is assembled from the per-season `SeasonGram` blocks (no possession is
re-read) and solved by one Cholesky factorization.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `grams` | `Mapping[int, SeasonGram]` |  | `{season: SeasonGram}`; must hold every season of `seasons`. |
| `ages` | `DataFrame` |  | `player_id` (Int64), `season` (Int64), `age` (Float64) rows. A missing age is taken as `BOX_PRIOR_AGE_CENTER`. |
| `seasons` | `Sequence[int]` |  | The seasons to fit over. |
| `center` | `int` |  | The season the time weight is centred on (the season the prior is for). |
| `half_life` | `float` | `4.0` | Half-life of the time weight, in seasons. |
| `penalty` | `float \| None` | `None` | Ridge penalty on the career coefficients; default the NBA plain-RAPM penalty. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per player-season of `seasons`: `player_id` (Int64), `season` (Int64), `o_target` / `d_target` (Float64, per 100, `d` positive = good defense), `off_poss` / `def_poss` (Float64, that season's possessions).

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | NBA stats person id. |
| `season` | Int64 | Season (end year) of the row. |
| `o_target` | Float64 | Career offensive RAPM per 100 plus the league aging curve at the player's age that season: the box prior's offensive target. |
| `d_target` | Float64 | Career defensive RAPM per 100 (positive = good defense) plus the defensive aging curve at his age that season. |
| `off_poss` | Float64 | Offensive possessions that season. |
| `def_poss` | Float64 | Defensive possessions that season. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `seasons` is empty or a season has no gram. |

**Example**

```python
from sportsdataverse.nba.nba_box_prior import box_prior_training_seasons, career_rapm_target
train = box_prior_training_seasons(2015, grams)
target = career_rapm_target(grams, ages, seasons=train, center=2015)
```

### current_impact_prior {#current_impact_prior}

`current_impact_prior(player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame', possessions: 'pl.DataFrame', bio: 'pl.DataFrame', model: 'BoxPriorModel', as_of: 'dt.date | str', *, window_days: 'int' = 365, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The box prior v2 as of a date: features from the box lines of the `window_days` before it.

Only games dated before `as_of` (and on or after `as_of - window_days`) are read, so the prior holds
nothing from the date itself or later. The logs and possessions may span two seasons.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | Per-player-game box lines (`leaguegamelog` `P`) with `game_id` and `game_date`. |
| `team_logs` | `DataFrame` |  | Per-team-game lines (`leaguegamelog` `T`). |
| `possessions` | `DataFrame` |  | Possession+lineup rows of the same games (`game_id`, `points`, the lineups). |
| `bio` | `DataFrame` |  | `player_id`, `age`, `height_in`, `position_num` for the season of `as_of`. |
| `model` | `BoxPriorModel` |  | The `~sportsdataverse.nba.nba_box_prior.BoxPriorModel` for that season (one that did not learn from it: `~sportsdataverse.nba.nba_box_prior.box_prior_training_seasons`). |
| `as_of` | `date \| str` |  | The date the prior is for. |
| `window_days` | `int` | `365` | Days of box lines to score. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`CURRENT_IMPACT_PRIOR_SCHEMA`: one row per player on court in the window, `prior_date` = `as_of`, `o_prior` / `d_prior` per 100 (`d` positive = good defense). Zero rows when the window has no games.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | NBA stats person id. |
| `prior_date` | Date | The date the prior is for: it reads only box lines of games dated before it (and within the window before it). |
| `o_prior` | Float64 | Box prior v2 offense per 100, scored on the window's box lines with the season's out-of-fold rule. |
| `d_prior` | Float64 | Box prior v2 defense per 100 (positive = good defense), scored the same way. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `game_id` has a different dtype in the logs and the possessions, or the window has games but none of their possessions (a silent empty prior otherwise). A window game without possessions is left out of the box lines too, so both come from the same games. |

**Example**

```python
from sportsdataverse.nba.nba_current_impact import current_impact_prior
prior = current_impact_prior(logs_p, logs_t, possessions, bio, model_2025, "2024-10-22")
```

### darko_forecast_accuracy {#darko_forecast_accuracy}

`darko_forecast_accuracy(panel: 'pl.DataFrame', ages: 'pl.DataFrame', *, aging_curve: "'AgingCurve | None'" = None, process_var: "'float | None'" = None, obs_base: "'float | None'" = None, min_history: 'int' = 1) -> 'ForecastResult'`

Holdout forecast accuracy: for each transition, forecast N+1 from history <= N vs actual.

For each player and each split at index `t` (prefix seasons `0..t` used to forecast
season `t+1`), run the Kalman filter on the prefix then forecast; the baseline is
carry-forward (`ratings[t]`).  Global `aging_curve` and `(q, obs_base)` are fit on
the full panel (low-dim parameters — standard practice; the holdout is on each player's
rating-history prefix).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `panel` | `DataFrame` |  | `player_id`, `season`, `rating` (+ optional `weight`) panel. |
| `ages` | `DataFrame` |  | `player_id`, `season`, `age`. |
| `aging_curve` | `AgingCurve \| None` | `None` | Fitted `AgingCurve`; fit from `panel` if None. |
| `process_var` | `float \| None` | `None` | Kalman process variance `q`; MLE-fit from `panel` if None. |
| `obs_base` | `float \| None` | `None` | Kalman base observation variance; MLE-fit from `panel` if None. |
| `min_history` | `int` | `1` | Minimum prefix length before a forecast is scored (default 1). |

**Returns**

`ForecastResult` with `forecast_rmse` / `forecast_corr` vs the actual next-season rating, `baseline_rmse` = carry-forward RMSE, and `n_forecasts` (total held-out transitions across all players).

**Example**

```python
from sportsdataverse.nba.nba_darko import darko_forecast_accuracy
res = darko_forecast_accuracy(rating_panel, ages_panel)
print(res.forecast_rmse, res.baseline_rmse, res.forecast_corr)

# Pass pre-fitted params to skip the global MLE step

from sportsdataverse.nba.nba_darko import fit_aging_curve, _fit_noise_params
curve = fit_aging_curve(panel, ages)
q, ob = _fit_noise_params(panel, ages, curve)
res = darko_forecast_accuracy(panel, ages, aging_curve=curve, process_var=q, obs_base=ob)
```

### decay_weights {#decay_weights}

`decay_weights(game_date: 'pl.Series', asof: 'Optional[datetime.date]', half_life_days: 'float') -> 'np.ndarray'`

Exponential time-decay sample weights `w = 0.5 ** (days_ago / half_life)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_date` | `Series` |  | Per-possession game dates (`pl.Date` Series), aligned row-for-row with the design the weights will be applied to. |
| `asof` | `Optional[date]` |  | Reference "today". `None` disables decay (all weights `1.0`). Games dated after `asof` are clamped to `days_ago = 0` (weight `1.0`); callers that want a strict as-of cutoff must filter first. |
| `half_life_days` | `float` |  | Days at which a possession's weight halves. Must be > 0. |

**Returns**

Float64 array of weights, one per row of `game_date`.

**Example**

```python
import datetime
import polars as pl
from sportsdataverse.nba.nba_rapm_variants import decay_weights

dates = pl.Series("game_date", [datetime.date(2023, 1, 1)])
w = decay_weights(dates, datetime.date(2023, 1, 31), half_life_days=30.0)
print(round(float(w[0]), 3))  # 0.5
```

### expected_possessions {#expected_possessions}

`expected_possessions(home_pace: 'float', away_pace: 'float', *, league_id: 'str' = '00') -> 'float'`

Expected possessions for a matchup (Pythagorean-tempo blend).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_pace` | `float` |  | Home team's adjusted pace (possessions/game). |
| `away_pace` | `float` |  | Away team's adjusted pace. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League -- selects the league's baseline pace. |

**Returns**

Expected possessions for the matchup.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import expected_possessions
expected_possessions(100.0, 98.0)
```

### external_validity {#external_validity}

`external_validity(ratings: 'pl.DataFrame', oracle: 'pl.DataFrame', *, rating_col: 'str', oracle_col: 'str', join: 'str' = 'id', ratings_id_col: 'str' = 'player_id', oracle_id_col: 'str' = 'player_id', ratings_name_col: 'str' = 'player_name', oracle_name_col: 'str' = 'player_name', n_permutations: 'int' = 200, seed: 'int' = 0) -> 'ExternalValidityResult'`

Oracle 5: correlate a model's ratings against a published external metric.

`join="id"` inner-joins on `ratings_id_col`/`oracle_id_col` (both
cast to Int64 defensively, per the project's join-key dtype discipline).
`join="name"` normalizes both name columns with
`~sportsdataverse.nba.nba_oracle_data.normalize_player_name` and
inner-joins on the normalized key -- the DARKO-family case: no shared id,
a brittle display-name join whose `coverage_pct` is the signal to watch.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | The model's own per-player ratings frame (from `nba_rapm`, `nba_adj_rapm`, `nba_spm`, `nba_darko`, etc.). |
| `oracle` | `DataFrame` |  | A tidy oracle frame from one of the `nba_oracle_data` loaders. |
| `rating_col` | `str` |  | Column in `ratings` holding the model's rating value. |
| `oracle_col` | `str` |  | Column in `oracle` holding the published metric value. |
| `join` | `str` | `'id'` | `"id"` (default) or `"name"`. |
| `ratings_id_col` | `str` | `'player_id'` | Player-id column name in `ratings` (`join="id"`). |
| `oracle_id_col` | `str` | `'player_id'` | Player-id column name in `oracle` (`join="id"`). |
| `ratings_name_col` | `str` | `'player_name'` | Player-name column name in `ratings` (`join="name"`). |
| `oracle_name_col` | `str` | `'player_name'` | Player-name column name in `oracle` (`join="name"`). |
| `n_permutations` | `int` | `200` | Number of random shuffles for the self-computed null ceiling. |
| `seed` | `int` | `0` | RNG seed for the permutation shuffle. |

**Returns**

`ExternalValidityResult`. `corr` and `permutation_p95` are `nan` when fewer than 3 rows matched; `coverage_pct` is `0.0` when `ratings` is empty.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `join` is not `"id"` or `"name"`. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_oracle_data import load_rapm_ryan_davis
from sportsdataverse.nba.nba_model_validation import external_validity

oracle = load_rapm_ryan_davis(f"{oracle_dir}/rapm_ryan_davis.csv").filter(
    pl.col("season") == "2022-23"
)
res = external_validity(ratings, oracle, rating_col="rapm", oracle_col="RAPM")
print(res.corr, res.coverage_pct)

# A name-keyed family (DARKO)

res = external_validity(
    ratings, darko_oracle, rating_col="projected_rating", oracle_col="dpm",
    join="name", ratings_name_col="player_name", oracle_name_col="player_name",
)
```

### fit_aging_curve {#fit_aging_curve}

`fit_aging_curve(panel: 'pl.DataFrame', ages: 'pl.DataFrame', *, smooth: 'int' = 3) -> 'AgingCurve'`

Fit the aging curve by the delta method: avg YoY rating change grouped by starting age.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `panel` | `DataFrame` |  | `player_id`, `season`, `rating` (per-player-season ratings). |
| `ages` | `DataFrame` |  | `player_id`, `season`, `age`. |
| `smooth` | `int` | `3` | Odd window for a centered moving average over ages (1 = no smoothing). |

**Returns**

An `AgingCurve` mapping each integer starting age to its mean YoY delta.

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_darko import fit_aging_curve

panel = pl.DataFrame({"player_id": [1, 1], "season": [2020, 2021], "rating": [10.0, 11.0]})
ages = pl.DataFrame({"player_id": [1, 1], "season": [2020, 2021], "age": [24.0, 25.0]})
curve = fit_aging_curve(panel, ages, smooth=1)
print(curve.delta(24))  # ~1.0
```

### fit_reml_prior_ridge {#fit_reml_prior_ridge}

`fit_reml_prior_ridge(X: 'csr_matrix', y: 'np.ndarray', prior_mean: 'np.ndarray', *, lambdas: 'tuple[float, float] | None' = None, bounds: 'tuple[float, float]' = (10.0, 10000000.0)) -> 'RemlFit'`

Ridge toward `prior_mean` with one penalty for offense and one for defense, chosen by REML.

The model is `y = c + X b + e`, `e ~ N(0, s2)`, with each offensive coefficient
`b_j ~ N(prior_j, s2 / lambda_o)` and each defensive one `~ N(prior_j, s2 / lambda_d)`, and a flat
prior on the intercept `c`. The restricted (REML) likelihood integrates `b` and `c` out; with
`s2` profiled, `-2` times it is, up to a constant,

.. code-block:: text

    (n - 1) log RSS + log |M| - P log lambda_o - P log lambda_d,
    M = Xc'Xc + diag(lambda),  RSS = min_d |yc' - Xc d|^2 + d' diag(lambda) d,

`Xc` / `yc'` the centred design and prior-residualized response. Its gradient in
`log lambda_o` is `lambda_o ((n - 1) |d_o|^2 / RSS + tr_o(M^-1)) - P` (and likewise for defense),
so a bounded quasi-Newton search finds the pair in a few dozen `2P x 2P` Cholesky factorizations.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `csr_matrix` |  | Sparse design `(n, 2P)` from `~sportsdataverse.nba.nba_rapm.build_rapm_design`. |
| `y` | `ndarray` |  | Possession points `(n,)`. |
| `prior_mean` | `ndarray` |  | Prior mean per possession `(2P,)` (offense `o / 100`, defense `-d / 100`). |
| `lambdas` | `tuple[float, float] \| None` | `None` | `(lambda_o, lambda_d)` to fit at instead of the REML choice. |
| `bounds` | `tuple[float, float]` | `(10.0, 10000000.0)` | Search bounds on each penalty. |

**Returns**

A `RemlFit`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `X` has no rows or an odd number of columns, or a given penalty is not positive. |

**Example**

```python
from sportsdataverse.nba.nba_rapm import build_rapm_design
from sportsdataverse.nba.nba_reml_rapm import fit_reml_prior_ridge
X, y, pids = build_rapm_design(possessions)
fit = fit_reml_prior_ridge(X, y, prior_mean)
print(fit.lambda_o, fit.lambda_d)
```

### game_excitement {#game_excitement}

`game_excitement(scored: 'pl.DataFrame', *, league_id: 'str | None' = None, league: 'str | None' = None, regulation_periods: 'int | None' = None, completed: 'bool | None' = None) -> 'dict[str, Any]'`

How exciting one game was, with no direction: the game excitement index, lead changes and the biggest swing.

Football's GEI (Game on Paper's `calculateGEI` / `excitement.py`) on the basketball WP
series: `GEI_SCALE[league] / n * sum(|home WP change|)` over the game's `n` plays. Each
play's change is measured from the previous play's home WP (the pregame prior for the first
play). The last play of a completed game swings to the result (1 if home won, 0 if it lost, 0.5
for a tie); while the game is live the swings so far are summed. The scale is the league's mean
plays per game (`GEI_SCALE`), so the index reads as the total WP movement of a game of
average length.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored` | `DataFrame` |  | One game from `score_win_prob`, in play order (needs `home_win_prob`, `pregame_home_prob`, `home_score`, `away_score`; `type_text` and `period_number` to tell a completed game). |
| `league_id` | `str \| None` | `None` | Legacy alias: `"00"` NBA / `"10"` WNBA. |
| `league` | `str \| None` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |
| `regulation_periods` | `int \| None` | `None` | The game's regulation periods; default from the frame's `season` via `game_regulation_periods`. |
| `completed` | `bool \| None` | `None` | Whether the game is final. `None` reads it from the plays: final when a row ends the game ("End Game" / "End Period" from the last regulation period on, score not tied), the rule `score_win_prob` settles WP on. Without `type_text` it is live. |

**Returns**

`{"gei", "lead_changes", "max_swing_pts", "completed", "winner_team_id", "winner_low_wp", "winner_low_period", "winner_low_clock"}`: the index (`None` for no plays), how often the leader flipped (a tie is not a lead, so A ahead, tied, then B ahead counts once), the largest of the changes the index sums in percentage points (the last play's swing to the result included; `None` for no plays), whether the last play was taken to the result, and the comeback low: the winner's `home_team_id` / `away_team_id`, its lowest WP after any play (0-1, the prior is not a play) and that play's (the earliest at the low) `period_number` and `clock_display_value`; `None` for a column the frame lacks, and all four `None` for a game not completed or ending tied.

**Raises**

| Exception | Description |
|---|---|
| `polars.exceptions.ColumnNotFoundError` | If a required column is missing. |
| `ValueError` | If the league is unknown, `league` contradicts `league_id`, or `scored` holds more than one `game_id`. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_page import game_excitement, score_win_prob
from sportsdataverse.nba.nba_loaders import load_nba_pbp
one = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
game_excitement(score_win_prob(one))

# A men's college game

from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
one = load_mbb_pbp([2026]).filter(pl.col("game_id") == 401830342)
game_excitement(score_win_prob(one, league="mbb"), league="mbb")
```

### game_lines {#game_lines}

`game_lines(lines_checkpoints: 'pl.DataFrame', crosswalk: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league: 'str | None' = None) -> 'pl.DataFrame'`

Per ESPN game: opening and closing spread, total and moneylines, the no-vig market WP, and the result.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lines_checkpoints` | `DataFrame` |  | The odds store's `lines_checkpoints` rows (any sports and years: rows of other sports are ignored). Needs `event_id`, `sport_key`, `home_team`, `away_team`, `bookmaker_key`, `market_key`, `outcome_name`, `open_price` / `open_point`, `close_price` / `close_point` and `close_mins_before`. |
| `crosswalk` | `DataFrame` |  | The odds store's `espn_crosswalk` (`sport_key`, `event_id`, `espn_game_id`, `match_method`). |
| `schedule` | `DataFrame` |  | The league's ESPN schedule (`load_<league>_schedule`): `game_id`, `season`, `home_score`, `away_score`, `status_type_completed`. Only its games are returned, with its `season`. |
| `league` | `str \| None` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |

**Returns**

One row per game with a line, `GAME_LINES_SCHEMA`: `game_id`, `season`, the odds `event_id` (the one with the most quotes when the crosswalk maps two events to a game), `n_books` (books with a closing quote), `spread_home_open` / `_close` (betting convention, negative when home is favoured), `total_open` / `_close`, `ml_home` / `ml_away` `_open` / `_close` (American), `market_home_wp` / `market_away_wp` `_open` / `_close` (no vig, the pair sums to 1), and for completed games `home_margin`, `ats_result` (`"home"` / `"away"` / `"push"`: who covered the closing spread) and `total_result` (`"over"` / `"under"` / `"push"` against the closing total). A column the books did not quote is null; an empty frame with those columns when nothing matches.

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | ESPN game id, the join key to the schedule, play-by-play and box-score frames. |
| `season` | Int32 | ESPN season of the game, from the schedule. |
| `event_id` | String | Odds-store event id the lines come from (the one with the most quotes when two map to the game). |
| `n_books` | UInt32 | Sportsbooks with a closing (pre-tip) quote on the game. |
| `spread_home_open` | Float64 | Median opening spread on ESPN's home team, betting convention (negative when home is favoured). |
| `spread_home_close` | Float64 | Median closing spread on ESPN's home team, betting convention (negative when home is favoured). |
| `total_open` | Float64 | Median opening game total (over/under points). |
| `total_close` | Float64 | Median closing game total (over/under points). |
| `ml_home_open` | Float64 | Median opening moneyline on the home team, American odds (median taken on the decimal price). |
| `ml_away_open` | Float64 | Median opening moneyline on the away team, American odds. |
| `ml_home_close` | Float64 | Median closing moneyline on the home team, American odds. |
| `ml_away_close` | Float64 | Median closing moneyline on the away team, American odds. |
| `market_home_wp_open` | Float64 | No-vig home win probability from the opening moneylines, median over books. |
| `market_away_wp_open` | Float64 | No-vig away win probability from the opening moneylines (1 - market_home_wp_open). |
| `market_home_wp_close` | Float64 | No-vig home win probability from the closing moneylines, median over books. |
| `market_away_wp_close` | Float64 | No-vig away win probability from the closing moneylines (1 - market_home_wp_close). |
| `home_margin` | Int32 | Final home score minus away score; null until the game is completed. |
| `ats_result` | String | Who covered the closing spread (home, away or push); null until completed. |
| `total_result` | String | The final total against the closing total (over, under or push); null until completed. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the league is unknown. |
| `polars.exceptions.ColumnNotFoundError` | If a required column is missing. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_lines import game_lines
from sportsdataverse.nba.nba_loaders import load_nba_schedule
odds = "odds-data/odds"  # a local checkout of the odds store
lines = game_lines(
    pl.read_parquet(f"{odds}/lines_checkpoints_202[45].parquet"),
    pl.read_parquet(f"{odds}/espn_crosswalk.parquet"),
    load_nba_schedule([2025]),
    league="nba",
)

# Against the spread

lines.group_by("ats_result").len()
```

### game_minutes {#game_minutes}

`game_minutes(periods: 'int', *, league_id: 'Optional[str]' = None, league: 'Optional[str]' = None, regulation_periods: 'Optional[int]' = None) -> 'int'`

Minutes played in a game of `periods` periods: the league's regulation length plus five per overtime.

Regulation is 48 minutes in the NBA and 40 in the WNBA and both college leagues (two 20-minute
halves or four 10-minute quarters); every overtime is 5 minutes. A period past the game's
`regulation_periods` is an overtime, so period 3 of a halves game is the first overtime.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `periods` | `int` |  | Highest period number played. |
| `league_id` | `Optional[str]` | `None` | Legacy alias: `"00"` NBA / `"10"` WNBA. |
| `league` | `Optional[str]` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |
| `regulation_periods` | `Optional[int]` | `None` | The game's regulation periods (2 or 4); default `game_regulation_periods` for the league without a season (MBB 2, otherwise 4). |

**Returns**

Game minutes.

**Raises**

| Exception | Description |
|---|---|
| `TypeError` | If `periods` is `None` or another non-numeric type. |
| `ValueError` | If `periods` is a string that is not an integer, the league is unknown, or `league` contradicts `league_id`. |

**Example**

```python
from sportsdataverse.nba.nba_game_page import game_minutes
game_minutes(5)  # 53

# A double-overtime men's college game (two halves + two OTs)

game_minutes(4, league="mbb")  # 50
```

### game_page_frames {#game_page_frames}

`game_page_frames(final: 'dict', *, league_id: 'Optional[str]' = None, league: 'Optional[str]' = None, rated_home_prob: 'Optional[float]' = None, shot_cells: 'Optional[pl.DataFrame]' = None) -> 'dict[str, object]'`

Everything the Game on Paper basketball game page needs, from one `espn_<league>_pbp` payload.

Reads the game's regulation (`format.regulation.periods`, else the season cutoff of
`game_regulation_periods`) and neutral site (`header.competitions[0].neutralSite`),
runs the league's hoopR / wehoop converters (`helper_<league>_play_by_play` /
`helper_<league>_team_box` / `helper_<league>_player_box`) and then
`game_page_tables`. The sdv-db nightly stage calls `game_page_tables` directly on
the warehouse's copies of the same frames.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `final` | `dict` |  | The dict `espn_<league>_pbp(..., return_parsed=False)` returns (for example `~sportsdataverse.nba.nba_pbp.espn_nba_pbp`). |
| `league_id` | `Optional[str]` | `None` | Legacy alias: `"00"` NBA / `"10"` WNBA. |
| `league` | `Optional[str]` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |
| `rated_home_prob` | `Optional[float]` | `None` | The game's rated pregame home win probability, the prior when there is no line (NBA / WNBA: `~sportsdataverse.nba.nba_rated_prior.nba_rated_priors`). |
| `shot_cells` | `Optional[DataFrame]` | `None` | The league's shot counts as of the game date (`~sportsdataverse.nba.nba_game_expected.shot_cells`), for the expected points of `team_game_expected`; `None` leaves its expected columns null. |

**Returns**

`{"pbp", "team_box", "team_game_rates", "player_box", "player_game_rates", "team_game_splits", "player_game_splits", "team_game_expected", "periods", "prior", "league", "regulation_periods", "period_kind", "neutral_site"}`; see `game_page_tables` for the shared keys. Missing boxes retain empty identifier columns. Team rates need both team rows, and player rates also need player rows. Unavailable rates are empty frames with their documented columns.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the league is unknown or `league` contradicts `league_id`. |
| `KeyError` | If `final` lacks the `header` or `plays` keys the converters read. |
| `polars.exceptions.ColumnNotFoundError` | If a converter frame lacks a column the derivations need. |

**Example**

```python
from sportsdataverse.nba.nba_game_page import game_page_frames
from sportsdataverse.nba.nba_pbp import espn_nba_pbp
frames = game_page_frames(espn_nba_pbp(401766458, return_parsed=False))
frames["pbp"].select("text", "home_win_prob", "wp_delta").head()

# A men's college game

from sportsdataverse.mbb.mbb_pbp import espn_mbb_pbp
frames = game_page_frames(espn_mbb_pbp(401830342, return_parsed=False), league="mbb")
frames["regulation_periods"], frames["period_kind"], frames["periods"]  # (2, 'half', 4)
```
