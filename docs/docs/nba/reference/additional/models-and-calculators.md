---
title: "NBA — additional Python functions — Models and calculators: AdjRapmModel–predict_margin"
sidebar_label: "Models and calculators: AdjRapmModel–predict_margin"
sidebar_position: 6
description: "NBA — additional Python functions — Models and calculators: AdjRapmModel–predict_margin — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Models and calculators: AdjRapmModel–predict_margin

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
| `alphas` | `ndarray` | `<factory>` | RidgeCV alpha grid forwarded to fit_prior_ridge`. |
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

Delegate to fit_prior_ridge` using this model's hyperparameters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `csr_matrix` |  | Sparse design `(n, 2P)` from `~sportsdataverse.nba.nba_rapm.build_rapm_design`. |
| `y` | `ndarray` |  | Possession points `(n,)`. |
| `prior_mean` | `ndarray` |  | Per-possession prior mean `(2P,)` built by the harness. |

**Returns**

`~sportsdataverse.nba.nba_model_validation.FitResult` with posterior of shape `(n_samples, 2P)`.

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
| `hfa` | `float` |  | Home-court advantage in points (fitted on the as-of-date backtest for the league; see `dev/nba_prediction/fit_pregame.py`). |
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

**Example**

```python
from sportsdataverse.nba.nba_team_ratings import adjust_pace, raw_game_efficiency
pace = adjust_pace(raw_game_efficiency(sched, box))
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

**Example**

```python
import datetime as dt
from sportsdataverse._common.metrics import as_of_ratings_split
as_of_ratings_split(results, dt.date(2023, 9, 8))
```

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

**Example**

```python
from sportsdataverse.nba.nba_war import calibrate_replacement_level
repl = calibrate_replacement_level(
    ratings, poss, pts_per_win=250.0, target_total_war=300.0,
)
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

### get_constants {#get_constants}

`get_constants(league_id: 'str') -> 'LeagueConstants'`

Return the `LeagueConstants` for a `league_id`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_id` | `str` |  | stats.nba.com league id -- `"00"` NBA, `"10"` WNBA, `"20"` G-League. |

**Returns**

The league's `LeagueConstants`.

**Example**

```python
from sportsdataverse.nba.nba_prediction_constants import get_constants
get_constants("00").hfa
```

### get_shrinkage_k {#get_shrinkage_k}

`get_shrinkage_k(league_id: 'str') -> 'float'`

Shooter-talent shrinkage `k` for a league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_id` | `str` |  | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |

**Returns**

The pseudo-attempt shrinkage constant (fitted split-half).

**Example**

```python
from sportsdataverse.nba.nba_shot_value_constants import get_shrinkage_k
get_shrinkage_k("00")
```

### in_game_features {#in_game_features}

`in_game_features(pbp: 'pl.DataFrame', pregame_home_prob: 'float') -> 'pl.DataFrame'`

Per-play in-game win-probability features from a `load_nba_pbp` frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame with `start_game_seconds_remaining`, `home_score`, `away_score`, `team_id` (event team) and `home_team_id` (the `load_nba_pbp` schema). |
| `pregame_home_prob` | `float` |  | The pregame home win probability (e.g. from `win_prob_from_margin`), encoded as a constant logit column. Clipped to `[1e-6, 1 - 1e-6]` so a saturated CDF (exact 0/1) cannot crash the logit. |

**Returns**

One row per input play: `score_diff` (home - away), `sec_left` (clipped at 0 -- overtime plays count as 0 seconds left), `sqrt_sec_left`, `pregame_logit`, `home_has_ball` (`Int8`; dead-ball / unknown-team plays are 0).

**Example**

```python
from sportsdataverse.nba.nba_game_predict import in_game_features
from sportsdataverse.nba.nba_loaders import load_nba_pbp
pbp = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
feats = in_game_features(pbp, 0.62)
```

### luck_adjusted_response {#luck_adjusted_response}

`luck_adjusted_response(possessions: 'pl.DataFrame', shooting: 'pl.DataFrame', player_rates: 'Optional[dict[int, tuple[float, float]]]' = None, *, fg3_k: 'float' = 100.0, ft_k: 'float' = 50.0) -> 'pl.DataFrame'`

Attach a per-possession `la_points` expected-points response.

**DECISION 2/4**: `la_points = 2*fg2m + 3*Σ_shooter fg3a·p̂3 + Σ_shooter fta·p̂ft`
(offense-only, "one_way"). 2-pt makes stay realized. `p̂` come from
`player_rates` when given, else shrunk_shooter_rates` on `shooting`.

**Defense-shooter exclusion (bugfix)**: `shooting`
(`~sportsdataverse.nba.nba_possessions.build_possession_shooting`)
deliberately retains defense-team shooters in a possession group — e.g. a
defensive technical free throw shooter — because it is a per-shooter
companion frame, not a team-attributed one (that's why it carries its own
`team_id` column). The expected-points sum is offense-only by
definition (**DECISION 2**), so *before* aggregating `exp_extra` this
function joins `possessions[["game_id", "possession_number",
"offense_team_id"]]` onto `shooting` and filters to
`team_id == offense_team_id`, dropping any defense-team shooter row.
Without this filter a defense tech-FT's `fta·p̂ft` term leaks into the
offense's `la_points`, inflating it (reproduced: 2.9 vs. the
offense-only-correct 2.0 for a single offense 2-pt make plus one defense
tech FT).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup frame carrying team-level `fg2m` and the join keys `game_id` + `possession_number` + `offense_team_id`. |
| `shooting` | `DataFrame` |  | Per-(possession, shooter) frame (`build_possession_shooting`), which may include defense-team shooter rows (e.g. technical FTs) — filtered out here before aggregation. |
| `player_rates` | `Optional[dict[int, tuple[float, float]]]` | `None` | Optional `{player_id: (p3, pft)}` override (e.g. planted truth in tests); `None` → shrink from `shooting`. |
| `fg3_k` | `float` | `100.0` | 3-point shrinkage pseudo-count, forwarded to shrunk_shooter_rates` when `player_rates` is `None`. |
| `ft_k` | `float` | `50.0` | Free-throw shrinkage pseudo-count, forwarded to shrunk_shooter_rates` when `player_rates` is `None`. |

**Returns**

`possessions` with an added `la_points: Float64` column (same rows, same order). Empty `possessions` → returned unchanged with an empty `la_points` column.

**Example**

```python
from sportsdataverse.nba.nba_rapm_variants import luck_adjusted_response
out = luck_adjusted_response(possessions_df, shooting_df)
print(out["la_points"].mean())

# Planted-truth override for testing

out = luck_adjusted_response(possessions_df, shooting_df, {7: (0.4, 0.8)})
```

### nba_adj_rapm {#nba_adj_rapm}

`nba_adj_rapm(possessions: 'pl.DataFrame', prior: 'Dict[int, Tuple[float, float]]', *, alphas: 'np.ndarray' = array([   100.        ,    268.26957953,    719.685673  ,   1930.69772888,
         5179.47467923,  13894.95494373,  37275.93720315, 100000.        ]), n_samples: 'int' = 200, seed: 'int' = 0, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

One-shot prior-informed RAPM over a possession frame -> per-player ratings.

Builds the sparse design matrix via
`~sportsdataverse.nba.nba_rapm.build_rapm_design`, constructs the
per-possession `prior_mean` vector from `prior`, fits a residualized
ridge with an RTO posterior via fit_prior_ridge`, and returns the
per-player offensive, defensive, and combined adj-RAPM ratings alongside
possession counts.

Sign convention (matches `~sportsdataverse.nba.nba_rapm.nba_rapm`):
`d_adj_rapm` is positive for a good defender (lowers opponent points);
`adj_rapm = o_adj_rapm + d_adj_rapm`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | A possession+lineup frame produced by the possession engine (`game_id`, `offense_team_id`, `points`, `off_player_1..5`, `def_player_1..5`). |
| `prior` | `Dict[int, Tuple[float, float]]` |  | Per-player `{player_id: (o_prior, d_prior)}` in per-100 units. Players absent from `prior` receive a `(0.0, 0.0)` default. |
| `alphas` | `ndarray` | `array([   100.        ,    268.26957953,    719.685673  ,   1930.69772888,
         5179.47467923,  13894.95494373,  37275.93720315, 100000.        ])` | RidgeCV alpha grid for the regularisation strength (default `DEFAULT_RAPM_ALPHAS`). |
| `n_samples` | `int` | `200` | Number of RTO posterior samples (default 200). |
| `seed` | `int` | `0` | RNG seed for the RTO sampler (default 0). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with columns `player_id` (Int64), `o_adj_rapm` (Float64), `d_adj_rapm` (Float64), `adj_rapm` (Float64), `off_poss` (Int64), `def_poss` (Int64).

**Example**

```python
from sportsdataverse.nba import nba_adj_rapm
ratings = nba_adj_rapm(possessions, spm_prior_dict)
print(ratings.sort("adj_rapm", descending=True).head())
```

### nba_aging_curve {#nba_aging_curve}

`nba_aging_curve(*, league: 'str' = 'nba', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Load the bundled per-age value-multiplier curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `age:Int64, rel_value:Float64, peak_age:Float64` (`peak_age` repeated on every row for convenient filtering/joining).

| col_name | type | description |
|---|---|---|
| `age` | integer | Player age (in years). |
| `rel_value` | double | Value multiplier for this age relative to the peak age: a delta-method curve chaining minutes-weighted within-player consecutive-age changes in per-100-possession box-score value, quadratic-smoothed and min-max scaled to [0.4, 1.0], so the peak age is exactly 1.0 and the lowest-valued age 0.4. |
| `peak_age` | double | Age at which rel_value reaches its maximum of 1.0, repeated on every row for filtering and joining (29.0 in the bundled curve). |

**Example**

```python
from sportsdataverse.nba import nba_aging_curve
curve = nba_aging_curve()
print(curve.sort("rel_value", descending=True).head(1))

# Pipeline next step (one line)

curve.filter(pl.col("age").is_between(24, 30))
```

### nba_bpm {#nba_bpm}

`nba_bpm(player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame', positions: 'pl.DataFrame', *, team_adjust: 'bool' = True, granularity: 'str' = 'season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Faithful BPM 2.0 per player, at season or single-game granularity.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | per-player-per-game box lines (`nba_box_logs`'s `player`); must carry `game_id` when `granularity="game"`. |
| `team_logs` | `DataFrame` |  | per-team-per-game lines incl. `plus_minus` (`nba_box_logs`'s `team`); must carry `game_id` when `granularity="game"`. |
| `positions` | `DataFrame` |  | listed positions (`nba_player_positions`): player_id, position_num. |
| `team_adjust` | `bool` | `True` | apply the team adjustment (True) or return raw box-BPM (False). |
| `granularity` | `str` | `'season'` | `"season"` (default) aggregates every row in `player_logs`/ `team_logs` into one row per player. `"game"` runs the exact same pipeline independently per `game_id` (position/role are estimated game-native, mirroring `NbaBpmModel`'s existing fold-native design) and returns one row per (game_id, player_id) with a leading `game_id` column; `gp` is always 1 in this mode. |
| `return_as_pandas` | `bool` | `False` | return pandas instead of polars. |

**Returns**

`"season"`: frame with `player_id`, `obpm`, `dbpm`, `bpm`, `min`, `gp` (Int64 player_id/gp, Float64 obpm/dbpm/bpm/min). `"game"`: the same columns prefixed with `game_id` (Utf8), one row per (game_id, player_id). Empty (that schema) input -> zero-row frame with the same schema; never raises on empty.

**Example**

```python
from sportsdataverse.nba import nba_bpm, nba_box_logs, nba_player_positions
logs = nba_box_logs("2023-24"); pos = nba_player_positions("2023-24")
bpm = nba_bpm(logs["player"], logs["team"], pos)
print(bpm.sort("bpm", descending=True).head())

# Per-game BPM

bpm_game = nba_bpm(logs["player"], logs["team"], pos, granularity="game")
print(bpm_game.filter(pl.col("game_id") == "0022300001").sort("bpm", descending=True))

# Raw (no team adjustment)

bpm_raw = nba_bpm(logs["player"], logs["team"], pos, team_adjust=False)

# Pandas output

bpm_pd = nba_bpm(logs["player"], logs["team"], pos, return_as_pandas=True)
```

### nba_career_trajectory {#nba_career_trajectory}

`nba_career_trajectory(player_values: 'pl.DataFrame', *, league: 'str' = 'nba', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Age-adjust player-season values with the bundled aging curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_values` | `DataFrame` |  | Frame `player_id, age:Int64, value:Float64`. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`player_values` plus `age_adjusted_value` (`value / rel_value(age)`, peak-centered) and `proj_next_value` (`value * rel_value(age+1) / rel_value(age)`). Ages outside the bundled curve's range fall back to `rel_value = 1.0` (no adjustment). Empty input returns the zero-row schema.

**Example**

```python
import polars as pl
from sportsdataverse.nba import nba_career_trajectory
player_values = pl.DataFrame({"player_id": ["1"], "age": [24], "value": [10.0]})
nba_career_trajectory(player_values)
```

### nba_darko {#nba_darko}

`nba_darko(panel: 'pl.DataFrame', ages: 'pl.DataFrame', *, aging_curve: "'AgingCurve | None'" = None, process_var: "'float | None'" = None, obs_base: "'float | None'" = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Project each player's next-season rating via a per-player Kalman filter + aging curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `panel` | `DataFrame` |  | `player_id, season, rating` (+ optional `weight`) — a multi-season rating panel. |
| `ages` | `DataFrame` |  | `player_id, season, age` (from `nba_player_ages`). |
| `aging_curve` | `AgingCurve \| None` | `None` | an `AgingCurve`; fitted from `panel` if None. |
| `process_var` | `float \| None` | `None` | Kalman process variance `q`; MLE-fit from `panel` if None. |
| `obs_base` | `float \| None` | `None` | Kalman base observation variance; MLE-fit from `panel` if None. |
| `return_as_pandas` | `bool` | `False` | return pandas instead of polars. |

**Returns**

`player_id, last_season, forecast_season, filtered_skill, projected_rating, projected_sd`.

**Example**

```python
from sportsdataverse.nba import nba_darko, nba_player_ages
proj = nba_darko(rating_panel, ages_panel)
print(proj.sort("projected_rating", descending=True).head())
```

### nba_decay_rapm {#nba_decay_rapm}

`nba_decay_rapm(possessions: 'pl.DataFrame', *, asof: 'Optional[datetime.date]' = None, half_life_days: 'float' = 180.0, alphas: 'Optional[np.ndarray]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Time-decay RAPM: ridge weighted by `0.5 ** (days_ago / half_life_days)`.

`asof=None` disables decay: every possession is weighted `1.0` and the
fit uses **exactly** plain `~sportsdataverse.nba.nba_rapm.nba_rapm`'s
own schedule (`alphas=DEFAULT_RAPM_ALPHAS`, sklearn's efficient default
LOOCV) so the two agree byte-for-byte (see
`test_decay_rapm_asof_none_equals_plain_rapm`). When `asof` is set,
possessions dated after `asof` are dropped, the remainder is
exponentially down-weighted by age, and the fit switches to the
**oracle** regularization schedule (`oracle_rapm_alphas` evaluated
at the post-filter possession count, `cv=` `ORACLE_RAPM_CV`) per
the binding WP2 ridge-schedule ruling documented in the module docstring.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Multi-season possession+lineup frame. Must carry a `game_date` (`pl.Date`) column when `asof` is not `None`. |
| `asof` | `Optional[date]` | `None` | Reference date; `None` -> unweighted, plain-RAPM-equivalent fit. |
| `half_life_days` | `float` | `180.0` | Weight half-life in days (default 180). |
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override. `None` (default) auto-selects `~sportsdataverse.nba.nba_rapm.DEFAULT_RAPM_ALPHAS` when `asof is None` or `oracle_rapm_alphas` (evaluated at the possession count) when `asof` is set. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `DECAY_RAPM_SCHEMA`. Empty input, or an `asof` that drops every possession, -> zero-row frame.

**Example**

```python
import datetime
from sportsdataverse.nba.nba_rapm_variants import nba_decay_rapm

df = nba_decay_rapm(season_poss, asof=datetime.date(2024, 3, 1), half_life_days=120.0)
print(df.sort("decay_rapm", descending=True).head())

# Plain-RAPM-equivalent (no decay)

df = nba_decay_rapm(season_poss)  # asof=None
```

### nba_draft_model {#nba_draft_model}

`nba_draft_model(draft_year: "'Union[int, list[int]]'", *, league: 'str' = 'nba', college_prior: "'Optional[pl.DataFrame]'" = None, gleague_bridge: 'bool' = False, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Project prospect career value + draft probability from combine measurements.

Loads the draft-combine wrappers for `draft_year` (or each year in the
list), builds the shared combine-feature vector
(`sportsdataverse.nba.nba_draft_constants.build_combine_features`),
and applies the bundled ridge (`proj_career_value`) / logistic
(`draft_prob`) heads fit in `dev/nba_draft/fit_draft_model.py`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `draft_year` | `Union[int, list[int]]` |  | A draft year (e.g. `2019`) or list of years. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"` -- selects the bundled artifact and the combine-wrapper family. |
| `college_prior` | `Optional[DataFrame]` | `None` | Optional frame keyed on `player_id:Utf8` carrying the college-side MBB/WBB player-value spine's `projected_pick` / `box_bpm` / `archetype` (model ⑤, see design doc §3.5). When present and the bundled artifact has matching feature columns, it is left-joined as an extra feature block. This function **never** imports `sportsdataverse.mbb` -- callers pass the frame in. |
| `gleague_bridge` | `bool` | `False` | When `True`, left-joins G-League (`league_id="20"`) bulk production (`gleague_pts`/`gleague_gp`/`gleague_min`) for the draft year's season as extra, forward-looking feature columns. Not part of any bundled artifact's scored features today (joining it never changes `proj_career_value`/`draft_prob`); gracefully absent when the G-League bulk call returns no rows -- never raises. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, draft_year:Int64, proj_career_value:Float64, draft_prob:Float64, projected_pick:Int64, pro_tier:Utf8` — one row per prospect with combine measurements for that class. `projected_pick` is a contiguous 1..N rank within each draft year. Empty/malformed input returns the zero-row schema, never raises.

**Example**

```python
from sportsdataverse.nba import nba_draft_model
board = nba_draft_model(2019)
print(board.sort("proj_career_value", descending=True).head())

# With a college-side prior

board = nba_draft_model(2019, college_prior=mbb_prior_df)

# Pipeline next step (one line)

board.filter(pl.col("pro_tier") == "lottery")
```

### nba_expected_turnovers {#nba_expected_turnovers}

`nba_expected_turnovers(season: 'str', *, league_id: 'str' = '00', base: 'Optional[pl.DataFrame]' = None, player_mix: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Expected TOV + residual ball-security skill from Synergy play-type mix.

`lg_to_rate_t` = poss-weighted league mean of `turnover_freq` for play
type `t`; `expected_tov = scale · Σ_t poss_t · lg_to_rate_t`;
`ball_security_skill = 100·(expected_tov − tov)/poss` (fewer turnovers
than expected ⇒ positive skill -- sign flipped vs. the foul-drawing model).

`scale = Σ actual tov / Σ raw type-mix estimate` is derived from the
fetched season itself (covers players below Synergy's per-type
classification threshold) so `Σ expected_tov ≡ Σ tov` holds exactly.
Swapping each player's own `turnover_freq` in for the league rate
reconstructs their real season TOV almost exactly (slope ~1.03, Spearman
~0.97 on the 2023-24 oracle corpus) -- confirming the column semantics are
correct. The *expected* (league-rate) version necessarily explains less
variance than *actual* (turnover-avoidance is a more individual,
less play-type-bound skill than foul-drawing), so its calibration slope
runs lower than model (3)'s -- see the oracle gate for the observed floor.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `base` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leaguedashplayerstats` (`Base` measure) frame: `player_id`, `tov`, `poss` (bypasses the live fetch). |
| `player_mix` | `Optional[DataFrame]` | `None` | Injected Synergy player-level offensive mix: `player_id`, `play_type`, `poss`, `turnover_freq`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player: `player_id` (Int64), `poss`/`tov`/ `expected_tov`/`ball_security_skill` (Float64). Zero-row frame with this schema when the inputs are empty (sparse-coverage leagues never raise).

**Example**

```python
from sportsdataverse.nba import nba_expected_turnovers
t = nba_expected_turnovers("2023-24")
print(t.sort("ball_security_skill", descending=True).head(10))

# Injected offline (oracle / test) path

t = nba_expected_turnovers("2023-24", base=base_df, player_mix=mix_df)

# Pipeline next step

t.filter(pl.col("poss") >= 200).sort("ball_security_skill", descending=True)
```

### nba_four_factor_rapm {#nba_four_factor_rapm}

`nba_four_factor_rapm(possessions: 'pl.DataFrame', *, alphas: 'Optional[np.ndarray]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Four-factor RAPM: four independent ridge fits (efg/ftr/orbd/tov) on the SAME design.

Each factor is regressed on the identical offense/defense design matrix,
differing only in the per-possession response (FACTOR_RESPONSES`).
Output mirrors the oracle's `RA_*__Off/__Def` layout. **DECISION 5/6/7**
govern the response definitions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup frame carrying team-level `fg2m, fg3m, ftm, oreb, tov` and the ten lineup columns. Must also carry a `points` column -- `~sportsdataverse.nba.nba_rapm.build_rapm_design` (invoked internally) requires it unconditionally even though none of the four factor responses use it. `offense_team_id` is NOT required here (unlike `nba_la_rapm`): none of the four factor responses need the offense-only shooter join. |
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override, shared by all four factor fits. `None` (default) auto-selects `oracle_rapm_alphas` evaluated at the possession count -- the operative WP2 oracle schedule. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `FOUR_FACTOR_SCHEMA` — `{factor}__off` / `{factor}__def` columns per factor, plus possession counts. Empty input → zero-row frame.

**Example**

```python
from sportsdataverse.nba.nba_rapm_variants import nba_four_factor_rapm
ff = nba_four_factor_rapm(season_poss)
print(ff.sort("efg__off", descending=True).head())
```

### nba_in_game_win_prob {#nba_in_game_win_prob}

`nba_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-play home win probability from the bundled in-game model.

Scores `in_game_features` through the committed artifact
(`sportsdataverse/nba/models/nba_in_game_wp.ubj` for NBA -- a shallow
xgboost booster, trained on 2022-23 so the 2023-24 calibration backtest
stays out-of-sample; escalated from a plain logistic that failed the
per-bucket calibration gate).

Gate note: the plan's concurrent oracle (stats.nba.com
`winprobabilitypbp` HOME_PCT) is a dead endpoint, so this model is
validated ONLY on realized-outcome calibration, not against a native WP
feed. See the fixtures README + SDD ledger.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play for ONE game in the `load_nba_pbp` schema (`start_game_seconds_remaining`, `home_score`, `away_score`, `team_id`, `home_team_id`). |
| `pregame_home_prob` | `float` |  | Pregame home win probability (e.g. from `win_prob_from_margin`). |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per play: the five feature columns plus `home_win_prob`.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import nba_in_game_win_prob
from sportsdataverse.nba.nba_loaders import load_nba_pbp
pbp = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
wp = nba_in_game_win_prob(pbp, 0.62)
```

### nba_la_rapm {#nba_la_rapm}

`nba_la_rapm(possessions: 'pl.DataFrame', shooting: 'pl.DataFrame', player_rates: 'Optional[dict[int, tuple[float, float]]]' = None, *, alphas: 'Optional[np.ndarray]' = None, fg3_k: 'float' = 100.0, ft_k: 'float' = 50.0, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Luck-adjusted RAPM: ridge on an expected-points response (high-variance shooting regressed).

Replaces realized 3-point and free-throw outcomes with the shooter's shrunk
expected value (`luck_adjusted_response`); 2-pt makes stay realized.
**DECISION 2/3/4** govern the response recipe and shrinkage constants.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup frame with team-level `fg2m` and the ten lineup columns; join keys `game_id` + `possession_number` + `offense_team_id` (the last is required by `luck_adjusted_response`'s defense-shooter leak filter). Must also carry a `points` column even though the LA response (`la_points`) supersedes it for fitting -- `~sportsdataverse.nba.nba_rapm.build_rapm_design` (invoked internally via prepare`) requires it unconditionally. |
| `shooting` | `DataFrame` |  | Per-(possession, shooter) frame from `build_possession_shooting`. |
| `player_rates` | `Optional[dict[int, tuple[float, float]]]` | `None` | Optional `{player_id: (p3, pft)}` override; `None` → shrink from `shooting`. |
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override. `None` (default) auto-selects `oracle_rapm_alphas` evaluated at the possession count -- the operative WP2 oracle schedule (`cv=` `ORACLE_RAPM_CV` always; there is no plain-schedule mode). |
| `fg3_k` | `float` | `100.0` | 3-point shrinkage pseudo-count, forwarded to `luck_adjusted_response` when `player_rates` is `None`. |
| `ft_k` | `float` | `50.0` | Free-throw shrinkage pseudo-count, forwarded to `luck_adjusted_response` when `player_rates` is `None`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `LA_RAPM_SCHEMA`. Empty input → zero-row frame.

**Example**

```python
from sportsdataverse.nba.nba_rapm_variants import nba_la_rapm
df = nba_la_rapm(season_poss, season_shooting)
print(df.sort("la_rapm", descending=True).head())

# Planted-truth shooter rates (e.g. for testing)

df = nba_la_rapm(season_poss, season_shooting, {7: (0.4, 0.8)})
```

### nba_matchup_drapm {#nba_matchup_drapm}

`nba_matchup_drapm(season: 'str', *, league_id: 'str' = '00', matchups: 'Optional[pl.DataFrame]' = None, config: 'Optional[PlaytypeConfig]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Matchup-based defensive RAPM (offense-quality-controlled).

Fits `points_allowed_per_100 ~ defender_FE + offense_FE` via
`~sklearn.linear_model.RidgeCV` (weighted by matchup possessions),
reusing the shipped RAPM ridge machinery on the
`build_matchup_drapm_design` two-way-FE design.

**Sign + scale:** the design target `y` is already points-allowed *per 100*
matchup possessions (`100 * player_pts / partial_poss`), so the defender
coefficient is already on the per-100 scale -- `matchup_drapm =
-(beta_defender - mean_beta_defender)` (centered, NO extra ×100, unlike
`~sportsdataverse.nba.nba_rapm.nba_rapm` whose `y` is per-*possession*
and needs the ×100). Sign is negated so higher = better defense (fewer points
allowed), matching the `d_rapm` convention. Typical magnitudes are a few to
low-double-digit points per 100 vs the league defender average.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `matchups` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leagueseasonmatchups`-shaped frame (bypasses the live fetch -- used for tests / oracle fixtures). |
| `config` | `Optional[PlaytypeConfig]` | `None` | `~sportsdataverse.nba.nba_playtype_constants.PlaytypeConfig`; defaults to a fresh instance (`ridge_alphas` = the shared RAPM grid, `min_matchup_poss` = 25.0 inclusion floor). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per defender: `player_id` (Int64), `matchup_drapm` (Float64, points-allowed-per-100 estimate, higher = better defense), `matchup_poss` (Float64, total matchup possessions guarded). Returns a zero-row frame with this schema when the upstream fetch/injection is empty or no row survives the `min_matchup_poss` floor (sparse-coverage leagues never raise).

**Example**

```python
from sportsdataverse.nba import nba_matchup_drapm
d = nba_matchup_drapm("2023-24")
print(d.sort("matchup_drapm", descending=True).head(10))

# Injected offline (oracle / test) path

d = nba_matchup_drapm("2023-24", matchups=matchups_df)

# Pipeline next step

d.filter(pl.col("matchup_poss") >= 200).sort("matchup_drapm", descending=True)
```

### nba_predict_games {#nba_predict_games}

`nba_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Vectorized pregame predictions for a schedule of games.

Joins the ratings frame twice (home/away) and applies the closed-form
`predict_margin` / `win_prob_from_margin` / `predict_total`
math column-wise.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game with `game_id`, `home_team_id`, `away_team_id` and optionally `neutral_site` (missing column means every game is a true home game). Team-id dtypes must match `ratings['team_id']` exactly. |
| `ratings` | `DataFrame` |  | One row per team with `team_id, adj_off_rtg, adj_def_rtg, adj_net_rtg, adj_pace` (the `~sportsdataverse.nba.nba_team_ratings.nba_team_ratings` output for one season/as-of date). |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per input game: `game_id, home_team_id, away_team_id, exp_margin, home_win_prob, exp_total`. Games whose teams are missing from `ratings` carry nulls.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import nba_predict_games
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings
preds = nba_predict_games(games, nba_team_ratings(2024))
```

### nba_rookie_projection {#nba_rookie_projection}

`nba_rookie_projection(draft_year: "'int | list[int]'", *, league: 'str' = 'nba', college_prior: "'Optional[pl.DataFrame]'" = None, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Project rookie/sophomore value by composing draft x aging x availability.

Composition (no re-derived features -- each term is the verbatim public
output of ①②③):

- `base = nba_draft_model(...).proj_career_value * rookie_fraction`
  (`rookie_fraction` from the bundled residual artifact -- the share of
  career value realized in a single rookie season).
- `proj_rookie_value = base * rel_value(rookie_age) / rel_value(peak_age)
  + residual[pro_tier]`; `proj_soph_value` uses `rookie_age + 1`.
- `proj_avail_pct` from `sportsdataverse.nba.nba_availability.nba_availability`
  at rookie age -- reported separately, **never** multiplied into the
  value columns (availability is availability, not skill).
- `proj_rookie_min = games_full_season * proj_avail_pct * expected_mpg(pro_tier)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `draft_year` | `int \| list[int]` |  | A draft year or list of years. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `college_prior` | `Optional[DataFrame]` | `None` | Optional college-side prior frame, forwarded verbatim to `sportsdataverse.nba.nba_draft_model.nba_draft_model`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, draft_year:Int64, proj_rookie_value:Float64, proj_soph_value:Float64, proj_rookie_min:Float64, proj_avail_pct:Float64, pro_tier:Utf8`. Empty input -> zero-row schema.

**Example**

```python
from sportsdataverse.nba import nba_rookie_projection
board = nba_rookie_projection(2019)
print(board.sort("proj_rookie_value", descending=True).head())
```

### nba_spm {#nba_spm}

`nba_spm(box_features: 'pl.DataFrame', coefficients: 'SpmCoefficients', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Apply fitted SPM coefficients to per-100 box features -> OSPM/DSPM/SPM.

Applies a linear scoring rule:

.. code-block:: text

    ospm = X @ o_coef + o_intercept
    dspm = X @ d_coef + d_intercept
    spm  = ospm + dspm

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_features` | `DataFrame` |  | Per-player per-100 features. Must contain `player_id`, every column in `coefficients.feature_names`, `min`, and `gp`. |
| `coefficients` | `SpmCoefficients` |  | A `SpmCoefficients` instance from `train_spm`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

Per-player frame with columns `player_id` (Int64), `ospm` (Float64), `dspm` (Float64), `spm` (Float64), `min` (Float64), `gp` (Int64).

**Example**

```python
from sportsdataverse.nba import nba_spm
ratings = nba_spm(box_feats, coef)
print(ratings.sort("spm", descending=True).head())

# Pipeline next step

ratings.filter(pl.col("min") >= 500).sort("spm", descending=True)
```

### nba_team_ratings {#nba_team_ratings}

`nba_team_ratings(seasons: 'Union[int, list[int]]', *, league_id: 'str' = '00', as_of_date: 'Union[dt.date, None]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Opponent-adjusted team ratings (AdjOffRtg/AdjDefRtg/AdjNet/AdjPace), as-of-date aware.

Loads schedule + team box score for `seasons`, optionally filters to
games strictly before `as_of_date` (the leakage boundary, via
`~sportsdataverse.nba.nba_prediction_constants.as_of_ratings_split`),
computes per-game efficiency, runs the opponent-adjustment fixed points,
and adds a per-season dense `rank` (on `adj_net_rtg` descending) and
`adj_net_z` (z-score of `adj_net_rtg`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2024`) or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `as_of_date` | `Union[date, None]` | `None` | If given, only games with `date < as_of_date` are used (predictive/backtest usage); `None` computes full-season descriptive ratings. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per (season, team_id): `season, team_id, adj_off_rtg, adj_def_rtg, adj_net_rtg, adj_pace, raw_off_rtg, raw_def_rtg, raw_pace, games, rank, adj_net_z`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings
ratings = nba_team_ratings(2024)
ratings.sort("rank").head()

# As-of-date (leakage-safe) ratings for a backtest

import datetime as dt
ratings = nba_team_ratings(2024, as_of_date=dt.date(2024, 1, 15))

# WNBA / G-League via ``league_id``

wnba_ratings = nba_team_ratings(2024, league_id="10")
```

### nba_war {#nba_war}

`nba_war(ratings: 'pl.DataFrame', poss: 'pl.DataFrame', *, replacement_level: 'float', pts_per_win: 'float', rating_col: 'str' = 'rating', poss_col: 'str' = 'poss', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Points-above-replacement -> wins for each player.

`war_i = (rating_i - replacement_level) * poss_i / 100 / pts_per_win`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | Per-player rating frame with `player_id` and `rating_col` (e.g. `nba_rapm`'s `rapm` column renamed, a `nba_ratings_panel` row filtered to one date, or `nba_bpm`'s `bpm` column). |
| `poss` | `DataFrame` |  | Per-player possession-count frame with `player_id` and `poss_col` (e.g. `off_poss + def_poss` from `nba_rapm`). |
| `replacement_level` | `float` |  | Per-100-possession rating of a replacement-level player. No built-in default — calibrate via `calibrate_replacement_level`. |
| `pts_per_win` | `float` |  | Points of season point-margin per marginal win. No built-in default — calibrate via `calibrate_pts_per_win`. |
| `rating_col` | `str` | `'rating'` | Column in `ratings` to score. |
| `poss_col` | `str` | `'poss'` | Column in `poss` giving total possessions played. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

Frame with `WAR_SCHEMA` columns (`player_id`, `war`). Empty (that schema) when either input is empty.

**Example**

```python
from sportsdataverse.nba.nba_war import nba_war
war = nba_war(rapm_df.rename({"rapm": "rating"}), poss_df,
               replacement_level=-2.0, pts_per_win=250.0)
print(war.sort("war", descending=True).head())

# Derive both required kwargs from real data first

from sportsdataverse.nba.nba_war import (
    calibrate_pts_per_win, calibrate_replacement_level, nba_war,
)
pts_per_win = calibrate_pts_per_win(team_standings)
repl = calibrate_replacement_level(
    ratings, poss, pts_per_win=pts_per_win, target_total_war=300.0,
)
war = nba_war(ratings, poss, replacement_level=repl, pts_per_win=pts_per_win)
```

### predict_margin {#predict_margin}

`predict_margin(home_net: 'float', away_net: 'float', *, home_pace: 'float', away_pace: 'float', neutral: 'bool' = False, league_id: 'str' = '00') -> 'float'`

Expected home-minus-away margin from two adjusted net ratings.

The AdjNet difference (points/100 possessions) is scaled by the
matchup's `expected_possessions` before the home-court advantage
is added.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_net` | `float` |  | Home team's adjusted net rating (`adj_net_rtg`). |
| `away_net` | `float` |  | Away team's adjusted net rating. |
| `home_pace` | `float` |  | Home team's adjusted pace. |
| `away_pace` | `float` |  | Away team's adjusted pace. |
| `neutral` | `bool` | `False` | True for a neutral-site game (no home-court advantage). |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` -- selects the fitted HFA. |

**Returns**

Expected margin in points (positive favors the home team).

**Example**

```python
from sportsdataverse.nba.nba_game_predict import predict_margin
predict_margin(10.0, -2.0, home_pace=100.0, away_pace=98.0, neutral=False)
```
