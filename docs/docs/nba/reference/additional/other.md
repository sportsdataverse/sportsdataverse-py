---
title: "NBA — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 9
description: "NBA — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Other

### load_darko_dpm {#load_darko_dpm}

`load_darko_dpm(path: 'str') -> 'pl.DataFrame'`

Parse a DARKO DPM leaderboard CSV (e.g. `2026-darko-dpm-leaderboard.csv`).

Name-keyed only (no shared player id with the model zoo) -- this is the
family `~sportsdataverse.nba.nba_model_validation.external_validity`
joins with `join="name"`. Handles two real-file quirks: a leading UTF-8
BOM (read with `encoding="utf-8-sig"`, which strips it) and
sign-prefixed integer columns (`"+7"`, not `"7"`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a DARKO DPM leaderboard CSV. |

**Returns**

Frame with schema `DARKO_DPM_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_darko_dpm
oracle = load_darko_dpm(f"{oracle_dir}/2026-darko-dpm-leaderboard.csv")
print(oracle.sort("dpm", descending=True).head())
```

### load_dunks_threes_stats {#load_dunks_threes_stats}

`load_dunks_threes_stats(path: 'str') -> 'pl.DataFrame'`

Parse a Dunks & Threes counting-stats CSV (e.g. `2025_Dunks_&_Threes_Stats.csv`).

Only `ewins` (estimated wins) is kept -- the WAR-layer oracle target
the spec pairs with LEBRON's `WAR` column. WP4's `nba_war` doesn't
exist yet, so this loader is built and tested standalone (see the
plan's "WP4/WP2 dependency notes").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a D&T counting-stats CSV. |

**Returns**

Frame with schema `DT_STATS_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_dunks_threes_stats
oracle = load_dunks_threes_stats(f"{oracle_dir}/2025_Dunks_&_Threes_Stats.csv")
```

### load_epm {#load_epm}

`load_epm(path: 'str') -> 'pl.DataFrame'`

Parse a Dunks & Threes EPM CSV (`{season}_EPM_data.csv`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a D&T EPM CSV. |

**Returns**

Frame with schema `EPM_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_epm
oracle = load_epm(f"{oracle_dir}/2025_EPM_data.csv")
```

### load_lebron_daily {#load_lebron_daily}

`load_lebron_daily(path: 'str') -> 'pl.DataFrame'`

Parse a LEBRON daily-snapshot CSV (e.g. `lebron_daily_2026-07-02.csv`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a LEBRON daily-snapshot CSV. |

**Returns**

Frame with schema `LEBRON_DAILY_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
import glob
from sportsdataverse.nba.nba_oracle_data import load_lebron_daily
latest = sorted(glob.glob(f"{oracle_dir}/lebron_daily_*.csv"))[-1]
oracle = load_lebron_daily(latest)
```

### load_lebron_season {#load_lebron_season}

`load_lebron_season(path: 'str') -> 'pl.DataFrame'`

Parse a LEBRON season-file CSV (e.g. `lebron-data-2026.csv`).

`seasons` is passed through as a raw string -- per-season files carry a
single year (`"2026"`); the combined all-years file carries a
multi-year window (`"2010-2013"`). Both parse with this one function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a LEBRON season CSV. |

**Returns**

Frame with schema `LEBRON_SEASON_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_lebron_season
oracle = load_lebron_season(f"{oracle_dir}/lebron-data-2026.csv")
```

### load_nba_stats_leaguedash {#load_nba_stats_leaguedash}

`load_nba_stats_leaguedash(family: 'str', seasons: 'int | Iterable[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load one asset family of the `nba_stats_leaguedash` release.

`nba_stats_leaguedash` is a parameter cube: one asset per
(family, season) pair rather than one per season, so a family must be named.
The valid families are exported as
`NBA_STATS_LEAGUEDASH_FAMILIES` -- import that tuple to discover them
rather than passing a bare string; an unknown family raises `ValueError`
listing every valid value.

Column sets are family-specific (a `lineups_*` frame keys on `group_id`,
a `player_*` frame on `player_id`), so this loader documents no fixed
returns table. `player_id` / `team_id` are `Int64` in every family and
season, so cross-family joins need no dtype reconciliation.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `family` | `str` |  | Asset family, e.g. `"player_stats_advanced"`. Must be one of `NBA_STATS_LEAGUEDASH_FAMILIES`. |
| `seasons` | `int \| Iterable[int]` |  | Season, or iterable of seasons, to load. Seasons are END years (`2024` = the 2023-24 NBA season). 1996 is the earliest season on the tag; per-family coverage starts later (`lineups_*` 2008, most `player_tracking_*` 2014). A requested season the family does not publish is warned about and skipped, not an error. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe with one row per player / team / lineup per requested season for the requested family; an empty frame when no requested season is published.

**Example**

```python
from sportsdataverse.nba import load_nba_stats_leaguedash
adv = load_nba_stats_leaguedash("player_stats_advanced", seasons=2024)
print(adv.shape)

# Discover the valid families

from sportsdataverse.nba import NBA_STATS_LEAGUEDASH_FAMILIES
print([f for f in NBA_STATS_LEAGUEDASH_FAMILIES if f.startswith("player_tracking_")])

# Multi-season, pandas round-trip

drives_pd = load_nba_stats_leaguedash(
    "player_tracking_drives", seasons=range(2020, 2025), return_as_pandas=True
)

# Pipeline next step (top usage rates in 2024)

import polars as pl
usage = load_nba_stats_leaguedash("player_stats_usage", seasons=2024)
usage.sort("usg_pct", descending=True).head()
```

### load_rapm_ryan_davis {#load_rapm_ryan_davis}

`load_rapm_ryan_davis(path: 'str') -> 'pl.DataFrame'`

Parse a Ryan Davis published RAPM CSV (single-season or multi-year window).

Serves BOTH real files -- `rapm_ryan_davis.csv` (`season` like
`"2009-10"`) and `rapm_multi_ryan_davis.csv` (`season` like
`"2011-16"`, a multi-year decay window) -- since they share an
identical header. Only the combined (not per-side Off`/Def`)
rating columns are kept, matching the model zoo's combined-rating
convention (`nba_rapm`'s `rapm` column, not separate offense/defense).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a Ryan Davis RAPM CSV. |

**Returns**

Frame with schema `RAPM_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_oracle_data import load_rapm_ryan_davis
oracle = load_rapm_ryan_davis(f"{oracle_dir}/rapm_ryan_davis.csv")
season = oracle.filter(pl.col("season") == "2022-23")
```

### year_to_season {#year_to_season}

`year_to_season(year)`

Convert a season START year (e.g. 2023) to the NBA's hyphenated label

(e.g. `"2023-24"`).

Callers working in the end-year convention pass `end_year - 1` (e.g.
`year_to_season(most_recent_nba_season() - 1)`).

Handles century rollover (1999 -> `"1999-00"`) and zero-pads the
second half of the label.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `int` |  | The starting calendar year of the season (e.g. 2023 for the 2023-24 season). |

**Returns**

NBA-style season label.

**Example**

```python
from sportsdataverse.nba import year_to_season
label = year_to_season(2023)
print(label)  # "2023-24"

# Century rollover

print(year_to_season(1999))  # "1999-00"
```

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

### add_ctg_shot_zones {#add_ctg_shot_zones}

`add_ctg_shot_zones(enhanced_pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Append CTG's shot-location zone (`ctg_shot_zone`) to an enhanced PBP frame.

CTG's zones differ from the official NBA zones emitted by
`~sportsdataverse.nba.nba_shot_zones.add_shot_zones`: CTG splits the
midrange at the free-throw-line distance rather than at the paint boundary.

* `at_rim` — shot distance < 4 ft ("Shots within 4 feet of the basket").
* `short_mid` — 4 ft <= distance < 14 ft ("outside of 4 feet, but inside of
  ~14 feet (the free throw line distance)").
* `long_mid` — >= 14 ft, inside the arc.
* `corner_3` — a three "below the break" (`|x_legacy| >= 220` and
  `y_legacy <= 87.5`).
* `arc_3` — any other three (CTG's "non-corner three").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Frame from `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`. |

**Returns**

The input frame with a `ctg_shot_zone` Utf8 column appended (null on non-field-goal rows). Empty input returns a zero-row frame carrying the column — never raises.

**Example**

```python
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_play_context import add_ctg_shot_zones
pbp = add_ctg_shot_zones(enhanced_pbp_from_payload(payload))
print(pbp.filter(pl.col("ctg_shot_zone").is_not_null())["ctg_shot_zone"].value_counts())
```

### add_play_context {#add_play_context}

`add_play_context(enhanced_pbp: 'pl.DataFrame', *, transition_seconds: 'float' = 6.0, transition_variant: 'str' = 'hoop_math', starters_on_court: 'Optional[dict[int, int]]' = None) -> 'pl.DataFrame'`

Build possessions and enrich them with the full CTG play-context surface.

One call: `~sportsdataverse.nba.nba_possessions.build_possessions` ->
`add_start_type_detail` -> `add_transition` ->
`flag_heave_possessions` -> `flag_garbage_time`.

The CTG filter columns are **flags, not filters** — nothing is dropped. Apply

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Frame from `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`. |
| `transition_seconds` | `float` | `6.0` | Transition initial-play cutoff (default 10.0). |
| `transition_variant` | `str` | `'hoop_math'` | See `add_transition`. |
| `starters_on_court` | `Optional[dict[int, int]]` | `None` | Optional starters-on-floor counts; see `flag_garbage_time`. |

**Returns**

The possession frame (`POSSESSIONS_SCHEMA`) plus every column in `PLAY_CONTEXT_POSSESSIONS_SCHEMA`.

**Example**

```python
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_play_context import add_play_context
poss = add_play_context(enhanced_pbp_from_payload(payload))
print(poss["possession_start_type_ctg"].value_counts())
```

### add_start_type_detail {#add_start_type_detail}

`add_start_type_detail(possessions: 'pl.DataFrame', enhanced_pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Append the full pbpstats start-type taxonomy to a possession frame.

Upgrades the engine's coarse 5-value `possession_start_type` into:

* `possession_start_type_detail` — the zone-split pbpstats vocabulary:
  `Off{AtRim|ShortMidRange|LongMidRange|Corner3|Arc3}{Make|Miss|Block}`,
  `OffFTMake` / `OffFTMiss`, `OffLiveBallTurnover`, `OffTimeout`,
  `OffDeadball`.
* `possession_start_type_ctg` — the coarse bucket CTG reports on:
  `off_made` / `off_live_rebound` / `off_steal` / `off_deadball` /
  `off_timeout` (see `~nba_play_context_constants.CTG_START_BUCKETS`).

Precedence (pbpstats): period start > timeout > previous boundary event. A
**team rebound** (`person_id == 0`) is a dead-ball start even though a
rebound row exists; a **timeout** beats a made basket (an after-timeout
possession is `OffTimeout`, not `OffMadeShot`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `~sportsdataverse.nba.nba_possessions.build_possessions`. |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame those possessions were built from. |

**Returns**

`possessions` with the two columns appended. Empty input returns a zero-row frame carrying them — never raises.

**Example**

```python
from sportsdataverse.nba.nba_possessions import build_possessions
from sportsdataverse.nba.nba_play_context import add_start_type_detail
poss = add_start_type_detail(build_possessions(pbp), pbp)
print(poss["possession_start_type_ctg"].value_counts())
```

### add_transition {#add_transition}

`add_transition(possessions: 'pl.DataFrame', enhanced_pbp: 'pl.DataFrame', *, transition_seconds: 'float' = 6.0, variant: 'str' = 'hoop_math') -> 'pl.DataFrame'`

Flag possessions that started in transition, and time their initial play.

CTG defines transition as beginning at the possession start and ending "once
the defense is set", **without publishing a seconds threshold**. We therefore
time the possession's *initial play* — its first shot attempt, trip to the
line, or turnover (CTG's own definition of a "play") — and call the
possession transition when that play lands within `transition_seconds`.

Variants (`~nba_play_context_constants.TRANSITION_VARIANTS`):

* `hoop_math` (default) — any non-timeout start type qualifies.
* `haslametrics` — steal starts only (conservative).
* `bigballr` — the previous possession must have ended live
  (`off_made` / `off_live_rebound` / `off_steal`); a dead-ball start can
  never be transition.

The first possession of a period is never transition. After a timeout the
defense is set by construction, so `off_timeout` never qualifies under any
variant.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame carrying `possession_start_type_ctg` (i.e. the output of `add_start_type_detail`). |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame the possessions were built from. |
| `transition_seconds` | `float` | `6.0` | Initial-play cutoff. Default 10.0 (hoop-math). Calibrate against Synergy transition frequency (league mean ~15-16%). |
| `variant` | `str` | `'hoop_math'` | One of `~nba_play_context_constants.TRANSITION_VARIANTS`. |

**Returns**

`possessions` with `seconds_to_first_play` (Float64, null when the possession had no play), `is_transition` (Boolean), `transition_source` (Utf8: `steal` / `live_rebound` / `made` / `deadball`; null when not transition) and `possession_context` (Utf8: `transition` / `halfcourt` / `misc`) appended.

**Example**

```python
poss = add_transition(add_start_type_detail(poss, pbp), pbp)
print(poss["is_transition"].mean())          # transition frequency

# Tune the knob against the Synergy oracle

poss8 = add_transition(poss, pbp, transition_seconds=8.0)
```

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

### box_features {#box_features}

`box_features(player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame', *, game_ids: 'Optional[List[str]]' = None) -> 'pl.DataFrame'`

Aggregate per-player per-100-possession box features over a set of games.

Restricting `game_ids` to a fold's games is the harness leakage guard.

Per-100 possessions are computed per game (so mid-window trades use each
game's own team pace), then summed — the result is fully deterministic.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | Per-player-per-game box lines (`game_id`, `team_id`, `player_id`, `min`, and the counting stats in STATS`). |
| `team_logs` | `DataFrame` |  | Per-team-per-game lines (`game_id`, `team_id`, `min`, `fga`, `oreb`, `tov`, `fta`) for the possession estimate. |
| `game_ids` | `Optional[List[str]]` | `None` | Optional subset of `game_id` to include (default: all). |

**Returns**

One row per player: `player_id`, the STATS` per-100 rates, `min` (total), `gp` (games). Empty frame with that schema on empty input.

### build_athlete_identity_lookup {#build_athlete_identity_lookup}

`build_athlete_identity_lookup(rosters: 'dict[int | str, dict]') -> 'dict[str, dict[str, Any]]'`

R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rosters` | `dict[int \| str, dict]` |  | Mapping of team_id -> that team's raw roster payload (`wbb/team_rosters/json/{season}/{team_id}.json`). NOTE: R walks `raw$athletes` directly here (no position-bucket unwrap, unlike the rosters dataset itself). |

**Returns**

athlete_id (str) -> identity fields for `helper_wbb_player_season_stats`.

### build_nba_player_identity_lookup {#build_nba_player_identity_lookup}

`build_nba_player_identity_lookup(player_box: 'pl.DataFrame') -> 'dict[str, dict[str, Any]]'`

R `build_identity_lookup(season)`: athlete_id -> identity from the

season's already-compiled `player_box` -- the authoritative "who played
in season Y" source (ESPN's team-roster endpoint is current-only and
cannot answer that for historical seasons).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_box` | `DataFrame` |  | The season's compiled player_box frame (e.g. `nba/player_box/parquet/player_box_{season}.parquet`, or whatever the season builder just wrote for this pass). Must carry `athlete_id`; other identity columns are best-effort. |

**Returns**

athlete_id (str) -> identity fields for `helper_nba_player_season_stats`. When an athlete appears in multiple rows (multiple games), the LAST row (by frame order) wins -- mirroring R's `!duplicated(athlete_id, fromLast = TRUE)`, which keeps an athlete's most recent team within the season.

### build_play_context_shots {#build_play_context_shots}

`build_play_context_shots(possessions: 'pl.DataFrame', enhanced_pbp: 'pl.DataFrame', *, putback_seconds: 'float' = 2.0) -> 'pl.DataFrame'`

Build the per-shot frame carrying CTG's play context.

CTG assigns context **per play**, not per possession: one possession can
contain a transition miss, a halfcourt reset and a putback. This frame is the
play-level view — one row per field-goal attempt.

* `is_putback` — pbpstats `field_goal.py:112-144`: an **unassisted 2-point**
  attempt whose preceding event is a **real offensive rebound by the same
  player**, within `putback_seconds`. A three is never a putback.
* `is_second_chance_shot` — the shot follows an offensive rebound earlier in
  the same possession.
* `shot_context` — `transition` / `putback` / `halfcourt`. **Transition
  wins over putback**, reproducing CTG exactly: "if a team comes down in
  transition and misses a shot but gets a putback, that putback is classified
  as part of the overall transition event."

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_transition` (needs `is_transition`). |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame the possessions were built from. |
| `putback_seconds` | `float` | `2.0` | Rebound-to-shot window. Default 2.0 (pbpstats). |

**Returns**

Polars DataFrame with schema `PLAY_CONTEXT_SHOTS_SCHEMA` — one row per field-goal attempt. Empty input returns the zero-row schema.

**Example**

```python
shots = build_play_context_shots(poss, pbp)
print(shots.group_by("shot_context").len())
print(shots.filter(pl.col("is_putback") == True).height)
```

### build_possession_shooting {#build_possession_shooting}

`build_possession_shooting(enhanced_pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Build the per-shooter companion frame from an enhanced play-by-play DataFrame.

Companion to `build_possessions`: instead of one team-level row per
possession, emits one row per distinct shooter (`player_id`) per
possession, with their own `fg2a/fg2m/fg3a/fg3m/fta/ftm` counts. Shares
the same possession-group traversal as `build_possessions` via
assemble` — the two frames are always built from a single
consistent pass over the play-by-play. Consumed by WP2's luck-adjusted
shooting response.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Polars DataFrame with schema `ENHANCED_PBP_SCHEMA` (from `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`). An empty or malformed frame returns a zero-row frame with `POSSESSION_SHOOTING_SCHEMA` — never raises. |

**Returns**

Polars DataFrame with schema `POSSESSION_SHOOTING_SCHEMA`. One row per `(possession_number, player_id)` pair. Events with `person_id == 0` are skipped (unattributable to a shooter — they still count toward `build_possessions`' team-level totals). Per-possession sums of the six shooting columns match the corresponding `build_possessions` columns exactly.

**Example**

```python
import json, pathlib
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_possessions import build_possession_shooting

payload = json.loads(pathlib.Path("playbyplayv3.json").read_text())
pbp = enhanced_pbp_from_payload(payload)
sh = build_possession_shooting(pbp)
print(sh.shape, sh.schema["player_id"])

# Per-player shooting totals

import polars as pl
totals = sh.group_by("player_id").agg(
    pl.col("fg3m").sum(), pl.col("ftm").sum()
)
print(totals.head())
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

### clutch_delta {#clutch_delta}

`clutch_delta(clutch: 'pl.DataFrame', ratings: 'pl.DataFrame') -> 'pl.DataFrame'`

Clutch net-rating delta vs a full-game baseline, per (season, team_id).

`clutch_delta = clutch_net_rating - adj_net_rtg`. Joins `clutch` to the
baseline `ratings` frame on `(season, team_id)` (asserting dtype
agreement first).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clutch` | `DataFrame` |  | Frame with `season, team_id, clutch_net_rating, clutch_poss`. |
| `ratings` | `DataFrame` |  | Full-game baseline with `season, team_id, adj_net_rtg` (the stats full-game net, or any per-team baseline). |

**Returns**

One row per matched (season, team_id): `season, team_id, clutch_net_rating, adj_net_rtg, clutch_delta, clutch_poss`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_clutch import clutch_delta
d = clutch_delta(clutch_frame, baseline_frame)
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

### flag_garbage_time {#flag_garbage_time}

`flag_garbage_time(possessions: 'pl.DataFrame', enhanced_pbp: 'pl.DataFrame', *, starters_on_court: 'Optional[dict[int, int]]' = None) -> 'pl.DataFrame'`

Flag CTG garbage time (excluded from CTG stats by default).

CTG (exact): "the game has to be in the **4th quarter**, the score
differential has to be **>= 25 for minutes 12-9, >= 20 for minutes 9-6, and
>= 10 for the remainder of the quarter**. Additionally, there have to be **two
or fewer starters on the floor combined between the two teams**. Importantly,
the game can never go back to being non-garbage time, or this clock resets."

The margin x minutes bands are reproduced exactly, evaluated on the score at
each possession's start. The reset semantics fall out of that per-possession
evaluation: if the trailing team claws back inside the band's threshold, the
condition stops holding and those possessions are NOT garbage time (CTG's own
"comeback is not counted as garbage time" example); if the lead re-expands,
the flag turns back on.

**The starters clause is applied only when `starters_on_court` is supplied**
— it needs lineup + box `START_POSITION` data this frame does not carry.
Without it the flag is the **margin-only superset** of CTG's definition (it can
flag a blowout stretch in which the starters are still on the floor), and
`garbage_time_basis` records which rule was actually used. This is a
deliberate, documented divergence — do not read a `margin_only` flag as
CTG-exact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame with `period`, `start_seconds_remaining` and `start_order_index`. |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame (supplies the running score). |
| `starters_on_court` | `Optional[dict[int, int]]` | `None` | Optional map `possession_number -> number of starters on the floor across BOTH teams`. When given, a possession is garbage time only if that count is <= `~nba_play_context_constants.GARBAGE_TIME_MAX_STARTERS`. |

**Returns**

`possessions` with Boolean `is_garbage_time` and Utf8 `garbage_time_basis` (`"margin_and_starters"` or `"margin_only"`) appended.

**Example**

```python
poss = flag_garbage_time(poss, pbp)
print(poss.filter(pl.col("is_garbage_time") == True).height)

# CTG-exact, with the starters clause

poss = flag_garbage_time(poss, pbp, starters_on_court=starters_by_possession)
```

### flag_heave_possessions {#flag_heave_possessions}

`flag_heave_possessions(possessions: 'pl.DataFrame') -> 'pl.DataFrame'`

Flag CTG's "projected heave possessions" (excluded from CTG stats by default).

CTG (exact): "possessions that start with **4 or fewer seconds on the game
clock at the end of one of the first three quarters**." Q4/OT are exempt — a
late Q4 possession is a real possession.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Any frame with `period` and `start_seconds_remaining`. |

**Returns**

`possessions` with a Boolean `is_heave_possession` column appended.

**Example**

```python
poss = flag_heave_possessions(poss)
clean = poss.filter(pl.col("is_heave_possession") == False)
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

### hoopshype_salaries {#hoopshype_salaries}

`hoopshype_salaries(*, proxy: 'Any' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

League-wide NBA player salaries from HoopsHype.

One row per player per contract season (current plus the future seasons
HoopsHype lists), for the whole league (~600 players).

HoopsHype is a Next.js app: the single `/salaries/players/` page paginates
client-side and only ~20 rows survive a static fetch, but each team page
embeds that team's complete roster in `<script id="__NEXT_DATA__">`. This
walks the 30 slugs in `HOOPSHYPE_TEAMS` **serially** -- ~30 requests per
call -- and parses that JSON. A team page that fails is warned about and
skipped rather than aborting the league.

Pacing is environment-tunable, never hardcoded in the fetch path:

============================ ==================================================
`SDV_PY_HOOPSHYPE_DELAY`   seconds slept between team pages (default `0.5`)
============================ ==================================================

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `proxy` | `Any` | `None` | Proxy configuration forwarded to `~sportsdataverse.dl_utils.download` (`requests` `proxies=` shape). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player-season with `player_id`, `player`, `first_name`, `last_name`, `team_id`, `team`, `season`, `salary`, `cap_allocation`, `team_option`, `player_option`, `two_way` and `qualifying_offer`. Ids are `Utf8`, money is `Float64`, options are `Boolean`. All 30 pages failing yields a zero-row frame with that schema.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `player` | character | Player name. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `team_id` | character | Unique team identifier. |
| `team` | character | Team-side label or team identifier. |
| `season` | integer | Season year. |
| `salary` | double | Total cap-counting salary for the season ($). |
| `cap_allocation` | double | Cap allocation for the season (USD). |
| `team_option` | logical | Whether the season is a team option. |
| `player_option` | logical | Whether the season is a player option. |
| `two_way` | logical | Whether it is a two-way contract. |
| `qualifying_offer` | logical | Whether it is a qualifying offer. |

**Example**

```python
from sportsdataverse.nba import hoopshype_salaries

salaries = hoopshype_salaries()
print(salaries.shape)

# As pandas

salaries_pd = hoopshype_salaries(return_as_pandas=True)

# Pipeline next step (this season's top-paid)

salaries.filter(pl.col("season") == 2026).sort("salary", descending=True).head()
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

### lineup_play_context {#lineup_play_context}

`lineup_play_context(possessions: 'pl.DataFrame', *, min_poss: 'int' = 0, league_non_transition_ppp: 'Optional[float]' = None, apply_ctg_filters: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Roll possessions up into a per-5-man-lineup Play-Context table.

The lineup analogue of `team_play_context`: same metric columns, grouped
by the five players on the floor **for the offense**. Lineups are identified by
`lineup_id` — the five player ids sorted ascending and hyphen-joined — so the
same five players always land in the same bucket regardless of slot order.

Requires the `off_player_1..5` columns from
`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`
(which passes the play-context columns through, so the two compose in either
order).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_play_context` **with lineups attached**. |
| `min_poss` | `int` | `0` | Drop lineups below this possession count (CTG's tables carry a minimum; 0 keeps everything, which is what the partition identity needs). |
| `league_non_transition_ppp` | `Optional[float]` | `None` | Pts+/Poss baseline; see `team_play_context`. |
| `apply_ctg_filters` | `bool` | `True` | Drop garbage-time / heave / non-counting possessions first. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (team, lineup) with `LINEUP_PLAY_CONTEXT_SCHEMA`. Empty input returns a zero-row frame with that schema.

**Example**

```python
poss = attach_possession_lineups(add_play_context(enh), oncourt, enh, home_team_id=home)
lu = lineup_play_context(poss, min_poss=25)
print(lu.sort("pts_per_100", descending=True).head())
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

### make_prob_by_context {#make_prob_by_context}

`make_prob_by_context(ptshots: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'dict[str, Union[pl.DataFrame, pd.DataFrame]]'"`

Marginal FG% tables by defender distance and by shot clock.

The public API exposes defender-distance and shot-clock only as aggregate
bucket tables (`playerdashptshots`), not per-shot fields, so this
aggregates `Σfgm/Σfga` across players within each bucket.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ptshots` | `DataFrame` |  | The stacked `playerdashptshots` fixture — one frame with a `result_set` tag (`ClosestDefenderShooting` / `ShotClockShooting`) plus `bucket, fga, fgm`. |
| `return_as_pandas` | `bool` | `False` | Return pandas DataFrames instead of polars. |

**Returns**

`{"defender": frame, "shot_clock": frame}` each with rows per `bucket` (`bucket, fga, fgm, fg_pct`). Missing result sets return the zero-row schema.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import make_prob_by_context
tables = make_prob_by_context(ptshots)
tables["defender"].sort("fg_pct")
```

### make_prob_joint {#make_prob_joint}

`make_prob_joint(defender: 'pl.DataFrame', shot_clock: 'pl.DataFrame', overall_fg_pct: 'float', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Independence-combined defender x shot-clock make probability.

Combines the two marginal FG% tables under a conditional-independence
assumption via odds multipliers: `odds(p) = p/(1-p)`;
`odds_joint = odds_overall * (odds_def/odds_overall) *
(odds_clock/odds_overall)`; `joint = odds_joint/(1+odds_joint)`. This
assumes defender distance and shot-clock effects are independent given the
league baseline — a simplification (a late clock correlates with tighter
defense), documented here so callers weigh it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `defender` | `DataFrame` |  | The `"defender"` marginal table from `make_prob_by_context` (`bucket, fg_pct`). |
| `shot_clock` | `DataFrame` |  | The `"shot_clock"` marginal table (`bucket, fg_pct`). |
| `overall_fg_pct` | `float` |  | The league overall FG% baseline. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `(close_def_dist_range, shot_clock_range)`: `close_def_dist_range:Utf8, shot_clock_range:Utf8, joint_fg_pct:Float64`. Empty inputs return the zero-row schema.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import make_prob_by_context, make_prob_joint
t = make_prob_by_context(ptshots)
joint = make_prob_joint(t["defender"], t["shot_clock"], 0.47)
```
