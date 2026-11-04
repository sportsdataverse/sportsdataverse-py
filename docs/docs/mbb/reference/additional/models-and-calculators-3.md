---
title: "Men's college basketball — additional Python functions — Models and calculators: predict_margin–win_prob"
sidebar_label: "Models and calculators: predict_margin–win_prob"
sidebar_position: 12
description: "Men's college basketball — additional Python functions — Models and calculators: predict_margin–win_prob — function reference in sdv-py, the SportsDataverse Python package."
---
# Men's college basketball — additional Python functions — Models and calculators: predict_margin–win_prob

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
