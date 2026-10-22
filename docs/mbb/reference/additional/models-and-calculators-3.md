# MBB — additional Python functions — Models and calculators: roc_auc–win_prob

> MBB — additional Python functions — Models and calculators: roc_auc–win_prob — function reference in sdv-py, the SportsDataverse Python package.

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

Parse a `"scored-allowed"` score string (`ExtractorUtils.score_to_tuple`,

`ExtractorUtils.scala:107-113`).

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
| `player_id` | character | Unique player identifier. |
| `from_team_id` | character | Unique identifier for from team. |
| `to_team_id` | character | Unique identifier for to team. |
| `from_season` | integer |  |
| `to_season` | integer |  |

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
