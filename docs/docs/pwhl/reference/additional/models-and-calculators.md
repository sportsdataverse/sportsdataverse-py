---
title: "PWHL — additional Python functions — Models and calculators"
sidebar_label: "Models and calculators"
sidebar_position: 3
description: "PWHL — additional Python functions — Models and calculators — function reference in sdv-py, the SportsDataverse Python package."
---
# PWHL — additional Python functions — Models and calculators

### LeagueConstants {#LeagueConstants}

`LeagueConstants(hfa: 'float', margin_sd: 'float', avg_xgf: 'float', avg_total_goals: 'float', total_scale: 'float', shrink_k: 'float', prop_kappa: 'dict', pos_priors: 'dict', prop_team_volume_slope: 'float', in_game_wp_artifact: 'str', min_season: 'int') -> None`

Fitted, league-specific constants for the NHL/PWHL prediction spine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `hfa` | `float` |  | home-ice edge, expected-goals units. |
| `margin_sd` | `float` |  | standard deviation of the final goal margin (deliberately WIDE for hockey). |
| `avg_xgf` | `float` |  | league mean even-strength xG-for, per game. |
| `avg_total_goals` | `float` |  | league mean total goals per game. |
| `total_scale` | `float` |  | multiplier converting rating differential to total-goals deviation. |
| `shrink_k` | `float` |  | games-played prior strength for rating shrinkage. |
| `prop_kappa` | `dict` |  | empirical-Bayes shrinkage strength per player-prop stat family. |
| `pos_priors` | `dict` |  | per-position (F/D) per-stat-family prior rates. |
| `prop_team_volume_slope` | `float` |  | game-script tilt on a player-prop projection (favored team -> fewer late shots-for). SEEDED PLACEHOLDER (~0.04), not yet fitted -- a future prop-fit task should estimate it from the realized shots-vs-exp_margin slope, mirroring how fit_props.py fits prop_kappa/pos_priors. |
| `in_game_wp_artifact` | `str` |  | filename of the bundled in-game win-probability model under `sportsdataverse/nhl/models/`. |
| `min_season` | `int` |  | earliest season this league's prediction spine supports. |

### as_of_ratings_split {#as_of_ratings_split}

`as_of_ratings_split(df: 'pl.DataFrame', cutoff_date: '_dt.date', *, date_col: 'str' = 'date') -> 'pl.DataFrame'`

Filter a frame to rows strictly before `cutoff_date` (the leakage boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | a polars DataFrame with a date column. |
| `cutoff_date` | `date` |  | the game date being predicted; only strictly-earlier rows are kept. |
| `date_col` | `str` | `'date'` | name of the date column (default `"date"`). |

**Returns**

The subset of `df` with `df[date_col] < cutoff_date`.

**Example**

```python
import datetime as dt
import polars as pl
from sportsdataverse.nhl.nhl_prediction_constants import as_of_ratings_split
df = pl.DataFrame({"date": [dt.date(2023, 1, 1), dt.date(2023, 1, 2)]})
as_of_ratings_split(df, dt.date(2023, 1, 2))
```

### brier_score {#brier_score}

`brier_score(y_true: 'np.ndarray', p_pred: 'np.ndarray') -> 'float'`

Mean squared error between predicted probabilities and binary outcomes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |

**Returns**

The Brier score (0.0 is a perfect forecast).

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import brier_score
brier_score(np.array([1, 0]), np.array([0.9, 0.1]))
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

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import calibration_table
calibration_table(np.array([1, 0, 1, 0]), np.array([0.9, 0.1, 0.8, 0.2]))
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

### pwhl_team_ratings {#pwhl_team_ratings}

`pwhl_team_ratings(seasons: 'Any', *, league: 'str' = 'pwhl', **kwargs: 'Any') -> 'Any'`

PWHL opponent-adjusted, shrunk even-strength xG team ratings.

Delegates to `sportsdataverse.nhl.nhl_team_ratings.nhl_team_ratings`
with `league="pwhl"` defaulted. Oracle gate deferred (no xG-bearing
PWHL pbp yet -- see module docstring).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Any` |  | an int or iterable of seasons. |
| `league` | `str` | `'pwhl'` | league key (defaults to `"pwhl"`). |

**Returns**

The NHL core's ratings frame, computed with PWHL constants.

**Example**

```python
from sportsdataverse.pwhl.pwhl_team_ratings import pwhl_team_ratings
ratings = pwhl_team_ratings(2024)
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
