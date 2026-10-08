---
title: "MBB — additional Python functions — Models and calculators: spearman_corr–win_prob"
sidebar_label: "Models and calculators: spearman_corr–win_prob"
sidebar_position: 11
description: "MBB — additional Python functions — Models and calculators: spearman_corr–win_prob — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — Models and calculators: spearman_corr–win_prob

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
