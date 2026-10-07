---
title: "CFB — additional Python functions — Models and calculators: win_prob"
sidebar_label: "Models and calculators: win_prob"
sidebar_position: 8
description: "CFB — additional Python functions — Models and calculators: win_prob — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Models and calculators: win_prob

### win_prob_from_margin {#win_prob_from_margin}

`win_prob_from_margin(exp_margin: 'float', *, era: 'str' = 'modern') -> 'float'`

Home win probability from an expected margin via the Gaussian CDF.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home margin in points (e.g. from `predict_margin`). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS` supplying `margin_sd`. |

**Returns**

`Phi(exp_margin / margin_sd)` -- the probability the home team wins under a `Normal(exp_margin, margin_sd**2)` margin model. `0.5` at a zero expected margin.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import win_prob_from_margin
win_prob_from_margin(7.0)
```
