---
title: "MLB — additional Python functions — Analytics: tto_penalty–x_era"
sidebar_label: "Analytics: tto_penalty–x_era"
sidebar_position: 8
description: "MLB — additional Python functions — Analytics: tto_penalty–x_era — function reference in sdv-py, the SportsDataverse Python package."
---
# MLB — additional Python functions — Analytics: tto_penalty–x_era

### tto_penalty_table {#tto_penalty_table}

`tto_penalty_table(feats: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Observed mean run value by times-through-order, with the penalty vs TTO=1.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `feats` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.add_sequence_features` (needs `times_through_order` and `run_value`). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`times_through_order`, `mean_run_value`, `penalty_vs_first` (`mean_run_value` minus the TTO=1 mean run value), `n`. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `times_through_order` | Int64 | Times through the batting order (1-3). |
| `mean_run_value` | Float64 | Mean observed run value for pitches at this TTO level. |
| `penalty_vs_first` | Float64 | mean_run_value minus the TTO=1 mean run value. |
| `n` | Int64 | Number of pitches observed at this TTO level. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
from sportsdataverse.mlb.mlb_pitch_fatigue import tto_penalty_table
feats = add_sequence_features(pitch_features(raw_pitches))
out = tto_penalty_table(feats)
print(out.sort("times_through_order"))
```

### x_era {#x_era}

`x_era(pitches: 'pl.DataFrame', season: 'int', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Parametric xERA from Statcast expected wOBA allowed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw (or feature-substrate) pitch frame carrying `pitcher` and `estimated_woba_using_speedangle`. |
| `season` | `int` |  | Season year (selects the league baseline via `sportsdataverse.mlb.mlb_pitching_constants.get_baselines`). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `season`, `x_woba`, `x_era`. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | Int64 | Whether the position is a pitcher. |
| `season` | Int64 | Season year. |
| `x_woba` | Float64 | Expected wOBA allowed, the mean of Statcast estimated_woba_using_speedangle over the pitcher's pitches that carry one (batted balls). |
| `x_era` | Float64 | Parametric xERA, league ERA + (x_woba - league wOBA) / wOBA scale * PA per 9, from the season's league baselines. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_era import x_era
out = x_era(raw_pitches, 2024)
print(out.sort("x_era").head())
```
