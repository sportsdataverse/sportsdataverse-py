---
title: "CFB — additional Python functions — Calculate"
sidebar_label: "Calculate"
sidebar_position: 1
description: "CFB — additional Python functions — Calculate — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Calculate

### calculate_completion_probability {#calculate_completion_probability}

`calculate_completion_probability(df, *, season=None, return_as_pandas=False)`

Completion probability for each pass attempt.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `down`, `distance`, `yards_to_goal`, `score_diff`, `seconds_remaining`, `is_home`, `period`, `passing_down`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with a `cp` column appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_completion_probability
calculate_completion_probability(df, season=2024)
```

### calculate_epa {#calculate_epa}

`calculate_epa(df, *, season=None, return_as_pandas=False)`

Expected points added: the change in EP across a play.

Recomputes `ep` when it is absent, matching nflfastR's behaviour. Requires
`ep_end` -- the expected points after the play -- because EPA is a
difference and this function scores rows, not sequences.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame with the EP features and an `ep_end` column. |
| `season` |  | `None` | Unused by the EP model; accepted for signature consistency. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with `ep` (if it was absent) and `epa` appended.

**Example**

```python
from sportsdataverse.cfb import calculate_epa
calculate_epa(pbp)
```

### calculate_expected_points {#calculate_expected_points}

`calculate_expected_points(df, *, season=None, return_as_pandas=False)`

Expected points for each row.

Mirrors `sportsdataverse.nfl.calculate_expected_points()`. The EP booster
is `multi:softprob` over seven next-score classes; this collapses those
probabilities to a points expectation using the package's own
`ep_class_to_score_mapping` rather than restating the class order.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `TimeSecsRem`, `yards_to_goal`, `distance`, `down_1` through `down_4` and `pos_score_diff_start`. |
| `season` |  | `None` | Unused by this model (EP consumes no era feature); accepted so every calculator shares one signature. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with the seven class probability columns and an `ep` column appended.

**Example**

```python
from sportsdataverse.cfb import calculate_expected_points
calculate_expected_points(pbp)
```

### calculate_field_goal_probability {#calculate_field_goal_probability}

`calculate_field_goal_probability(df, *, season=None, return_as_pandas=False)`

Field-goal make probability for each row.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `yards_to_goal`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with an `fg_make_prob` column appended. Named `fg_make_prob`, not `fg_prob`: `calculate_expected_points` emits `fg_prob` for the probability the NEXT SCORE is a field goal, which is a different quantity. Sharing the name made chaining the two silently lossy. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_field_goal_probability
calculate_field_goal_probability(df, season=2024)
```

### calculate_fourth_down {#calculate_fourth_down}

`calculate_fourth_down(df, *, season=None, return_as_pandas=False)`

Fourth-down conversion model output for each row.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `down`, `distance`, `yards_to_goal`, `posteam_total`, `posteam_spread`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with `fd_conversion_prob` (probability the gain reaches `distance`) and `fd_expected_yards` appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_fourth_down
calculate_fourth_down(df, season=2024)
```

### calculate_qbr {#calculate_qbr}

`calculate_qbr(df, *, season=None, return_as_pandas=False)`

Model QBR for each row.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `qbr_epa`, `sack_epa`, `pass_epa`, `rush_epa`, `pen_epa`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with a `qbr` column appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_qbr
calculate_qbr(df, season=2024)
```

### calculate_two_point_probability {#calculate_two_point_probability}

`calculate_two_point_probability(df, *, season=None, return_as_pandas=False)`

Two-point conversion success probability.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `posteam_spread`, `posteam_total`, `pos_score_diff`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with a `two_pt_prob` column appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_two_point_probability
calculate_two_point_probability(df, season=2024)
```

### calculate_win_probability {#calculate_win_probability}

`calculate_win_probability(df, *, season=None, return_as_pandas=False)`

Win probability for each row.

Selects the booster the way the pipeline does: `wp_spread` when the frame
carries a `spread_time` column, `wp_naive` otherwise. The naive model is
the spread model minus that single feature, so which one applies is decided
by whether the caller has spread information at all.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying the win-probability features. Include `spread_time` to use the spread model. |
| `season` |  | `None` | Unused by these models; accepted for signature consistency. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with a `wp` column appended.

**Example**

```python
from sportsdataverse.cfb import calculate_win_probability
calculate_win_probability(pbp)
```

### calculate_wpa {#calculate_wpa}

`calculate_wpa(df, *, season=None, return_as_pandas=False)`

Win probability added: the change in WP across a play.

Recomputes `wp` when it is absent. Requires `wp_end` for the same reason
`calculate_epa` requires `ep_end`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame with the WP features and a `wp_end` column. |
| `season` |  | `None` | Unused by these models; accepted for signature consistency. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with `wp` (if it was absent) and `wpa` appended.

**Example**

```python
from sportsdataverse.cfb import calculate_wpa
calculate_wpa(pbp)
```

### calculate_xpass {#calculate_xpass}

`calculate_xpass(df, *, season=None, return_as_pandas=False)`

Expected pass probability for each row.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `down`, `distance`, `yards_to_goal`, `pos_score_diff`, `TimeSecsRem`, `period`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with an `xpass` column (probability the play is a pass) appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_xpass
calculate_xpass(df, season=2024)
```
