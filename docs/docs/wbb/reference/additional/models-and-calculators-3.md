---
title: "Women's college basketball — additional Python functions — Models and calculators: wbb_team–win_prob"
sidebar_label: "Models and calculators: wbb_team–win_prob"
sidebar_position: 10
description: "Women's college basketball — additional Python functions — Models and calculators: wbb_team–win_prob — function reference in sdv-py, the SportsDataverse Python package."
---
# Women's college basketball — additional Python functions — Models and calculators: wbb_team–win_prob

### wbb_team_ratings {#wbb_team_ratings}

`wbb_team_ratings(seasons: 'Union[int, list[int]]', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Women's opponent-adjusted team ratings (AdjO/AdjD/AdjEM/AdjTempo).

Delegates to `sportsdataverse.mbb.mbb_team_ratings.mbb_team_ratings`
with `league="womens"` (WBB loaders + women's fitted constants).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2024`) or list of seasons. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per (season, team_id) -- see the mbb core for the schema.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `team_id` | String | Unique team identifier. |
| `adj_o` | Float64 | Adj o. |
| `adj_d` | Float64 | Adj d. |
| `adj_em` | Float64 | Adj em. |
| `adj_tempo` | Float64 |  |
| `raw_o` | Float64 | Raw o. |
| `raw_d` | Float64 | Raw d. |
| `games` | Int64 | Games played. |
| `rank` | Int64 | Rank. |
| `adj_em_z` | Float64 |  |

**Example**

```python
from sportsdataverse.wbb import wbb_team_ratings
wbb_team_ratings(2024).sort("rank").head()
```

### win_prob_from_margin {#win_prob_from_margin}

`win_prob_from_margin(exp_margin: 'float') -> 'float'`

Women's home win probability from an expected margin.

Delegates to `sportsdataverse.mbb.mbb_game_predict.win_prob_from_margin` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home-minus-away margin in points. |

**Returns**

Probability the home team wins, in `(0, 1)`.

**Example**

```python
from sportsdataverse.wbb.wbb_game_predict import win_prob_from_margin
win_prob_from_margin(5.0)
```
