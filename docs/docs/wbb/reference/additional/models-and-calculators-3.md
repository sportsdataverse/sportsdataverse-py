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
| `adj_o_blend` | Float64 | Adjusted offensive efficiency with the season sim's preseason prior blended in: the same fixed point as adj_o, each Division I team shrunk toward last season's regressed rating as if it had played prior_games extra games at it. |
| `adj_d_blend` | Float64 | Adjusted defensive efficiency with the preseason prior blended in, on the same basis as adj_o_blend (lower is better). |
| `adj_em_blend` | Float64 | adj_o_blend - adj_d_blend: the adjusted efficiency margin with the preseason prior, the rating the pregame projection uses. A team with g games is rated about g / (g + prior_games) on this season. |
| `rank_blend` | Int64 | Dense rank of adj_em_blend within the season (1 = best). |
| `prior_games` | Float64 | The preseason prior's weight in games (4 men, 3 women, fit by tools/fit_preseason_prior.py); 0 when the previous season is not released, and the blend then equals the raw rating. The five blend columns are in season assets built after 2026-11-13 (#1193); an asset published before then lacks them until the producer rebuilds it. |

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
