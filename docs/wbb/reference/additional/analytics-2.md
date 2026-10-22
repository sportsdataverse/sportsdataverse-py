# WBB — additional Python functions — Analytics: wbb_bracketology–weighted_avg

> WBB — additional Python functions — Analytics: wbb_bracketology–weighted_avg — function reference in sdv-py, the SportsDataverse Python package.

### wbb_bracketology {#wbb_bracketology}

`wbb_bracketology(season: 'int', *, as_of_date: 'Union[datetime.date, None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Women's projected tournament field for a season.

Delegates to `sportsdataverse.mbb.mbb_bracketology.mbb_bracketology` with `league="womens"` (WBB loaders + women's constants).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season to project (e.g. `2024`). |
| `as_of_date` | `Union[date, None]` | `None` | Only use games strictly before this date; `None` uses every completed game. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `season, team_id, resume_score, projected_seed, at_large_prob, auto_bid, bid` -- see the mbb core for the full contract.

No returns table is published for this function: no capture: it raises a join-key dtype mismatch on every real season (the schedule's home_team_id is Int32, the ratings' team_id is String).

**Example**

```python
from sportsdataverse.wbb import wbb_bracketology
field = wbb_bracketology(2024)
```

### wbb_strength_of_schedule {#wbb_strength_of_schedule}

`wbb_strength_of_schedule(seasons: 'list[int]', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Women's season-level SoS / Quad / WAB résumé.

Delegates to `sportsdataverse.mbb.mbb_strength_of_schedule.mbb_strength_of_schedule` with `league="womens"` (WBB loaders + women's constants).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to compute (e.g. `[2024]`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (season, team_id): `season, team_id, sos, sos_rank, wab, quad1_w .. quad4_l, quality_wins` -- see the mbb core for the full contract.

No returns table is published for this function: no capture: it raises a join-key dtype mismatch on every real season (the schedule's home_team_id is Int32, the ratings' team_id is String).

**Example**

```python
from sportsdataverse.wbb import wbb_strength_of_schedule
wbb_strength_of_schedule([2024]).sort("wab", descending=True).head(20)
```

### weighted_avg {#weighted_avg}

`weighted_avg(mutable_acc: 'LineupStatSet', obj: 'LineupStatSet') -> 'None'`

Merge `obj` into `mutable_acc` with possession weighting.

Faithful port of `LineupUtils.weightedAvg` (`LineupUtils.ts:645`).
Mutates `mutable_acc` in place (matching the upstream mutable-state
contract) and returns `None`. Each call accumulates a **weighted
sum**, not a weighted average -- the companion `completeWeightedAvg`
(upstream `LineupUtils.ts:752`, not yet ported) divides by the
accumulated weight totals to finish the average. The per-field weight
used at each merge step is derived from `obj`'s *own* totals (e.g.
that single lineup's `total_off_fga`), not from any running total on
`mutable_acc` -- callers accumulating many lineups must call
`weighted_avg` once per lineup so every lineup contributes its own
weight.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mutable_acc` | `LineupStatSet` |  | The running accumulator (`LineupStatSet`). Mutated in place; fields absent from the accumulator are initialized to `{"value": 0.0}` (plus `old_value` / `override` when `obj`'s field carries a luck-adjustment `override` marker) before `obj`'s contribution is added. |
| `obj` | `LineupStatSet` |  | The per-lineup `LineupStatSet` document to merge in. |

**Returns**

None. `mutable_acc` is mutated in place.

**Raises**

| Exception | Description |
|---|---|
| `NotImplementedError` | If `obj` carries a `game_info` key -- that merge branch is deliberately deferred (see module docstring). |

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import weighted_avg

acc: dict = {}
weighted_avg(acc, lineup_a)
weighted_avg(acc, lineup_b)
print(acc["off_poss"]["value"])  # plain sum (SUM_FIELDS)

# Two-lineup possession-weighted merge

acc = {}
for lineup in three_lineups:
    weighted_avg(acc, lineup)
# acc now holds weighted SUMS; complete_weighted_avg (not yet
# ported) is required to turn these into rate-stat averages.
```
