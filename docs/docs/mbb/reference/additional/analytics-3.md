---
title: "Men's college basketball — additional Python functions — Analytics: test_positional–weighted_avg"
sidebar_label: "Analytics: test_positional–weighted_avg"
sidebar_position: 15
description: "Men's college basketball — additional Python functions — Analytics: test_positional–weighted_avg — function reference in sdv-py, the SportsDataverse Python package."
---
# Men's college basketball — additional Python functions — Analytics: test_positional–weighted_avg

### test_positional_aware_filter {#test_positional_aware_filter}

`test_positional_aware_filter(sorted_to_test: 'list[dict[str, str]]', pve_frags: 'list[dict[str, Any]]', nve_frags: 'list[dict[str, Any]]') -> 'bool'`

Check a positional-aware filter (from `build_positional_aware_filter`)

against a sorted (`order_lineup`-ordered) lineup array.

Faithful port of `PositionUtils.testPositionalAwareFilter`
(`PositionUtils.ts:831-858`). A fragment matches if any of its
position-restricted slots (or, when `pos` is empty, any slot at all)
has a `code`/`id` containing the fragment's filter text
(case-insensitive substring match). Every positive fragment must match
(vacuously true if there are none); no negative fragment may match
(vacuously true if there are none).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_to_test` | `list[dict[str, str]]` |  | The ordered lineup, each a `{"id": ..., "code": ...}` dict (as returned by `order_lineup`). |
| `pve_frags` | `list[dict[str, Any]]` |  | Positive-filter fragments (must ALL match). |
| `nve_frags` | `list[dict[str, Any]]` |  | Negative-filter fragments (NONE may match). |

**Returns**

Whether the lineup satisfies both the positive and negative filters.

**Example**

```python
::

from sportsdataverse.mbb.mbb_positions import test_positional_aware_filter
lineup = [{"code": "AnCowan", "id": "Cowan, Anthony"}]
test_positional_aware_filter(lineup, [{"filter": "cowan", "pos": []}], [])
```

### using_roster_pos {#using_roster_pos}

`using_roster_pos(pos_class: 'str', roster_pos: 'str | None') -> 'tuple[str, str | None]'`

Reconcile a stats-derived position class against roster metadata.

Faithful port of `PositionUtils.usingRosterPos` (`PositionUtils.ts:583-626`).
When the classifier landed on an "unsure" bucket (`"G?"`/`"F/C?"`),
roster info narrows it (a roster `"C"` always wins outright); otherwise
an obviously-wrong stats classification is compromised toward the
roster-implied side, gated by `pos_class_to_score` thresholds.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pos_class` | `str` |  | The stats-derived position class. |
| `roster_pos` | `str \| None` |  | The roster-reported position (`"G"`/`"F"`/`"C"`), or `None`/`""` when unknown. `if (rosterPos)` (ts:587) is a plain JS truthiness check on a string -- `""` and `None` behave identically (both mean "no correction"), so `if not roster_pos` is the faithful Python mirror, not an `is None` landmine. |

**Returns**

A `(position, info)` tuple. `info` is `None` when no correction/explanation applies (matches the TS `undefined`), else a human-readable note on why the position was adjusted.

**Example**

```python
from sportsdataverse.mbb.mbb_positions import using_roster_pos
using_roster_pos("G?", "C")
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
