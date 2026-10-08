---
title: "MBB — additional Python functions — Analytics: pos_class–weighted_avg"
sidebar_label: "Analytics: pos_class–weighted_avg"
sidebar_position: 13
description: "MBB — additional Python functions — Analytics: pos_class–weighted_avg — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — Analytics: pos_class–weighted_avg

### pos_class_to_score {#pos_class_to_score}

`pos_class_to_score(pos_class: 'str') -> 'int'`

Ordinal "positional weight" for a position class, PG=1000..C=8000.

Faithful port of `PositionUtils.posClassToScore` (`PositionUtils.ts:629-654`,
a literal `switch`). Unmapped classes default to `4000` (the TS
default-case comment notes "won't happen").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pos_class` | `str` |  | A position-class code (e.g. `"PG"`, `"WF"`, `"C"`). |

**Returns**

The class's ordinal score.

**Example**

```python
::

from sportsdataverse.mbb.mbb_positions import pos_class_to_score
pos_class_to_score("WF")
```

### project_bracket {#project_bracket}

`project_bracket(resume: 'pl.DataFrame', auto_bids: 'set[str]', *, league: 'str' = 'mens', field_size: 'int' = 68) -> 'pl.DataFrame'`

Select and seed a tournament field from a per-team résumé frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `resume` | `DataFrame` |  | One row per (season, team_id) with `adj_em_z, sos, wab, quad1_w` (the ratings + strength-of-schedule outputs joined). |
| `auto_bids` | `set[str]` |  | `team_id` set of conference auto-bid winners (see conference_auto_bids`); always in the field. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (kept for shim parity; the blend is league-agnostic). |
| `field_size` | `int` | `68` | Tournament field size (68). |

**Returns**

One row per input team: `season, team_id, resume_score, projected_seed` (1-16, capped for the First Four; null outside the field), `at_large_prob` (logistic in `resume_score` centred on the selection cutoff -- every selected at-large clears 0.5), `auto_bid`, `bid` (exactly `field_size` true).

No returns table is published for this function: no capture: it needs a resume frame (strength of schedule joined with the ratings' adj_em_z) that no package function returns.

**Example**

```python
from sportsdataverse.mbb.mbb_bracketology import project_bracket
field = project_bracket(resume, auto_bids)
```

### regress_shot_quality {#regress_shot_quality}

`regress_shot_quality(stat: 'float', pos: 'int', feat: 'str', player: 'dict[str, Any]') -> 'float'`

Shrink a small-sample shot-quality stat toward its positional average.

Faithful port of `PositionUtils.regressShotQuality`
(`PositionUtils.ts:216-258`). Only the three relative shot-quality
features (`calc_three_relative` / `calc_rim_relative` /
`calc_mid_relative`) are regressed; any other `feat` passes `stat`
through unchanged. A player is regressed toward the positional average
whenever the relevant shot volume is below `max(0.25 * total_fga, 15)`
(i.e. under 25% of their attempts come from that zone, floored at 15
attempts). A `center` (`pos == 4`) who took 0-2 threes and made none is
left at `0` to avoid widespread changes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat` | `float` |  | The raw (unregressed) feature value. |
| `pos` | `int` |  | Position index (`0=pg` ... `4=c`). |
| `feat` | `str` |  | Feature field name (only the three relative shot-quality keys trigger regression; anything else is a passthrough). |
| `player` | `dict[str, Any]` |  | The player stat dict; reads `total_off_fga` and the per-feature volume field (`total_off_{3p,2pmid,2prim}_attempts`), each shaped `{"value": N}`. |

**Returns**

The regressed feature value (or `stat` unchanged when the feature is not regressed, volume is sufficient, or the center-3s carve-out fires).

**Example**

```python
from sportsdataverse.mbb.mbb_positions import regress_shot_quality
player = {"total_off_fga": {"value": 25},
          "total_off_3p_attempts": {"value": 1}}
regress_shot_quality(-15.5, 2, "misc_feature", player)

# Low-volume shrink toward the positional average

regress_shot_quality(100, 3, "calc_rim_relative",
    {"total_off_fga": {"value": 25},
     "total_off_2prim_attempts": {"value": 8}})
```

### strength_of_schedule {#strength_of_schedule}

`strength_of_schedule(results: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Per-team SoS + Quad 1-4 record + WAB from completed games and ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | Completed games with `game_id, season, home_team_id, away_team_id, home_score, away_score, neutral_site`. |
| `ratings` | `DataFrame` |  | One row per team with `season, team_id, adj_em, rank` (the `mbb_team_ratings` output). Team-id dtype must match `results`. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (quad thresholds, HFA, bubble EM). |

**Returns**

One row per (season, team_id): `season, team_id, sos, sos_rank, wab, quad1_w .. quad4_l, quality_wins`. `sos` is the mean opponent `adj_em` (rank 1 = hardest schedule); quads follow the NET venue-adjusted opponent-rank thresholds; `quality_wins` is Quad-1 + Quad-2 wins; `wab` is actual wins minus a bubble-quality team's expected wins against the same schedule. Empty input returns the schema with zero rows.

No returns table is published for this function: no capture: it needs a schedule with home_team_id / away_team_id columns, and no package function returns one (load_mbb_schedule ships home_id / away_id).

**Example**

```python
from sportsdataverse.mbb.mbb_strength_of_schedule import strength_of_schedule
resume = strength_of_schedule(results, ratings)
```

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
