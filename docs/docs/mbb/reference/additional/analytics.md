---
title: "MBB — additional Python functions — Analytics: ConcurrentClump–order_lineup"
sidebar_label: "Analytics: ConcurrentClump–order_lineup"
sidebar_position: 12
description: "MBB — additional Python functions — Analytics: ConcurrentClump–order_lineup — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — Analytics: ConcurrentClump–order_lineup

### ConcurrentClump {#ConcurrentClump}

`ConcurrentClump(evs: 'list[RawGameEvent]' = <factory>, lineups: 'list[LineupEvent]' = <factory>) -> None`

A clump of concurrent raw events, together with the lineups that end

in that clump (`Concurrency.ConcurrentClump`, `PossessionUtils.scala
:64-69`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `evs` | `list[RawGameEvent]` | `<factory>` | The raw game events in this clump, in chronological order. |
| `lineups` | `list[LineupEvent]` | `<factory>` | The lineups (if any) whose `end_min` falls in this clump. |

### PossState {#PossState}

`PossState(team_stats: 'PossCalcFragment', opponent_stats: 'PossCalcFragment', prev_clump: 'ConcurrentClump') -> None`

Running state threaded through `calculate_possessions_by_event`

(`PossessionUtils.PossState`, `PossessionUtils.scala:39-49`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_stats` | `PossCalcFragment` |  | Accumulated fragment for the team since the last lineup boundary. |
| `opponent_stats` | `PossCalcFragment` |  | Accumulated fragment for the opponent since the last lineup boundary. |
| `prev_clump` | `ConcurrentClump` |  | The previously-processed merged clump (used by `calculate_stats`'s and-one / deadball-rebound heuristics). |

**Methods**

#### PossState.init

`PossState.init() -> "'PossState'"`

A fresh starting state (`PossState.init`, `:45-48`) -- builds

new instances per call, matching the mutable-dataclass rationale in
`mbb_ncaa_models.py`'s `.empty()` factories.

### apply_relative_positional_overrides {#apply_relative_positional_overrides}

`apply_relative_positional_overrides(results: 'list[dict[str, str]]', team_season: 'str', recurse_count: 'int' = 0) -> 'list[dict[str, str]]'`

Recursively re-shuffle an ordered lineup per `RELATIVE_POSITION_FIXES`.

Faithful port of the private `PositionUtils.applyRelativePositionalOverrides`
(`PositionUtils.ts:657-693`). Finds the first rule (in table order) whose
`key` slots all match the current `results` codes (a `None` key slot
matches anything), applies that rule's `rule` slots (`None` = leave
unchanged, `int` = 1-based back-reference into the *pre-rule* results,
`dict` = literal replacement) to produce a new ordering, then recurses on
the new ordering -- since one swap can expose a second rule to match (e.g.
the Maryland 2019/20 Morsell/Wiggins swap can cascade into the Lindo/Smith
swap). Recursion is bounded by `recurse_count < len(rules)` (ported
verbatim from the TS bound), so it always terminates even if two rules
somehow ping-ponged each other.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `list[dict[str, str]]` |  | The current 5-slot `{"code": ..., "id": ...}` ordering (PG/SG/SF/PF/C, index 0-4). |
| `team_season` | `str` |  | Key into `RELATIVE_POSITION_FIXES`. A team/season absent from the table (or the recursion exhausting that team/season's rule count) returns `results` unchanged. |
| `recurse_count` | `int` | `0` | Internal recursion depth counter -- callers should not pass this explicitly (mirrors the TS default parameter). |

**Returns**

The (possibly re-shuffled) 5-slot ordering.

**Example**

```python
from sportsdataverse.mbb.mbb_positions import apply_relative_positional_overrides
results = [
    {"code": "AnCowan", "id": "Cowan, Anthony"},
    {"code": "ErAyala", "id": "Ayala, Eric"},
    {"code": "DaMorsell", "id": "Morsell, Darryl"},
    {"code": "AaWiggins", "id": "Wiggins, Aaron"},
    {"code": "JaSmith", "id": "Smith, Jalen"},
]
apply_relative_positional_overrides(results, "Men_Maryland_2019/20")
```

### assign_to_right_lineup {#assign_to_right_lineup}

`assign_to_right_lineup(state: 'PossState', team_stats: 'PossCalcFragment', opponent_stats: 'PossCalcFragment', clump: 'ConcurrentClump', prev_clump: 'ConcurrentClump') -> 'list[LineupEvent]'`

Assign a clump's possessions to the lineup(s) ending in it

(`PossessionUtils.assign_to_right_lineup`, `PossessionUtils.scala
:418-518`).

Applies the running `state` total (accumulated since the last lineup
boundary) to the *first* ending lineup only, then hands off to
`lineup_balancer` (this clump's own fragment, split across
candidates if there's more than one) and finally `lineup_fixer`
(the negative-possession clamp).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `state` | `PossState` |  | The running possession state since the last lineup boundary. |
| `team_stats` | `PossCalcFragment` |  | This clump's team-direction fragment. |
| `opponent_stats` | `PossCalcFragment` |  | This clump's opponent-direction fragment. |
| `clump` | `ConcurrentClump` |  | The merged clump ending one or more lineups. |
| `prev_clump` | `ConcurrentClump` |  | The previous merged clump. |

**Returns**

The lineup(s) ending in this clump, enriched with possession counts. Empty if `clump.lineups` is empty (see the module docstring's landmine-index note -- unreachable via `calculate_possessions_by_event`).

### build_3p_shot_info {#build_3p_shot_info}

`build_3p_shot_info(p: 'LineupStatSet') -> 'OffLuckShotInfo3P'`

3P-only shot-decomposition wrapper.

Public port of `build3PShotInfo` (`LuckUtils.ts:741-759`) --
remaps build_shot_info`'s generic keys to the 3pm`/
3pa`/3p` suffixes used throughout the luck-adjustment engine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `p` | `LineupStatSet` |  | The player's `LineupStatSet`/`IndivStatSet`-shaped dict. |

**Returns**

`{"shot_info_ast_3pm", "shot_info_early_3pa", "shot_info_scramble_3pa", "shot_info_unast_3pm", "shot_info_unknown_3pM", "shot_info_total_3p"}`.

**Example**

```python
from sportsdataverse.mbb.mbb_luck import build_3p_shot_info

info = build_3p_shot_info(player)
print(info["shot_info_total_3p"])
```

### build_adjusted_3p {#build_adjusted_3p}

`build_adjusted_3p(p: 'LineupStatSet', info: 'OffLuckShotInfo3P') -> 'OffLuckAdj3P'`

3P-only approx-unassisted/assisted-FG% wrapper.

Public port of `buildAdjusted3P` (`LuckUtils.ts:812-835`, "retained
for bwc [backwards compat]" per the upstream comment) -- a thin remap of
build_adjusted_fg` called with `shot_type="3p"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `p` | `LineupStatSet` |  | The (typically base-period) player dict driving `off_3p`/ `off_3p_ast`. |
| `info` | `OffLuckShotInfo3P` |  | An `build_3p_shot_info`-shaped dict (the "biggest sample available" per the upstream comment -- normally the base period, not the sample being luck-adjusted). |

**Returns**

`{"base3P", "unassisted3P", "assisted3P", "baseAssistPct"}`.

**Example**

```python
from sportsdataverse.mbb.mbb_luck import build_3p_shot_info, build_adjusted_3p

base_info = build_3p_shot_info(base_player)
adj = build_adjusted_3p(base_player, base_info)
print(adj["assisted3P"], adj["unassisted3P"])
```

### build_efficiency_margins {#build_efficiency_margins}

`build_efficiency_margins(mutable_stat_set: 'LineupStatSet', key_override: 'str | None' = None) -> 'None'`

Derive `off_net` / `off_raw_net` on a stat set, in place.

Faithful port of `LineupUtils.buildEfficiencyMargins` (`LineupUtils.ts:145`).
`off_net` is `off_adj_ppp - def_adj_ppp` (adjusted efficiency margin);
`off_raw_net` is `off_ppp - def_ppp` (raw/unadjusted margin). Both are
only written when their two source fields are both present on
`mutable_stat_set`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mutable_stat_set` | `LineupStatSet` |  | The `LineupStatSet` (or team-report equivalent) to mutate in place. |
| `key_override` | `str \| None` | `None` | `"value"` or `"old_value"` -- which sub-key to read from the source fields and write into `off_net` / `off_raw_net`. When `None` (the default), the upstream `nonLuckKey` fallback applies: use `"old_value"` if `mutable_stat_set["off_ppp"]["old_value"]` is present, otherwise `"value"`. When given explicitly, the written field is merged onto any existing `off_net` / `off_raw_net` dict (so a second call with the other key preserves the first call's key) rather than replacing it outright. |

**Returns**

None. `mutable_stat_set` is mutated in place.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import build_efficiency_margins

build_efficiency_margins(team_info, "value")
off_ppp = team_info.get("off_ppp")
if isinstance(off_ppp, dict) and off_ppp.get("old_value") is not None:
    build_efficiency_margins(team_info, "old_value")
print(team_info["off_net"]["value"])
```

### build_exp_3p {#build_exp_3p}

`build_exp_3p(info: 'OffLuckShotTypeAndAdj3P') -> 'float'`

Expected made-3P count given a player's shot-type mix + shooting %s.

Public port of `buildExp3P` (`LuckUtils.ts:838-847`): `(assisted
3PM * assisted3P%) + (unassisted 3PM * unassisted3P%) +
(early/scramble/unknown 3PA * base3P%)`. Pure weighted sum -- no
division, so this introduces no landmine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `info` | `OffLuckShotTypeAndAdj3P` |  | A dict carrying both `build_3p_shot_info`'s `shot_info_*` keys and `build_adjusted_3p`'s `*3P` keys (i.e. an `OffLuckShotTypeAndAdj3P`). |

**Returns**

The expected number of made 3-pointers (`3P% * total 3P`).

**Example**

```python
from sportsdataverse.mbb.mbb_luck import (
    build_3p_shot_info, build_adjusted_3p, build_exp_3p,
)

base_info = build_3p_shot_info(base_player)
info = {**build_3p_shot_info(player), **build_adjusted_3p(base_player, base_info)}
expected_makes = build_exp_3p(info)
```

### build_position {#build_position}

`build_position(confs: 'dict[str, float]', confs_no_height: 'dict[str, float] | None', player: 'dict[str, Any]', team_season: 'str') -> 'tuple[str, str]'`

Classify a player into a position label + diagnostic trace string.

Faithful port of `PositionUtils.buildPosition` (`PositionUtils.ts:401-580`)
-- the PG / s-PG / CG / WG / WF / S-PF / PF/C / C decision tree. A
`ABSOLUTE_POSITION_FIXES` manual override short-circuits the whole
tree (recursing once, with `team_season=""`, purely to compute the
diagnostic "what would this have been" string); otherwise the function
walks the confidence-threshold / assist-rate / 3PT-rate branch cascade,
applies the "too few effective possessions" (< 25) fallback, and
reconciles the result against roster metadata via `using_roster_pos`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `confs` | `dict[str, float]` |  | The 5-way positional confidence dict (`TRAD_POS_LIST` keys), typically the height-adjusted output of `build_position_confidences`. |
| `confs_no_height` | `dict[str, float] \| None` |  | The pre-height-adjustment confidences, or `None` when the caller has no height data. When present, a PG <-> s-PG flip caused solely by the height adjustment is reverted (the `maybeIgnoreHeight` closure, `ts:433-457`). The check is `is not None` (JS object-truthiness: an empty dict is still a truthy JS object), NOT a Python-falsy `if confs_no_height`. |
| `player` | `dict[str, Any]` |  | The player stat dict. Reads `key` (override lookup), `off_assist` / `off_3pr` / `off_usage` / `off_team_poss` (each `{"value": N}`-wrapped), and `roster` (a plain `{"pos": ..., "role": ...}` dict of un-wrapped strings). |
| `team_season` | `str` |  | `"{sport}_{team}_{season}"` key into `ABSOLUTE_POSITION_FIXES`. Pass `""` to disable override lookup for a given call (the recursive diagnostic call inside the override branch does exactly this). |

**Returns**

A `(position, diagnostic)` tuple. `position` is one of `ID_TO_POSITION`'s keys; `diagnostic` is a human-readable trace of which rule fired, byte-identical to the TS's template strings (including `.toFixed(1)`-style percentage formatting).

**Example**

```python
from sportsdataverse.mbb.mbb_positions import build_position, TRAD_POS_LIST
confs = dict(zip(TRAD_POS_LIST, [0.9, 0.1, 0, 0, 0]))
player = {"off_assist": {"value": 0.10}, "off_3pr": {"value": 0.20},
          "off_team_poss": {"value": 1000}, "off_usage": {"value": 0.20}}
build_position(confs, None, player, "Men_Boston College_2019/20")

# A manual-override short-circuit

build_position(confs, None, {"key": "Popovic, Nik",
    "off_usage": {"value": 1}, "off_team_poss": {"value": 200},
    "off_assist": {"value": 0.10}}, "Men_Boston College_2019/20")
```

### build_position_confidences {#build_position_confidences}

`build_position_confidences(player: 'dict[str, Any]', height_in: 'float | None' = None) -> 'tuple[dict[str, float], dict[str, Any]]'`

Build the 5-way positional confidence vector for a player.

Faithful port of `PositionUtils.buildPositionConfidences`
(`PositionUtils.ts:263-338`). Derives the six `calc_*` ratios from the
player's box-score fields, dot-products the resulting 17-feature vector
against `POSITION_FEATURE_WEIGHTS` (each field regressed via
`regress_shot_quality` and multiplied by its per-feature `scale`)
plus the `POSITION_FEATURE_INIT` intercepts, applies a softmax over
the five raw scores, and -- when `height_in` is supplied -- reweights the
confidences via `incorporate_height`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player` | `dict[str, Any]` |  | The player stat dict (ES-aggregation bucket shape); each stat field is `{"value": N}`. Reads `total_off_assist`, `total_off_to`, `off_3p`, `off_efg`, `off_2pmid`, `off_2prim`, `total_off_fga`, `total_off_fta`, `total_off_ftm` (for the `calc_*` ratios) plus every non-`calc_` field in `POSITION_FEATURE_WEIGHTS`. |
| `height_in` | `float \| None` | `None` | Optional player height in inches. When truthy, the returned confidences are height-adjusted; when `None` / `0`, the raw softmax confidences are returned. (JS `height_in ? ... : ...` falsy check, ts:324 -- a `0` height is treated as "no height".) |

**Returns**

A `(confidences, diagnostics)` tuple. `confidences` maps each `TRAD_POS_LIST` key (in order) to its final confidence. `diagnostics` carries `"scores"` (raw scores x `0.1`, keyed by position), `"confsNoHeight"` (the pre-height confidences, present only when `height_in` is truthy, else `None`), and `"calculated"` (the six derived `calc_*` ratios). The upstream diag object has exactly these three fields -- no UI-only fields are dropped.

**Example**

```python
from sportsdataverse.mbb.mbb_positions import build_position_confidences
confs, diags = build_position_confidences(player_bucket)
print(confs["pos_pg"], diags["calculated"]["calc_ast_tov"])

# Height-adjusted confidences

confs_h, diags_h = build_position_confidences(player_bucket, 78.0)
```

### build_positional_aware_filter {#build_positional_aware_filter}

`build_positional_aware_filter(filter_str: 'str') -> 'tuple[list[dict[str, Any]], list[dict[str, Any]], bool]'`

Decompose a search-filter string into positionally-aware +ve/-ve fragments.

Faithful port of `PositionUtils.buildPositionalAwareFilter`
(`PositionUtils.ts:764-828`). Picks a fragment separator by scanning
`[";", "/", ","]` in priority order for the first one present anywhere
in `filter_str` (a fragment separator of `"!!!"` -- never itself
present -- is the "no separator found" fallback, which leaves the whole
string as a single fragment). Splits on that separator, trims whitespace,
drops empty fragments and `[`-prefixed ones (reserved for aggregation-key
filters elsewhere in the app), then routes each fragment to the positive
or negative bucket by a leading `-`, and parses each fragment's optional
`=<tokens>` position spec via decomp_positional_filter_fragment`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filter_str` | `str` |  | A raw filter string, e.g. `"test1=pg / -test2=Pf+C / test3"`. |

**Returns**

A `(positive_fragments, negative_fragments, has_position)` triple. Each fragment is `{"filter": <lowercased name>, "pos": [indices]}`. `has_position` is `True` iff any fragment (either side) carried at least one recognized position token.

**Example**

```python
::

from sportsdataverse.mbb.mbb_positions import build_positional_aware_filter
build_positional_aware_filter("test1=pg / -test2=Pf+C / test3")
```

### calc_def_player_luck_adj {#calc_def_player_luck_adj}

`calc_def_player_luck_adj(sample: 'LineupStatSet', base: 'LineupStatSet', avg_eff: 'float') -> 'DefLuckAdjustmentDiags'`

Defensive 3P-luck adjustment for a single player.

Faithful port of `LuckUtils.calcDefPlayerLuckAdj` (`LuckUtils.ts:402-426`).
Unlike `calc_off_player_luck_adj`, this is **not** a pure
delegation -- see the module docstring's `calc_def_player_luck_adj`
note for the `translate()` remap this wraps around
`calc_def_team_luck_adj`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sample` | `LineupStatSet` |  | The player's stat dict for the period being luck-adjusted (must carry `oppo_total_def_3p_made`/`oppo_total_def_3p_attempts` -- there is no player-level `def_3p` field upstream, hence the remap). |
| `base` | `LineupStatSet` |  | The player's stat dict for the baseline/reference period. |
| `avg_eff` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |

**Returns**

Same shape as `calc_def_team_luck_adj`, computed against the translated (`oppo_*` -> `def_*`) player stat dicts.

**Example**

```python
from sportsdataverse.mbb.mbb_luck import calc_def_player_luck_adj

diags = calc_def_player_luck_adj(sample_player, base_player, 100.0)
print(diags["deltaDefAdjEff"])
```

### calc_def_team_luck_adj {#calc_def_team_luck_adj}

`calc_def_team_luck_adj(sample: 'LineupStatSet', base: 'LineupStatSet', avg_eff: 'float', sample_def_3pa_override: 'float | None' = None) -> 'DefLuckAdjustmentDiags'`

Defensive 3P-luck adjustment for a team (or lineup).

Faithful port of `LuckUtils.calcDefTeamLuckAdj` (`LuckUtils.ts:429-531`).
See the module docstring for the SoS-vs-luck-split formula (`LUCK_PCT`)
and the shared unguarded-division landmine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sample` | `LineupStatSet` |  | The team/lineup/player stat dict for the period being luck-adjusted (e.g. an on/off split or a single lineup). |
| `base` | `LineupStatSet` |  | The team/lineup/player stat dict for the baseline/reference period. |
| `avg_eff` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |
| `sample_def_3pa_override` | `float \| None` | `None` | When given, used as `sampleDef3PA` instead of `sample["total_def_3p_attempts"]` -- see `calc_off_team_luck_adj`'s `sample_3pa_override` docstring for the shared "lineup regression" rationale (`LuckUtils.ts:433-434`, verbatim comment). |

**Returns**

A `DefLuckAdjustmentDiags` dict -- TS-verbatim keys (`avgEff`, `luckPct`, `baseDef3P`, `baseDef3PSos`, `baseDef3PA`, `basePoss`, `base3PSosAdj`, `sampleDef3P`, `sampleDef3PSos`, `sampleDef3PA`, `samplePoss`, `sample3PSosAdj`, `sampleDefEfg`, `sampleDefPpp`, `sampleOffSos`, `sampleDef3PRate`, `sampleDefFGA`, `sampleDefOrb`, `avg3PSosAdj`, `adjDef3P`, `delta3P`, `deltaDefEfg`, `deltaDefPppNoOrb`, `deltaMissesPct`, `deltaDefOrbFactor`, `deltaPtsOffMisses`, `deltaDefPpp`, `deltaDefAdjEff`).

**Example**

```python
from sportsdataverse.mbb.mbb_luck import calc_def_team_luck_adj

diags = calc_def_team_luck_adj(sample_team_off, base_team, 100.0)
print(diags["deltaDefAdjEff"])
```

### calc_off_player_luck_adj {#calc_off_player_luck_adj}

`calc_off_player_luck_adj(sample_player: 'LineupStatSet', base_player: 'LineupStatSet', avg_eff: 'float') -> 'OffLuckAdjustmentDiags'`

Offensive 3P-luck adjustment for a single player.

Faithful port of `LuckUtils.calcOffPlayerLuckAdj` (`LuckUtils.ts:174-187`).
Per Task 2.1's surprise #4, this is a literal 1-player-team delegation
to `calc_off_team_luck_adj` -- ORB effects are ignored for an
individual player (the upstream comment: "the team calc basically
works fine here, apart from ORBs, which we'll ignore").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sample_player` | `LineupStatSet` |  | The player's stat dict for the period being luck-adjusted. |
| `base_player` | `LineupStatSet` |  | The player's stat dict for the baseline/reference period. |
| `avg_eff` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |

**Returns**

Same shape as `calc_off_team_luck_adj` -- identical to calling that function with `sample_players=[sample_player]`, `base_players_map={base_player["key"]: base_player}`.

**Example**

```python
from sportsdataverse.mbb.mbb_luck import calc_off_player_luck_adj

diags = calc_off_player_luck_adj(sample_player, base_player, 100.0)
print(diags["deltaOffAdjEff"])
```

### calc_off_team_luck_adj {#calc_off_team_luck_adj}

`calc_off_team_luck_adj(sample_team: 'LineupStatSet', sample_players: 'list[LineupStatSet]', base_team: 'LineupStatSet', base_players_map: 'dict[str, LineupStatSet]', avg_eff: 'float', sample_3pa_override: 'float | None' = None, manual_overrides: 'list[ManualOverride] | None' = None) -> 'OffLuckAdjustmentDiags'`

Offensive 3P-luck adjustment for a team (or lineup).

Faithful port of `LuckUtils.calcOffTeamLuckAdj` (`LuckUtils.ts:190-399`).
See the module docstring for the Bayesian-shrink formula, the JS-array-
truthiness / object-selection landmines, and the one unguarded-division
landmine this function carries.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sample_team` | `LineupStatSet` |  | The team/lineup stat dict for the period being luck-adjusted (e.g. an on/off split or a single lineup). |
| `sample_players` | `list[LineupStatSet]` |  | The roster of per-player stat dicts backing `sample_team` (`samplePlayers == players.map(on/off/baseline)` per the upstream comment). |
| `base_team` | `LineupStatSet` |  | The team stat dict for the baseline/reference period (typically full-season). |
| `base_players_map` | `dict[str, LineupStatSet]` |  | `{player_key: base_period_player_stat_dict}`. |
| `avg_eff` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |
| `sample_3pa_override` | `float \| None` | `None` | When given, used as `sample3PA` instead of `sample_team["total_off_3p_attempts"]`. Per the upstream comment (`LuckUtils.ts:196-198`, shared verbatim with `calc_def_team_luck_adj`'s `sample_def_3pa_override`): "when calc'ing luck on lineups, each lineup gets the total sample as its regression so its average is right over the set" -- i.e. this lets every lineup in a sweep share one common 3PA denominator (the team's) for its regression target, rather than each lineup regressing against its own much smaller, noisier 3PA count. Note that `calc_off_player_luck_adj` itself does *not* pass this (its delegation call omits it entirely) -- the jest oracle's own "3P override" cross-check (`LuckUtils.test.ts:100-115`) instead calls `calc_off_team_luck_adj` directly with the player's own 3PA as this override, purely to demonstrate the parameter's effect in isolation. |
| `manual_overrides` | `list[ManualOverride] \| None` | `None` | Per-player 3P%-expectation overrides from the UI. **A non-`None` empty list still activates the team-level override-delta branch** (JS array truthiness) -- see the module docstring's landmine note. `None` (the default) is the "no overrides at all" case. |

**Returns**

An `OffLuckAdjustmentDiags` dict -- TS-verbatim keys (`avgEff`, `samplePoss`, `sample3P`, `sample3PA`, `base3PA`, `player3PInfo` (per-player detail, sorted by descending `shot_info_total_3p`), `sampleBase3P`, `regress3P`, `sampleOff3PRate`, `sampleOffFGA`, `sampleOffOrb`, `sampleOffEfg`, `sampleOffPpp`, `sampleDefSos`, `delta3P`, `deltaOffEfg`, `deltaMissesPct`, `deltaOffPppNoOrb`, `deltaOffOrbFactor`, `deltaPtsOffMisses`, `deltaOffPpp`, `deltaOffAdjEff`).

**Example**

```python
from sportsdataverse.mbb.mbb_luck import calc_off_team_luck_adj

diags = calc_off_team_luck_adj(
    sample_team_on, sample_players_on, base_team, base_players_map, 100.0,
)
print(diags["deltaOffAdjEff"])

# With per-player manual 3P% overrides

diags = calc_off_team_luck_adj(
    sample_team_on, sample_players_on, base_team, base_players_map, 100.0,
    manual_overrides=[
        {"rowId": "Cowan, Anthony", "statName": "off_3p", "newVal": 0.5, "use": True},
    ],
)
```

### calculate_aggregated_lineup_stats {#calculate_aggregated_lineup_stats}

`calculate_aggregated_lineup_stats(lineups: 'list[LineupStatSet] | None') -> 'LineupStatSet'`

Combine all lineups into a single team stat set.

Faithful port of `LineupUtils.calculateAggregatedLineupStats`
(`LineupUtils.ts:106`). Seeds an accumulator from
`StatModels.emptyLineup()` (`{"key": "empty", "doc_count": 0}`) plus
an `all_lineups` sub-accumulator of the same shape, then merges every
lineup via `weighted_avg`: lineups without a truthy `rapmRemove`
key merge into the main accumulator, while `rapmRemove` lineups merge
into `all_lineups` instead (their contribution is folded back in
afterward). Calls `complete_weighted_avg` to turn the main
accumulator's weighted sums into weighted averages, then -- because
`StatModels.emptyLineup()` always carries `key`/`doc_count` and so
is never considered "empty" by the upstream `lodash.isEmpty` check --
unconditionally re-merges the (now-averaged) team totals into
`all_lineups` and finishes that sub-accumulator too. Finally rebuilds
`off_net` / `off_raw_net` via `build_efficiency_margins`
(value-key always; old-value-key too when the team is in luck-adjusted
mode, i.e. `off_ppp.old_value` is present) -- but only on the top-level
result, matching upstream's "don't bother for all_lineups" comment.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `list[LineupStatSet] \| None` |  | The per-lineup `LineupStatSet` docs to fold together (e.g. the ES aggregation buckets under `responses[0].aggregations.lineups.buckets`). `None` or an empty list yields an all-zero/empty team stat set (mirrors the upstream `lineups \|\| []` guard). |

**Returns**

The aggregated team-total `LineupStatSet`, including a nested `all_lineups` key holding the `rapmRemove`-lineups-plus-team-total composite sub-aggregate.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import calculate_aggregated_lineup_stats

buckets = raw_response["responses"][0]["aggregations"]["lineups"]["buckets"]
team_info = calculate_aggregated_lineup_stats(buckets)
print(team_info["off_ppp"]["value"], team_info["off_poss"]["value"])

# RAPM-exclusion flag

buckets[1]["rapmRemove"] = True  # divert into all_lineups instead
team_info = calculate_aggregated_lineup_stats(buckets)
```

### calculate_possessions {#calculate_possessions}

`calculate_possessions(lineup_events: 'Iterable[LineupEvent]') -> 'list[LineupEvent]'`

Top-level entry point: calculate team/opponent possessions for a

sequence of lineup events (`PossessionUtils.calculate_possessions`,
`PossessionUtils.scala:371-379`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_events` | `Iterable[LineupEvent]` |  | The lineups to enrich, in chronological order. |

**Returns**

The lineups, each enriched with possession counts.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_possessions import calculate_possessions

enriched = calculate_possessions(lineups)
enriched[0].team_stats.num_possessions
```

### calculate_possessions_by_event {#calculate_possessions_by_event}

`calculate_possessions_by_event(raw_events_as_clumps: 'Iterable[ConcurrentClump]') -> 'list[LineupEvent]'`

Drive the batch loop + per-clump scoring over an already-flattened

clump stream (`PossessionUtils.calculate_possessions_by_event`,
`PossessionUtils.scala:521-573`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `raw_events_as_clumps` | `Iterable[ConcurrentClump]` |  | The unbatched clump stream, e.g. from flat-mapping `lineup_as_raw_clumps` over several lineups. |

**Returns**

The lineups, each enriched with possession counts, in original order.

### calculate_stats {#calculate_stats}

`calculate_stats(clump: 'ConcurrentClump', prev: 'ConcurrentClump', dir: 'Direction') -> 'PossCalcFragment'`

Calculate one direction's possession-fragment for one merged clump

(`PossessionUtils.calculate_stats`, `PossessionUtils.scala:170-369`).

See the upstream source's inline worked examples (and-one detection,
technical/flagrant offsetting, the deadball-rebound heuristic) for the
hand-annotated NCAA play-by-play snippets that motivate each step; this
port reproduces every step in the same order.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `ConcurrentClump` |  | The merged clump to score. |
| `prev` | `ConcurrentClump` |  | The previously-processed merged clump (feeds the and-one and deadball-rebound heuristics -- see below). |
| `dir` | `Direction` |  | Which side (`Direction.TEAM`/`Direction.OPPONENT`) is "attacking" for this calculation. Named to match the Scala (shadows the `dir` builtin -- consistent with this port's existing precedent of naming params after their Scala originals, e.g. `RawGameEvent.for_team`'s `min`). |

**Returns**

A `~sportsdataverse.mbb.mbb_ncaa_models.PossCalcFragment` for this clump/direction.

### complete_weighted_avg {#complete_weighted_avg}

`complete_weighted_avg(mutable_acc: 'LineupStatSet', harmonic_weighting: 'bool' = False, regress_diffs: 'float' = 0.0) -> 'None'`

Finish a `weighted_avg` accumulator into true weighted averages.

Faithful port of `LineupUtils.completeWeightedAvg` (`LineupUtils.ts:752`).
Mutates `mutable_acc` in place and returns `None`, mirroring the
upstream `void` + mutable-arg contract. Recomputes the per-field weight
tables from `mutable_acc` itself (`getSimpleWeights(mutableAcc, 1,
regressDiffs)` -- note the `default_val=1`, unlike `weighted_avg`'s
`default_val=0`), then, unless `harmonic_weighting` is set, calls
recalculate_play_type_poss` to fix up the transition/scramble
possession fields that `weighted_avg` skipped. Finally divides every
non-ignored field's accumulated weighted sum by its matching weight
total (shot-type / `ppp_totals` / `orb_totals` / `fta_totals` /
`ast_totals` / generic FGA fallback); `total_*` and `SUM_FIELDS`
fields are left untouched (they are already true totals, not sums to be
averaged). `off_ftr` / `def_ftr` get a special non-`harmonic_weighting`
recompute straight from the accumulated `total_{off|def}_fta` rather
than dividing their own weighted sum.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mutable_acc` | `LineupStatSet` |  | The `weighted_avg`-accumulated `LineupStatSet` to finish in place. Every field with a non-`total_`/`SUM_FIELDS` key is converted from a weighted sum to a weighted average. |
| `harmonic_weighting` | `bool` | `False` | When `True`, skips the recalculate_play_type_poss` fixup and uses a harmonic-style division for `off_ftr`/`def_ftr` instead of the totals-based recompute. Matches the upstream default (`False`) used by `calculate_aggregated_lineup_stats`. |
| `regress_diffs` | `float` | `0.0` | Forwarded to get_simple_weights` -- regression toward ~1000 possessions for on/off diff calculations. Defaults to `0.0` (no regression), matching `calculate_aggregated_lineup_stats`'s call site. |

**Returns**

None. `mutable_acc` is mutated in place.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import weighted_avg, complete_weighted_avg

acc: dict = {}
for lineup in lineups:
    weighted_avg(acc, lineup)
complete_weighted_avg(acc)
print(acc["off_ppp"]["value"])  # now a true weighted average
```

### concurrent_event_handler {#concurrent_event_handler}

`concurrent_event_handler(clumps: 'Iterable[ConcurrentClump]') -> 'list[ConcurrentClump]'`

Batch a stream of singleton/boundary clumps into merged

concurrent-event clumps (`Concurrency.concurrent_event_handler` +
`StateUtils.foldLeft`'s clumping machinery, `PossessionUtils.scala
:71-111` -- see the module docstring for the full batching-predicate
breakdown and the post-game-break singleton port trap).

# ponytail: manual accumulate-and-flush loop replacing the generic
# Clumper/StateUtils.foldLeft abstraction -- this is the ONE clumper
# instantiation in the port, so a reusable abstraction buys nothing.
# Lift this back into a small clumper type if a second concurrent-event
# family needs the same batching later.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clumps` | `Iterable[ConcurrentClump]` |  | An ordered stream of `ConcurrentClump`\ s, each either a singleton raw event (`evs=[ev]`) or a lineup-boundary marker (`evs=[]`, `lineups=[lineup]`), e.g. from `lineup_as_raw_clumps`. |

**Returns**

The merged clumps, each an in-order concatenation of one batch's `evs`/`lineups`.

### count_matching {#count_matching}

`count_matching(evs: 'Iterable[RawGameEvent]', side: 'DirFn', *parsers: 'Parser') -> 'int'`

Count events on one side matching any of the given parsers.

Ports the pervasive `clump.evs.collect { case side(ParseX(_)) => () }
.size` idiom (and its multi-arm `case side(ParseX(_)) => ();
case side(ParseY(_)) => ()` union form, when more than one parser is
passed -- e.g. the and-one free-throw count, which matches *either* a
made or a missed free throw on the same event).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `evs` | `Iterable[RawGameEvent]` |  | The events to scan. |
| `side` | `DirFn` |  | `~sportsdataverse.mbb.mbb_ncaa_models.PossessionEvent .attacking_team` or `.defending_team`, selecting which raw string (if any) to test per event. |

**Returns**

The count of matching events.

### get_stats_diff {#get_stats_diff}

`get_stats_diff(stat_set1: 'LineupStatSet', stat_set2: 'LineupStatSet', off_title: 'str', def_title: 'str | None' = None) -> 'LineupStatSet'`

Straight (unweighted) field-by-field diff of two team stat sets.

Faithful port of `LineupUtils.getStatsDiff` (`LineupUtils.ts:185`).
For every field on `stat_set1`, subtracts the matching field's
`value` (and, when both sides carry one, `old_value`) from
`stat_set2`. No possession weighting or regression -- this is a raw
subtraction, unlike `weighted_avg` / `complete_weighted_avg`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_set1` | `LineupStatSet` |  | The "from" team stat set (e.g. this team). |
| `stat_set2` | `LineupStatSet` |  | The "to subtract" team stat set (e.g. the opponent, or a prior period). |
| `off_title` | `str` |  | Written into the result's `off_title` field verbatim. |
| `def_title` | `str \| None` | `None` | Written into the result's `def_title` field verbatim (`None` when omitted, mirroring the upstream optional arg). |

**Returns**

A new `LineupStatSet`: one `{"value": ..., "old_value": ..., "override": ...}` dict per field present on `stat_set1`, plus `off_title` / `def_title`. A field becomes `None` (the JS `undefined` analog) instead of a diff dict when either side is missing a `value` -- e.g. because that field was never populated for one of the two stat sets.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import get_stats_diff

diff = get_stats_diff(team_a, team_b, "Team A", "Team B")
print(diff["off_ppp"]["value"])  # team_a.off_ppp - team_b.off_ppp
```

### incorporate_height {#incorporate_height}

`incorporate_height(height_in: 'float', confs: 'list[float]') -> 'list[float]'`

Reweight positional confidences by height (Bayesian-ish height prior).

Faithful port of `PositionUtils.incorporateHeight`
(`PositionUtils.ts:346-368`; see `build_height_adj_probs` in the
linked hoop-explorer blog post). For each position `i` it computes a
height-plausibility mass `cdf(height + 1) - cdf(height - 1)` under
`N(mean_i, sqrt2 * std_i)` (the `sqrt2` "height dampening" widens the
variance so the effect is not too aggressive), multiplies it into the
prior confidence, and renormalizes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `height_in` | `float` |  | Player height in inches. |
| `confs` | `list[float]` |  | The five raw (pre-height) confidences, in `TRAD_POS_LIST` order. |

**Returns**

The five height-adjusted confidences, renormalized to sum to 1 (the `sum_product or 1` guard makes a degenerate all-zero product a no-op rather than a divide-by-zero -- see module landmine index item 1).

**Example**

```python
from sportsdataverse.mbb.mbb_positions import incorporate_height
incorporate_height(81, [0.03, 0.19, 0.49, 0.09, 0.18])
```

### inject_luck {#inject_luck}

`inject_luck(mutable_stats: 'LineupStatSet', off_luck: 'OffLuckAdjustmentDiags | None', def_luck: 'DefLuckAdjustmentDiags | None') -> 'None'`

Reversibly mutate a stat set in place with luck-adjustment deltas.

Faithful port of `LuckUtils.injectLuck` (`LuckUtils.ts:534-650`).
Works on a team, lineup, or player stat dict -- only the fields already
present on `mutable_stats` are touched (see
override_mutable_val`'s object-presence gate), so calling this
on a stat set that doesn't carry a given field (e.g. a bare
`{"key": ..., "doc_count": 0}` placeholder) is a safe no-op for that
field. Passing `off_luck=None, def_luck=None` resets every field this
function has ever touched back to its pre-luck value (see the module
docstring's landmine list for the exact mechanics, including the
absolute-vs-delta distinction on `def_3p`/`oppo_def_3p`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mutable_stats` | `LineupStatSet` |  | The stat-set dict to mutate in place. May be a team/lineup stat set (carries `off_net`/`off_raw_net`/no `oppo_total_def_3p_made`) or a player stat set (carries `oppo_total_def_3p_made`, gating the extra `oppo_def_3p` recompute -- see the module docstring's landmine #2). |
| `off_luck` | `OffLuckAdjustmentDiags \| None` |  | The output of `calc_off_team_luck_adj` / `calc_off_player_luck_adj`, or `None` to omit/reset the offensive-side fields. |
| `def_luck` | `DefLuckAdjustmentDiags \| None` |  | The output of `calc_def_team_luck_adj` / `calc_def_player_luck_adj`, or `None` to omit/reset the defensive-side fields. |

**Returns**

`None` -- this function mutates `mutable_stats` in place (TS `injectLuck` likewise returns nothing).

**Example**

```python
from sportsdataverse.mbb.mbb_luck import (
    calc_off_team_luck_adj, calc_def_team_luck_adj, inject_luck,
)

off_luck = calc_off_team_luck_adj(sample_team_on, sample_players_on, base_team, base_players_map, 100.0)
def_luck = calc_def_team_luck_adj(sample_team_off, base_team, 100.0)
inject_luck(sample_team_on, off_luck, def_luck)
print(sample_team_on["off_3p"])

# Reset back to the pre-luck values

inject_luck(sample_team_on, None, None)
```

### lineup_as_raw_clumps {#lineup_as_raw_clumps}

`lineup_as_raw_clumps(lineup: 'LineupEvent') -> 'Iterator[ConcurrentClump]'`

Turn one lineup's raw events into unprocessed singleton clumps, plus a

trailing lineup-boundary marker (`Concurrency.lineup_as_raw_clumps`,
`PossessionUtils.scala:114-120`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup` | `LineupEvent` |  | The lineup event to expand. |

**Returns**

One `ConcurrentClump([ev])` per raw event (in order), then a final `ConcurrentClump([], [lineup])` boundary marker.

### lineup_balancer {#lineup_balancer}

`lineup_balancer(lineups: 'list[LineupEvent]', team_stats: 'PossCalcFragment', opponent_stats: 'PossCalcFragment', clump: 'ConcurrentClump', prev_clump: 'ConcurrentClump') -> 'list[LineupEvent]'`

Attribute this clump's possessions to the candidate lineup(s)

(`PossessionUtils.assign_to_right_lineup.lineup_balancer`,
`PossessionUtils.scala:429-471`).

A single candidate just receives the whole clump's possessions. Multiple
candidates (a lineup change landing mid-clump) are split via a greedy
round-robin: for each direction, rank lineups by an "approximate" possession
count computed from just that lineup's own raw events at the clump's
minute, then hand out possessions one at a time to whichever lineup
currently has the highest remaining approximate share.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `list[LineupEvent]` |  | The candidate lineups (already updated with any running state total from `assign_to_right_lineup`). |
| `team_stats` | `PossCalcFragment` |  | This clump's team-direction fragment. |
| `opponent_stats` | `PossCalcFragment` |  | This clump's opponent-direction fragment. |
| `clump` | `ConcurrentClump` |  | The merged clump being assigned. |
| `prev_clump` | `ConcurrentClump` |  | The previous merged clump (only used for the first candidate's approximate stats -- see below). |

**Returns**

New lineup copies with `num_possessions` incremented.

### lineup_fixer {#lineup_fixer}

`lineup_fixer(lineups: 'list[LineupEvent]') -> 'list[LineupEvent]'`

Clamp obviously-broken possession counts (``PossessionUtils

.assign_to_right_lineup.lineup_fixer`, `PossessionUtils.scala:490-507`).

For both `team_stats` and `opponent_stats` independently: a lineup
that scored (`pts > 0``) but was attributed zero-or-fewer possessions
is clamped to exactly 1 (you can't score on zero possessions); any
still-negative possession count is clamped to 0.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `list[LineupEvent]` |  | The lineups to fix (already balanced). |

**Returns**

New lineup copies with clamped `num_possessions`.

### lineup_to_team_report {#lineup_to_team_report}

`lineup_to_team_report(lineup_report: 'LineupStatSet', inc_replacement: 'bool' = False, regress_diffs: 'float' = 0.0, rep_on_off_diag_mode: 'int' = 0) -> 'LineupStatSet'`

Build per-player on/off splits out of a team's lineups.

Faithful port of `LineupUtils.lineupToTeamReport` (`LineupUtils.ts:277`).
For every distinct player across `lineup_report["lineups"]`, partitions
the team's lineups into ON (the player was on the floor) and OFF (they
weren't) buckets, merging each bucket via `weighted_avg` /
`complete_weighted_avg`. Also builds a `teammates` map of
possession overlap with every other player, and -- when
`inc_replacement=True` -- a "replacement" on-minus-off composite via
combine_replacement_on_off`.

Lineups whose `key` is the empty string are skipped in the
on/off-partition loop (workaround for an upstream data issue, tracked
as upstream issue #53) but still contribute to the player roster.
Every lineup's `rapmRemove` key (if present, e.g. left over from a
prior `calculate_aggregated_lineup_stats` call sharing the same
input list) is deleted as a side effect while building the roster --
`lineup_to_team_report` itself never consults `rapmRemove`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_report` | `LineupStatSet` |  | `{"lineups": [...], "avgOff": ..., "error_code": ...}` -- the per-team lineup list plus metadata (mirrors upstream's `LineupStatsModel`). Only `lineups` and `error_code` are consumed here. |
| `inc_replacement` | `bool` | `False` | When `True`, additionally builds each player's `replacement` on-minus-off composite (more expensive -- scans every OFF lineup against every ON lineup for a 4-of-5-shared- players complement match). |
| `regress_diffs` | `float` | `0.0` | Forwarded to combine_replacement_on_off`'s final `complete_weighted_avg` call -- regression toward ~1000 possessions for the replacement diff (only meaningful when `inc_replacement=True`). |
| `rep_on_off_diag_mode` | `int` | `0` | When `> 0`, retains diagnostic detail (`myLineups` on each player's replacement entry, plus `lineupUsage` bookkeeping) instead of discarding it after use. |

**Returns**

`{"playerMap": {code: id}, "players": [...], "error_code": ...}`. Each entry in `players` is `{"playerId", "playerCode", "teammates", "on", "off", "replacement"}` -- `on`/`off` are finished `LineupStatSet` averages (or, for a player who's always ON, an all-zero `off`); `replacement` is `None` unless `inc_replacement=True`.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import lineup_to_team_report

report = lineup_to_team_report({"lineups": buckets, "error_code": None})
for player in report["players"]:
    print(player["playerId"], player["on"]["off_poss"]["value"])

# With replacement (on-minus-off) splits

report = lineup_to_team_report(
    {"lineups": buckets, "error_code": None},
    inc_replacement=True,
    regress_diffs=-500,
)
```

### mbb_archetypes {#mbb_archetypes}

`mbb_archetypes(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-player-season role archetype from the bundled KMeans centers.

Aggregates the season's player boxscores, builds the per-100 feature
vector (+ roster position score), standardizes with the artifact's
fit-time mean/sd, and assigns each player-season to the nearest center.
`dist_to_center` is the euclidean distance in z-space -- small = a
prototypical example of the archetype, large = a hybrid.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2025`) or list of seasons. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (player_id, season, team_id): `player_id:Utf8, player, season, team_id:Utf8, min, archetype, cluster:Int64, dist_to_center:Float64`. Empty input returns the schema with zero rows.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `player` | character | Player name. |
| `season` | integer | Season year. |
| `team_id` | character | Unique team identifier. |
| `min` | double | Minutes played. |
| `archetype` | character |  |
| `cluster` | integer |  |
| `dist_to_center` | double |  |

**Example**

```python
from sportsdataverse.mbb import mbb_archetypes
roles = mbb_archetypes(2025)

# Pipeline next step (one line)

roles.filter(pl.col("archetype") == "rim protector").sort("dist_to_center").head(10)
```

### mbb_bracketology {#mbb_bracketology}

`mbb_bracketology(season: 'int', *, as_of_date: 'datetime.date | None' = None, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Projected tournament field for a season from the released ESPN data.

Builds ratings + résumé (optionally as of a date -- games on or after
`as_of_date` are excluded), resolves conference auto-bids from the
standings, and selects/seeds the 68-team field via
`project_bracket`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season to project (e.g. `2024`). |
| `as_of_date` | `date \| None` | `None` | Only use games strictly before this date (Selection-Sunday style snapshots); `None` uses every completed game. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team -- see `project_bracket`.

No returns table is published for this function: no capture: it raises a join-key dtype mismatch on every real season (the schedule's home_team_id is Int32, the ratings' team_id is String).

**Example**

```python
from sportsdataverse.mbb import mbb_bracketology
field = mbb_bracketology(2024)

# Pipeline next step (one line)

field.filter(pl.col("bid") == True).sort("projected_seed")
```

### mbb_shot_selection {#mbb_shot_selection}

`mbb_shot_selection(scored: 'pl.DataFrame', *, group: 'str' = 'shooter_id', league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per shooter/team expected points per attempt vs the league-average mix.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored` | `DataFrame` |  | `mbb_shot_quality` output (needs `xpoints, point_value, made` + the group column). |
| `group` | `str` | `'shooter_id'` | `"shooter_id"` or `"team_id"`. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (interface parity; the math is league-free). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per group: `{group}:Utf8, n_shots:Int64, xppp, actual_ppp, selection_value, selection_value_total` (all value columns Float64). The attempt-weighted `selection_value` sums to zero across the league. Empty input returns the zero-row schema.

| col_name | type | description |
|---|---|---|
| `shooter_id` | character | Unique identifier for shooter. |
| `n_shots` | integer |  |
| `xppp` | double |  |
| `actual_ppp` | double |  |
| `selection_value` | double |  |
| `selection_value_total` | double |  |

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality, mbb_shot_selection
sel = mbb_shot_selection(mbb_shot_quality(mbb_shot_data(2025)), group="team_id")

# Pipeline next step (one line)

sel.sort("selection_value", descending=True).head(10)
```

### mbb_strength_of_schedule {#mbb_strength_of_schedule}

`mbb_strength_of_schedule(seasons: 'list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Season-level SoS / Quad / WAB résumé from the released ESPN data.

Loads the schedule + team boxscores, builds the opponent-adjusted ratings,
and applies `strength_of_schedule` per season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to compute (e.g. `[2024]`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (season, team_id) -- see `strength_of_schedule`.

No returns table is published for this function: no capture: it raises a join-key dtype mismatch on every real season (the schedule's home_team_id is Int32, the ratings' team_id is String).

**Example**

```python
from sportsdataverse.mbb import mbb_strength_of_schedule
resume = mbb_strength_of_schedule([2024])

# Pipeline next step (one line)

resume.sort("wab", descending=True).head(20)
```

### ncaa_mbb_lineups {#ncaa_mbb_lineups}

`ncaa_mbb_lineups(pbp: 'pl.DataFrame', *, include_transition: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Aggregate bigballR-contract play-by-play into per-lineup stats.

Port of bigballR `get_lineups` (`all_functions.R:1945-2521`). Rows
with any missing on-court player and substitution rows are dropped, each
row's home/away five are byte-sorted so a lineup always occupies the same
columns, and the home + away passes are combined per `(p1..p5, team)`.
Ratios are derived from the summed counters, rounded to 3 decimals, and
NA/Inf are zeroed exactly where R does it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract (`parse_ncaa_bb_game_pbp` output). May span multiple games. |
| `include_transition` | `bool` | `False` | When True, append the trans`/half` split surface plus `o_trans_pct`/`d_trans_pct` (213 columns total). |
| `fix_tip_in` | `bool` | `True` | When True (default), rim stats count the scrape engine's real `"Tip In"` vocabulary. When False, reproduce R's literal `"Tip-In"` test (`all_functions.R:2012`) — tip-ins silently excluded — for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with one row per lineup+team — 75 columns (`LINEUPS_COLUMNS`) or 213 with `include_transition=True` (`LINEUPS_TRANSITION_COLUMNS`), rows sorted by `p1..p5, team`. Empty input yields an empty frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `p1` | character |  |
| `p2` | character |  |
| `p3` | character |  |
| `p4` | character |  |
| `p5` | character |  |
| `team` | character | Team-side label or team identifier. |
| `mins` | double |  |
| `o_mins` | double |  |
| `d_mins` | double |  |
| `o_poss` | double |  |
| `d_poss` | double |  |
| `ortg` | double |  |
| `drtg` | double |  |
| `netrtg` | double |  |
| `pts` | double | Points scored. |
| `d_pts` | double |  |
| `fga` | double | Field goal attempts. |
| `d_fga` | double |  |
| `fgm` | double | Field goals made. |
| `d_fgm` | double |  |
| `tpa` | double |  |
| `d_tpa` | double |  |
| `tpm` | double |  |
| `d_tpm` | double |  |
| `fta` | double | Free throw attempts. |
| `d_fta` | double |  |
| `ftm` | double | Free throws made. |
| `d_ftm` | double |  |
| `rima` | double |  |
| `d_rima` | double |  |
| `rimm` | double |  |
| `d_rimm` | double |  |
| `orb` | double |  |
| `d_orb` | double |  |
| `drb` | double |  |
| `d_drb` | double |  |
| `blk` | double | Blocks. |
| `d_blk` | double |  |
| `to` | double | To. |
| `d_to` | double |  |
| `ast` | double | Assists. |
| `d_ast` | double |  |
| `e_poss` | double |  |
| `fg_pct` | double | Field goal percentage (0-1). |
| `d_fg_pct` | double |  |
| `tpp` | double |  |
| `d_tpp` | double |  |
| `ftp` | double |  |
| `d_ftp` | double |  |
| `efg_pct` | double |  |
| `d_efg_pct` | double |  |
| `ts_pct` | double | True shooting percentage (0-1). |
| `d_ts_pct` | double |  |
| `rim_pct` | double |  |
| `d_rim_pct` | double |  |
| `mid_pct` | double |  |
| `d_mid_pct` | double |  |
| `tp_rate` | double |  |
| `d_tp_rate` | double |  |
| `rim_rate` | double |  |
| `d_rim_rate` | double |  |
| `mid_rate` | double |  |
| `d_mid_rate` | double |  |
| `ft_rate` | double | Ft rate. |
| `d_ft_rate` | double |  |
| `ast_rate` | double |  |
| `d_ast_rate` | double |  |
| `to_rate` | double | To rate. |
| `d_to_rate` | double |  |
| `blk_rate` | double |  |
| `o_blk_rate` | double |  |
| `orb_pct` | double | Offensive rebound percentage. |
| `drb_pct` | double | Defensive rebound percentage. |
| `time_per_poss` | double |  |
| `d_time_per_poss` | double |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_lineups
lineups = ncaa_mbb_lineups(pbp)
print(lineups.shape)

# Transition/half-court splits, pandas out

df_pd = ncaa_mbb_lineups(pbp, include_transition=True, return_as_pandas=True)

# Pipeline next step (one line)

lineups.filter(pl.col("mins") > 10).sort("netrtg", descending=True).head()
```

### ncaa_mbb_on_off {#ncaa_mbb_on_off}

`ncaa_mbb_on_off(players: 'Union[str, Sequence[str]]', lineups: 'pl.DataFrame', *, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team stats for every on/off combination of the given players.

Port of bigballR `on_off_generator` (`all_functions.R:2555-2749`,
`include_transition=F` path). For k players, all `2^k` on/off
assignments are enumerated in R's `expand.grid` order (first player
varies fastest; first row all-On, last all-Off); each combination sums
the lineups whose membership matches exactly, re-derives the full ratio
block (including `e_poss`), rounds to 3 decimals, and zeroes NA/Inf.
Combinations matching zero lineups produce an all-zero row.

Faithful R quirk (kept, flagged): when `included`/`excluded` is
passed, the base lineup set comes from the membership filter ONLY — the
inferred-team filter is skipped (`all_functions.R:2584-2589`), so an
included player on another team would leak that team's lineups in.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `Union[str, Sequence[str]]` |  | Player name(s) to split on (the `Status` axis). |
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_mbb_lineups`. |
| `included` | `Union[str, Sequence[str], None]` | `None` | Optional membership filter forwarded to `ncaa_mbb_player_lineups` (replaces the team filter). |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Optional membership filter forwarded to `ncaa_mbb_player_lineups` (replaces the team filter). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with `2^k` rows — `status` (e.g. `"A.PLAYER On | B.PLAYER Off"`) + the 69 stat columns (`ON_OFF_COLUMNS`).

| col_name | type | description |
|---|---|---|
| `status` | character | Status label. |
| `mins` | double |  |
| `o_mins` | double |  |
| `d_mins` | double |  |
| `o_poss` | double |  |
| `d_poss` | double |  |
| `ortg` | double |  |
| `drtg` | double |  |
| `netrtg` | double |  |
| `pts` | double | Points scored. |
| `d_pts` | double |  |
| `fga` | double | Field goal attempts. |
| `d_fga` | double |  |
| `fgm` | double | Field goals made. |
| `d_fgm` | double |  |
| `tpa` | double |  |
| `d_tpa` | double |  |
| `tpm` | double |  |
| `d_tpm` | double |  |
| `fta` | double | Free throw attempts. |
| `d_fta` | double |  |
| `ftm` | double | Free throws made. |
| `d_ftm` | double |  |
| `rima` | double |  |
| `d_rima` | double |  |
| `rimm` | double |  |
| `d_rimm` | double |  |
| `orb` | double |  |
| `d_orb` | double |  |
| `drb` | double |  |
| `d_drb` | double |  |
| `blk` | double | Blocks. |
| `d_blk` | double |  |
| `to` | double | To. |
| `d_to` | double |  |
| `ast` | double | Assists. |
| `d_ast` | double |  |
| `e_poss` | double |  |
| `fg_pct` | double | Field goal percentage (0-1). |
| `d_fg_pct` | double |  |
| `tpp` | double |  |
| `d_tpp` | double |  |
| `ftp` | double |  |
| `d_ftp` | double |  |
| `efg_pct` | double |  |
| `d_efg_pct` | double |  |
| `ts_pct` | double | True shooting percentage (0-1). |
| `d_ts_pct` | double |  |
| `rim_pct` | double |  |
| `d_rim_pct` | double |  |
| `mid_pct` | double |  |
| `d_mid_pct` | double |  |
| `tp_rate` | double |  |
| `d_tp_rate` | double |  |
| `rim_rate` | double |  |
| `d_rim_rate` | double |  |
| `mid_rate` | double |  |
| `d_mid_rate` | double |  |
| `ft_rate` | double | Ft rate. |
| `d_ft_rate` | double |  |
| `ast_rate` | double |  |
| `d_ast_rate` | double |  |
| `to_rate` | double | To rate. |
| `d_to_rate` | double |  |
| `blk_rate` | double |  |
| `o_blk_rate` | double |  |
| `orb_pct` | double | Offensive rebound percentage. |
| `drb_pct` | double | Defensive rebound percentage. |
| `time_per_poss` | double |  |
| `d_time_per_poss` | double |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_on_off
split = ncaa_mbb_on_off("KEATON.WAGLER", lineups)
print(split.shape)

# Two-player interaction

duo = ncaa_mbb_on_off(["A.PLAYER", "B.PLAYER"], lineups)

# Pipeline next step (one line)

split.select("status", "netrtg")
```

### ncaa_mbb_player_combos {#ncaa_mbb_player_combos}

`ncaa_mbb_player_combos(lineups: 'pl.DataFrame', *, n: 'int' = 2, min_mins: 'float' = 0, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, include_transition: 'bool' = False, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team stats for every n-player combination on the court together.

Port of bigballR `get_player_combos` + `team_comb`
(`get_player_combos.R:19-38` + `:42-191`). Combos are enumerated per
team over the byte-sorted unique player pool (lexicographic
`gtools::combinations` order), filtered to combos whose lineups total
strictly more than `min_mins` minutes, then each combo's lineup rows
are summed and the ratio block re-derived. **Rounding is 2 decimals**
here (R rounds 3 in `get_lineups` / `on_off_generator`) and `e_poss`
is the SUM of the per-lineup estimates, not a recompute — both faithful.

R's `include_transition` switch is dead code (an exact-match guard at
`get_player_combos.R:28-30` always forces it back to `FALSE`); the
port implements the suffix-match intent but keeps the `False` default,
which is the only R-reachable behavior.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_mbb_lineups`. |
| `n` | `int` | `2` | Combination size, 1-5. |
| `min_mins` | `float` | `0` | Keep combos with total on-court minutes strictly greater than this (summed over the rounded per-lineup `mins`). |
| `included` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must be on the court in every lineup considered. |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must be off the court in every lineup considered. |
| `include_transition` | `bool` | `False` | Re-derive the trans`/half` ratio surface (requires a transition lineups frame; forced `False` otherwise). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with one row per combo: `team, p1..pn` + the stat surface of the input frame. Teams appear in byte-sorted order.

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `p1` | character |  |
| `p2` | character |  |
| `mins` | double |  |
| `o_mins` | double |  |
| `d_mins` | double |  |
| `o_poss` | double |  |
| `d_poss` | double |  |
| `ortg` | double |  |
| `drtg` | double |  |
| `netrtg` | double |  |
| `pts` | double | Points scored. |
| `d_pts` | double |  |
| `fga` | double | Field goal attempts. |
| `d_fga` | double |  |
| `fgm` | double | Field goals made. |
| `d_fgm` | double |  |
| `tpa` | double |  |
| `d_tpa` | double |  |
| `tpm` | double |  |
| `d_tpm` | double |  |
| `fta` | double | Free throw attempts. |
| `d_fta` | double |  |
| `ftm` | double | Free throws made. |
| `d_ftm` | double |  |
| `rima` | double |  |
| `d_rima` | double |  |
| `rimm` | double |  |
| `d_rimm` | double |  |
| `orb` | double |  |
| `d_orb` | double |  |
| `drb` | double |  |
| `d_drb` | double |  |
| `blk` | double | Blocks. |
| `d_blk` | double |  |
| `to` | double | To. |
| `d_to` | double |  |
| `ast` | double | Assists. |
| `d_ast` | double |  |
| `e_poss` | double |  |
| `fg_pct` | double | Field goal percentage (0-1). |
| `d_fg_pct` | double |  |
| `tpp` | double |  |
| `d_tpp` | double |  |
| `ftp` | double |  |
| `d_ftp` | double |  |
| `efg_pct` | double |  |
| `d_efg_pct` | double |  |
| `ts_pct` | double | True shooting percentage (0-1). |
| `d_ts_pct` | double |  |
| `rim_pct` | double |  |
| `d_rim_pct` | double |  |
| `mid_pct` | double |  |
| `d_mid_pct` | double |  |
| `tp_rate` | double |  |
| `d_tp_rate` | double |  |
| `rim_rate` | double |  |
| `d_rim_rate` | double |  |
| `mid_rate` | double |  |
| `d_mid_rate` | double |  |
| `ft_rate` | double | Ft rate. |
| `d_ft_rate` | double |  |
| `ast_rate` | double |  |
| `d_ast_rate` | double |  |
| `to_rate` | double | To rate. |
| `d_to_rate` | double |  |
| `blk_rate` | double |  |
| `o_blk_rate` | double |  |
| `orb_pct` | double | Offensive rebound percentage. |
| `drb_pct` | double | Defensive rebound percentage. |
| `time_per_poss` | double |  |
| `d_time_per_poss` | double |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_player_combos
duos = ncaa_mbb_player_combos(lineups, n=2, min_mins=5)
print(duos.shape)

# Anchored on one player

trios = ncaa_mbb_player_combos(lineups, n=3, included="KEATON.WAGLER")

# Pipeline next step (one line)

duos.sort("netrtg", descending=True).head()
```

### ncaa_mbb_player_lineups {#ncaa_mbb_player_lineups}

`ncaa_mbb_player_lineups(lineups: 'pl.DataFrame', *, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Filter a lineups frame by on-court player membership.

Port of bigballR `get_player_lineups` (`all_functions.R:2761-2792`):
keep rows where every `included` player is on the court (in `p1..p5`)
and no `excluded` player is. With both filters `None` the input is
returned unchanged (R's `Included = NA, Excluded = NA` passthrough).
Membership is tested by name against `p1..p5` (R tests the positional
first five columns); row order is preserved.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_mbb_lineups` (any frame with `p1..p5` works). |
| `included` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must ALL be on the court. |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must NONE be on the court. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Row-subset of `lineups`; schema unchanged.

| col_name | type | description |
|---|---|---|
| `p1` | character |  |
| `p2` | character |  |
| `p3` | character |  |
| `p4` | character |  |
| `p5` | character |  |
| `team` | character | Team-side label or team identifier. |
| `mins` | double |  |
| `o_mins` | double |  |
| `d_mins` | double |  |
| `o_poss` | double |  |
| `d_poss` | double |  |
| `ortg` | double |  |
| `drtg` | double |  |
| `netrtg` | double |  |
| `pts` | double | Points scored. |
| `d_pts` | double |  |
| `fga` | double | Field goal attempts. |
| `d_fga` | double |  |
| `fgm` | double | Field goals made. |
| `d_fgm` | double |  |
| `tpa` | double |  |
| `d_tpa` | double |  |
| `tpm` | double |  |
| `d_tpm` | double |  |
| `fta` | double | Free throw attempts. |
| `d_fta` | double |  |
| `ftm` | double | Free throws made. |
| `d_ftm` | double |  |
| `rima` | double |  |
| `d_rima` | double |  |
| `rimm` | double |  |
| `d_rimm` | double |  |
| `orb` | double |  |
| `d_orb` | double |  |
| `drb` | double |  |
| `d_drb` | double |  |
| `blk` | double | Blocks. |
| `d_blk` | double |  |
| `to` | double | To. |
| `d_to` | double |  |
| `ast` | double | Assists. |
| `d_ast` | double |  |
| `e_poss` | double |  |
| `fg_pct` | double | Field goal percentage (0-1). |
| `d_fg_pct` | double |  |
| `tpp` | double |  |
| `d_tpp` | double |  |
| `ftp` | double |  |
| `d_ftp` | double |  |
| `efg_pct` | double |  |
| `d_efg_pct` | double |  |
| `ts_pct` | double | True shooting percentage (0-1). |
| `d_ts_pct` | double |  |
| `rim_pct` | double |  |
| `d_rim_pct` | double |  |
| `mid_pct` | double |  |
| `d_mid_pct` | double |  |
| `tp_rate` | double |  |
| `d_tp_rate` | double |  |
| `rim_rate` | double |  |
| `d_rim_rate` | double |  |
| `mid_rate` | double |  |
| `d_mid_rate` | double |  |
| `ft_rate` | double | Ft rate. |
| `d_ft_rate` | double |  |
| `ast_rate` | double |  |
| `d_ast_rate` | double |  |
| `to_rate` | double | To rate. |
| `d_to_rate` | double |  |
| `blk_rate` | double |  |
| `o_blk_rate` | double |  |
| `orb_pct` | double | Offensive rebound percentage. |
| `drb_pct` | double | Defensive rebound percentage. |
| `time_per_poss` | double |  |
| `d_time_per_poss` | double |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_player_lineups
on = ncaa_mbb_player_lineups(lineups, included="KEATON.WAGLER")
print(on.shape)

# Included + excluded combination

df = ncaa_mbb_player_lineups(lineups, included=["A.PLAYER"], excluded=["B.PLAYER"])

# Pipeline next step (one line)

on.select(pl.col("mins").sum())
```

### order_lineup {#order_lineup}

`order_lineup(player_codes_and_ids: 'list[dict[str, str]]', players_by_id: 'dict[str, dict[str, Any]]', team_season: 'str') -> 'list[dict[str, str]]'`

Order a 5-man lineup `X1_X2_X3_X4_X5` into PG/SG/SF/PF/C slot order.

Faithful port of `PositionUtils.orderLineup` (`PositionUtils.ts:696-761`).
Greedily fits each player (in input order) to their best-scoring slot via
fit_player` (dominated by `pos_class_to_score` on the
player's `posClass`, tie-broken by their raw `posConfidences`),
evicting and recursively re-fitting any player displaced along the way,
then applies `apply_relative_positional_overrides` (keyed on
`team_season`) as a final hand-tuned correction pass.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_codes_and_ids` | `list[dict[str, str]]` |  | The lineup membership, each a `{"code": ..., "id": ...}` dict. Order does not affect the final result (the slot-fitting algorithm is order-invariant by construction -- displaced players are always re-fit). |
| `players_by_id` | `dict[str, dict[str, Any]]` |  | Per-player positional info keyed by `id`, each a `{"posConfidences": [pg, sg, sf, pf, c], "posClass": "..."}` dict (the tradPosList-ordered raw confidence scores plus the classifier's `ID_TO_POSITION`-keyed class label). |
| `team_season` | `str` |  | Key into `RELATIVE_POSITION_FIXES` for the final override pass. |

**Returns**

A 5-element list of `{"code": ..., "id": ...}` dicts in PG/SG/SF/PF/C order.

**Example**

```python
::

from sportsdataverse.mbb.mbb_positions import order_lineup
players_by_id = {
    "Cowan, Anthony": {"posConfidences": [60, 40, 10, 0, 0], "posClass": "s-PG"},
    "Ayala, Eric": {"posConfidences": [40, 60, 10, 0, 0], "posClass": "CG"},
}
order_lineup(
    [{"code": "AnCowan", "id": "Cowan, Anthony"},
     {"code": "ErAyala", "id": "Ayala, Eric"}],
    players_by_id, "",
)
```
