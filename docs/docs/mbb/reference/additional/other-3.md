---
title: "MBB — additional Python functions — Other (3)"
sidebar_label: "Other (3)"
sidebar_position: 11
description: "MBB — additional Python functions — Other (3) — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — Other (3)

### fit_shrinkage_k {#fit_shrinkage_k}

`fit_shrinkage_k(scored: 'pl.DataFrame', *, seed: 'int' = 0) -> 'float'`

Fit the talent shrinkage `k` split-half (see module docstring).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored` | `DataFrame` |  | `mbb_shot_quality` output. |
| `seed` | `int` | `0` | Split seed (deterministic fit). |

**Returns**

The `k` in `[1, 5000]` minimizing `talent_split_mse`.

**Example**

```python
from sportsdataverse.mbb.mbb_shooter_talent import fit_shrinkage_k
k = fit_shrinkage_k(scored)
```

### fix_combos {#fix_combos}

`fix_combos(first: 'str', last: 'str', code_start: 'Optional[str]' = None) -> 'list[tuple[str, Optional[str]]]'`

Pair each of `combos`' three name variants with a shared

player-code override (`DataQualityIssues.fix_combos`,
`DataQualityIssues.scala:340-346`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `first` | `str` |  | The player's first name. |
| `last` | `str` |  | The player's last name. |
| `code_start` | `Optional[str]` | `None` | The forced player-code prefix for every variant, or `None` to leave the default `build_player_code` truncation behavior in place. |

**Returns**

Three `(name_variant, code_start)` pairs.

### fix_possible_score_swap_bug {#fix_possible_score_swap_bug}

`fix_possible_score_swap_bug(lineup: 'list[LineupEvent]', box_lineup: 'LineupEvent') -> 'list[LineupEvent]'`

Undo a rare NCAA data bug where the scores get transposed

(`fix_possible_score_swap_bug`, `LineupUtils.scala:51-90`).

If the last lineup's ending score is the exact transpose of the box
score's ending score, every lineup's `score_info` is un-transposed and
`pts`/`plus_minus` are swapped/negated between `team_stats` and
`opponent_stats` -- nothing else in the stat trees changes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup` | `list[LineupEvent]` |  | The lineups to (maybe) fix, in chronological order. |
| `box_lineup` | `LineupEvent` |  | The trusted box-score lineup to compare the final score against. |

**Returns**

`lineup` unchanged if the scores aren't transposed (or `lineup` is empty); otherwise a new list with every entry's score/pts/ plus_minus corrected.

### fuzzy_box_match {#fuzzy_box_match}

`fuzzy_box_match(candidate: 'str', unassigned_box_names: 'list[str]', team_context: 'str') -> 'Union[str, FuzzyMatchError]'`

Pick the single unassigned box-score name a mis-spelled play-by-play

name most likely refers to (`NameFixer.fuzzy_box_match`, `:774-905`).

Resolution order: a single strong match wins outright; multiple strong
matches only resolve if there's a clear (>10-point) winner; failing
that, a single weak match wins; failing that, a first-name-only match
only wins if there are no other first-name matches (exact or fuzzy)
among the un-matched box names.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `candidate` | `str` |  | The raw play-by-play name. |
| `unassigned_box_names` | `list[str]` |  | Box-score full names not yet claimed by another resolution. |
| `team_context` | `str` |  | Debug-only context string (Scala used it to de-duplicate diagnostic prints; this port has no logging surface to de-duplicate, so the value is otherwise unused). |

**Returns**

The winning box-score name, or a `FuzzyMatchError` describing why no single name won.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import fuzzy_box_match
fuzzy_box_match(
    "sirena tuitele",
    ["Suitele, Sirena", "Tuitele, Peanut", "Guity, Amaya"],
    "team_context",
)
# "Suitele, Sirena"
```

### handle_common_sub_bug {#handle_common_sub_bug}

`handle_common_sub_bug(clump: 'BadLineupClump', box_lineup: 'LineupEvent', valid_player_codes: 'set[str]') -> 'tuple[list[LineupEvent], BadLineupClump]'`

Fixes the "2-in-1-out then a compensating 1-out" substitution bug

(`LineupErrorAnalysisUtils.handle_common_sub_bug`, `:269-298`).

Handles a **single-event** bad clump whose next known-good lineup carries
a lone sub-out that the clump's event should have applied but didn't (e.g.
`IN: X, Y, Z; OUT: A, B` in the bad event, then `OUT: C` in the good
one). Fires only when all three guard conditions hold
(`:275-278`):

* the bad event has more players subbing IN than OUT
  (`len(players_in) > len(players_out)`),
* the good event has **no** sub-ins (`len(good.players_in) == 0` --
  otherwise there's no way to tell which of its sub-outs to borrow), and
* the good event has at least one sub-out (`len(good.players_out) > 0`).

The fix (`:279-283`) removes the good event's sub-outs from the bad
event's on-floor `players` (value-equality `not in` -- the Scala's
`filterNot(good.players_out.toSet)`) and appends them to the bad
event's `players_out` (order-preserving distinct` -- the Scala's
`(bad.players_out ++ good.players_out).distinct`). The fix is **accepted
only if** the result then passes `validate_lineup` (`:284-294`):
on success the fixed event is returned as the sole `fixed` lineup and
the still-to-fix clump is emptied; on failure the *fixed* event (not the
original) is returned as the still-to-fix clump, keeping the same
`next_good` so a later pass can try again.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `BadLineupClump` |  | The bad-lineup clump to attempt to repair. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (roster + name context). |
| `valid_player_codes` | `set[str]` |  | Every player code on the box score / roster. |

**Returns**

`(fixed_lineups, still_to_fix)` -- `fixed_lineups` is `[fixed]` on an accepted fix else `[]`; `still_to_fix` is an empty clump on accept, the (unchanged) input clump on a guard miss, or the single-event *fixed*-but-still-invalid clump on a rejected fix.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
    handle_common_sub_bug,
)
fixed, still = handle_common_sub_bug(clump, box_lineup, valid_codes)
```

### has_kenpom_login {#has_kenpom_login}

`has_kenpom_login() -> 'bool'`

Whether KenPom credentials are set in the environment.

The Python counterpart of hoopR's `has_kp_user_and_pw()`; gates a live
test without attempting a login.

**Returns**

`True` when both an e-mail and a password resolve from the environment.

**Example**

```python
import pytest
from sportsdataverse.mbb import has_kenpom_login

pytestmark = pytest.mark.skipif(not has_kenpom_login(), reason="no KenPom login")
```

### in_game_features {#in_game_features}

`in_game_features(pbp: 'pl.DataFrame', pregame_home_prob: 'float') -> 'pl.DataFrame'`

Per-play in-game win-probability features from a `load_mbb_pbp` frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame with `start_game_seconds_remaining`, `home_score`, `away_score`, `team_id` (event team) and `home_team_id` (the `load_mbb_pbp` schema). |
| `pregame_home_prob` | `float` |  | The pregame home win probability (e.g. from `win_prob_from_margin`), encoded as a constant logit column. Clipped to `[1e-6, 1 - 1e-6]` so a saturated CDF (exact 0/1) cannot crash the logit. |

**Returns**

One row per input play: `score_diff` (home - away), `sec_left` (clipped at 0 -- overtime plays count as 0 seconds left), `sqrt_sec_left`, `pregame_logit`, `home_has_ball` (`Int8`; dead-ball / unknown-team plays are 0).

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import in_game_features
from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
pbp = load_mbb_pbp([2024]).filter(pl.col("game_id") == 401638643)
feats = in_game_features(pbp, 0.62)
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

### inject_rapm_into_players {#inject_rapm_into_players}

`inject_rapm_into_players(players: 'list[PlayerOnOffStats]', off_rapm_input: 'RapmProcessingInputs', def_rapm_input: 'RapmProcessingInputs', stats_averages: 'PureStatSet', ctx: 'RapmPlayerContext', adaptive_correl_weights: 'list[float] | None', read_value_keys: 'tuple[ValueKey, ValueKey]' = ('value', 'value'), write_value_key: 'ValueKey' = 'value') -> 'None'`

Write `pick_ridge_regression`'s RAPM predictions back onto each player.

Faithful port of `RapmUtils.injectRapmIntoPlayers` (`RapmUtils.ts:781-916`).
For every `onOffReportReplacement` field (minus the possession/title/
separator/`adj_opp` housekeeping keys -- see landmine 11 for the exact,
faithfully-ported omit-key quirk), re-derives that field's off/def target
vectors via `calc_lineup_outputs`, applies each side's
`calculate_rapm` solver, blends in the strong prior (mirroring
`pick_ridge_regression`'s own blend, except for `adj_ppp` which
reuses `off_rapm_input["rapm_adj_ppp"]`/`def_rapm_input["rapm_adj_ppp"]`
directly rather than recomputing), then writes `{playerId}.rapm[field]
= {write_value_key: result, "override": ...}` onto every player not in
`ctx["removed_players"]`.

**NOTE (upstream comment, verbatim): when `write_value_key ==
"old_value"`, this must be called *after* an initial `write_value_key
== "value"` call on the same `players` list** -- the `old_value`
pass .merge`s (lodash_merge`) its results into each player's
*existing* `rapm` dict rather than replacing it, so a player's
`rapm["field"]` ends up carrying both a `value` (from the first
call) and an `old_value` (from the second) side by side.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `list[PlayerOnOffStats]` |  | The players to write RAPM results onto (mutated in place -- each qualifying player gets a `"rapm"` key set/merged). |
| `off_rapm_input` | `RapmProcessingInputs` |  | `pick_ridge_regression`'s offensive output. |
| `def_rapm_input` | `RapmProcessingInputs` |  | `pick_ridge_regression`'s defensive output. |
| `stats_averages` | `PureStatSet` |  | League/context average stat set -- consulted for each field's off/def offset before `ctx["team_info"]`. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` (the same one `pick_ridge_regression` was called with). |
| `adaptive_correl_weights` | `list[float] \| None` |  | Optional per-player adaptive-correlation weights, forwarded to `calc_lineup_outputs` / get_strong_weight` exactly as `pick_ridge_regression` does. |
| `read_value_keys` | `tuple[ValueKey, ValueKey]` | `('value', 'value')` | `(off_key, def_key)` -- which key (`"value"`/`"old_value"`) to prefer when reading `stats_averages`/`ctx["team_info"]` offsets and when calling `calc_lineup_outputs` (forwarded as its `use_old_val_if_possible` flag). |
| `write_value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- which key each written field carries its result under. |

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import inject_rapm_into_players

inject_rapm_into_players(players, off_results, def_results, {}, ctx, None)
print(players[0]["rapm"]["off_adj_ppp"])  # {"value": ..., "override": None}

# Luck-adjusted two-call sequence (``"value"`` first, THEN ``"old_value"``)

inject_rapm_into_players(
    players, off_results, def_results, {}, ctx, None, ("value", "old_value"), "value"
)
inject_rapm_into_players(
    players, off_results, def_results, {}, ctx, None, ("old_value", "old_value"), "old_value"
)
```

### inject_starting_lineup_into_box {#inject_starting_lineup_into_box}

`inject_starting_lineup_into_box(sorted_pbp_events: 'list[PlayByPlayEvent]', box_lineup: 'LineupEvent', external_roster: 'tuple[list[str], list[RosterEntry]]', format_version: 'int') -> 'LineupEvent'`

Infer the starting five and reorder the box-score roster so they lead

(`PlayByPlayUtils.inject_starting_lineup_into_box`,
`PlayByPlayUtils.scala:684-845`).

The v1 (2018+) NCAA box score dropped the ordered list of starters, so we
reconstruct it from the play-by-play sub sequencing. A player is a starter
if, walking the events forward, they are seen *before* their first sub-in
-- either subbed *out* (before ever being subbed in), or *named in a
team-side play* that isn't concurrent with a sub. Anyone subbed *in* before
ever being seen is excluded. The reconstruction stops once five starters
are found.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_pbp_events` | `list[PlayByPlayEvent]` |  | The full play-by-play event stream, ascending time. |
| `box_lineup` | `LineupEvent` |  | The box-score lineup event (its `players` is the full roster to reorder). |
| `external_roster` | `tuple[list[str], list[RosterEntry]]` |  | Unused here -- carried for signature parity with the Scala (its pipeline caller passes it). See the module note. |
| `format_version` | `int` |  | Unused here -- carried for signature parity. See the module note. |

**Returns**

A copy of `box_lineup` with `players` reordered so the inferred starters lead. If fewer than five starters could be inferred (a "40-trillion" player who was never subbed nor mentioned), the roster is ordered starters -> possible-starters -> definitely-not-starters as the best available guess.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import inject_starting_lineup_into_box
fixed = inject_starting_lineup_into_box(pbp_events, box_lineup, ([], []), 1)
```

### inject_validated_players {#inject_validated_players}

`inject_validated_players(ordered_lineup_from_box: 'list[str]', box_minus_players: 'LineupEvent', external_roster: 'tuple[list[str], list[RosterEntry]]') -> 'list[str]'`

Validates box players against the roster (if available) and any

other available box scores (`BoxscoreParser.inject_validated_players`,
`:233-279`).

See the module docstring's "un-threaded `tidy_ctx`" note -- every
fuzzy-resolution call inside the loop uses the SAME original context,
never the updated one a call returns (ported verbatim, including this
apparent Scala oversight).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ordered_lineup_from_box` | `list[str]` |  | The raw player-name strings scraped straight off the box-score page (already v0-normalized if the source was v1, by `get_box_lineup`'s caller). |
| `box_minus_players` | `LineupEvent` |  | The in-progress `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` (used only for its `team` field, both to scope the fuzzy-match context and to key `~sportsdataverse.mbb.mbb_ncaa_data_quality.players_missing_from_boxscore`). |
| `external_roster` | `tuple[list[str], list[RosterEntry]]` |  | `(other_players, roster_players)` -- extra known player names, and a full team roster (if available) to validate against / fuzzy-correct box names onto. |

**Returns**

`ordered_lineup_from_box` with any name not found in `roster_players` fuzzy-corrected onto the closest roster name (if a roster was supplied at all), followed by any roster/other/ known-missing players not already present in that corrected list (see the module docstring's "Extra-players Set ordering" note for this trailing group's order).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import inject_validated_players
inject_validated_players(["Player One"], box_lineup, ([], []))
```

### is_cached {#is_cached}

`is_cached(path: 'str', *, cache_dir: 'Optional[Path]' = None) -> 'bool'`

Return whether *path* already has a cache file on disk.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  |  |
| `cache_dir` | `Optional[Path]` | `None` |  |

### is_end_of_game_fouling_vs_fastbreak {#is_end_of_game_fouling_vs_fastbreak}

`is_end_of_game_fouling_vs_fastbreak(curr_clump: 'ConcurrentClump', event_parser: 'PossessionEvent') -> 'bool'`

Check for intentional fouling to prolong the game, specifically so it

can be excluded from being counted as a fast break
(`is_end_of_game_fouling_vs_fastbreak`, `LineupUtils.scala:603-656`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `curr_clump` | `ConcurrentClump` |  | The clump to classify. |
| `event_parser` | `PossessionEvent` |  | Selects which side of each event is "attacking". |

**Returns**

`True` iff the FIRST attacking-side FT-made/FT-missed event in `curr_clump.evs` is both near the end of a period AND has the attacking team ahead by `(0, 10]` points; `False` if no such event exists.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import is_end_of_game_fouling_vs_fastbreak

is_end_of_game_fouling_vs_fastbreak(curr_clump, event_parser)
```

### is_gen2 {#is_gen2}

`is_gen2(ev: 'RawGameEvent') -> 'bool'`

Detect the new/"gen2" NCAA event format (`EventUtils.is_gen2`,

`EventUtils.scala:12-14`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ev` | `RawGameEvent` |  | The raw game event to inspect. |

**Returns**

`True` if `ev.info` contains a comma-space (`", "`), the gen2 format's field separator; `False` for the old/legacy format.

### is_scramble {#is_scramble}

`is_scramble(curr_clump: 'ConcurrentClump', prev_clumps: 'list[ConcurrentClump]', event_parser: 'PossessionEvent', player_version: 'bool') -> 'tuple[Callable[[RawGameEvent], bool], str]'`

Figure out if (each event of) the current clump is part of a

"scramble scenario" following an ORB (`is_scramble`, `LineupUtils
.scala:222-597`).

Returns a `(predicate, debug_tag)` tuple -- **the tuple shape is
load-bearing**: the oracle asserts the debug tag string directly
(`"N/A"`/`"0a"`/`"1aa"`/`"1ab"`/`"1b"`/`"2aa"`/`"2ab"`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `curr_clump` | `ConcurrentClump` |  | The clump to classify. |
| `prev_clumps` | `list[ConcurrentClump]` |  | Prior merged clumps, most-recent-first. |
| `event_parser` | `PossessionEvent` |  | Selects which side of each event is "attacking". |
| `player_version` | `bool` |  | Unused -- see the module docstring's `is_scramble` port notes (the Scala's debug-print gate this flag controls is permanently `false` regardless of its value). |

**Returns**

`(predicate, debug_tag)` where `predicate(ev)` reports whether `ev` is part of a scramble.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import is_scramble

predicate, tag = is_scramble(curr_clump, prev_clumps, event_parser, player_version=False)
[predicate(ev) for ev in curr_clump.evs]
```

### is_team_shooting_left_to_start {#is_team_shooting_left_to_start}

`is_team_shooting_left_to_start(sorted_very_raw_events: 'list[tuple[int, ShotEvent]]') -> 'tuple[bool, int]'`

Infers which side of the SVG court the team under analysis shoots

towards in the first period, from its own made/missed shot locations
(`ShotEventParser.is_team_shooting_left_to_start`, `:540-555`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_very_raw_events` | `list[tuple[int, ShotEvent]]` |  | The chronologically-sorted (period, shot) pairs, pre-geometry-transform. |

**Returns**

`(team_shooting_left_in_first_period, first_period)` -- the first element of `sorted_very_raw_events`, if any, determines `first_period`; the majority side (by count) of the team's own (`is_off`) shots within that period determines the direction.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import is_team_shooting_left_to_start
is_team_shooting_left_to_start([(1, shot_a), (1, shot_b)])
```

### is_women_game {#is_women_game}

`is_women_game(sorted_very_raw_events: 'list[tuple[int, ShotEvent]]') -> 'bool'`

Infers men's vs. women's game from timing evidence

(`ShotEventParser.is_women_game`, `:558-566`). **Shot-parser-specific
variant** -- distinct from the play-by-play parser's own
`is_women_game` (Task 5e.3), which uses PbP event timing instead of
shot timing; the plan's recon flags both as "its OWN is_women_game
variant" per module.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_very_raw_events` | `list[tuple[int, ShotEvent]]` |  | The chronologically-sorted (period, shot) pairs. |

**Returns**

`True` if at least 4 periods were seen AND no shot was taken with more than 10 minutes showing on the (descending) clock in the very first event (women's quarters are 10 minutes; a shot at >10:00 remaining could only happen in a longer men's period).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import is_women_game
is_women_game([(1, shot), (2, shot), (3, shot), (4, shot)])  # True
```

### jsoup_text {#jsoup_text}

`jsoup_text(el: 'Optional[Tag]') -> 'str'`

JSoup `Element.text()`: all descendant text, whitespace-collapsed.

JSoup's `.text()` joins every text node under `el` (including
descendants) and collapses runs of whitespace (spaces, tabs, newlines)
into single spaces, trimming the ends. bs4's `.get_text()` does the
joining but not the collapsing, so captured HTML's indentation/newlines
would otherwise leak into every extracted value.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `el` | `Optional[Tag]` |  | The element to extract text from, or `None`. |

**Returns**

The whitespace-collapsed text, or `""` if `el` is `None`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import jsoup_text, parse_html
soup = parse_html("<td>\n  Akin,\tDaniel  </td>")
jsoup_text(soup.find("td"))  # "Akin, Daniel"
```

### kenpom_login {#kenpom_login}

`kenpom_login(email: 'Optional[str]' = None, password: 'Optional[str]' = None, *, proxy: 'Any' = None) -> 'requests.Session'`

Log into kenpom.com and return the authenticated session.

The Python counterpart of hoopR's `login()`. Calling this directly is
optional -- every wrapper logs in on demand and reuses a cached session --
but it is the fastest way to verify credentials or a proxy before a long
pull, and the returned session can be passed to a wrapper as `session=`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `email` | `Optional[str]` | `None` | KenPom account e-mail. Falls back to `KENPOM_EMAIL` / `KP_USER` / `SDV_PY_KENPOM_EMAIL`. |
| `password` | `Optional[str]` | `None` | KenPom password. Falls back to `KENPOM_PW` / `KP_PW` / `SDV_PY_KENPOM_PW`. |
| `proxy` | `Any` | `None` | Proxy URL `str` or `requests` `proxies=` `dict`. Falls back to `SDV_PY_KENPOM_PROXY` then `SDV_PY_PROXY`. |

**Returns**

An authenticated `requests.Session` carrying the subscription cookie and the resolved proxy.

**Example**

```python
from sportsdataverse.mbb import kenpom_login

session = kenpom_login(proxy="http://user:pw@proxy.example:8080")
```

### kmeans_fit {#kmeans_fit}

`kmeans_fit(X: 'np.ndarray', k: 'int', seed: 'int', n_init: 'int' = 10, max_iter: 'int' = 100) -> "'tuple[np.ndarray, np.ndarray]'"`

Seeded Lloyd's KMeans, best-of-`n_init` by inertia.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix `(n, d)` (standardize first). |
| `k` | `int` |  | Number of clusters. |
| `seed` | `int` |  | RNG seed (deterministic output). |
| `n_init` | `int` | `10` | Independent restarts. |
| `max_iter` | `int` | `100` | Lloyd iterations per restart. |

**Returns**

`(centers[k, d], labels[n])`.

**Example**

```python
centers, labels = kmeans_fit(Z, k=8, seed=0)
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

### lineup_stats_bucket {#lineup_stats_bucket}

`lineup_stats_bucket(ev: 'LineupEvent', *, avg_eff: 'float' = 100.0, opponent_baselines: 'Optional[dict[str, float]]' = None, doc_count: 'int' = 1) -> 'LineupStatSet'`

Assemble one lineup's full 254-field `{value}` bucket.

`lineup_stats_bucket` is the Python entry point for stage 2 of the port (see the
module docstring) -- the faithful composition of this module's factories in the order
`commonLineupAggregations.ts` (572-line ES aggregation) issues them: `sum` (
sum_fields`) -> merge the play-type `pts`/`poss` bucket_script (
play_type_pts_poss`) -> mint every other rate bucket_script (
all_rate_fields`) -> the SOS-adjusted-efficiency bucket_script (
adj_fields`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ev` | `LineupEvent` |  | One already-summed lineup event (`team_stats`/`opponent_stats` populated by stage 1, `~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.enrich_lineup`). |
| `avg_eff` | `float` | `100.0` | League-average efficiency passed through to adj_fields`. |
| `opponent_baselines` | `Optional[dict[str, float]]` | `None` | SOS baseline lookup passed through to adj_fields`; only `None` (no baselines) is implemented. |
| `doc_count` | `int` | `1` | The ES `doc_count` for this bucket (number of raw events folded in). |

**Returns**

The full bucket: every `total_*`/rate/adj field wrapped in `{"value": <float>}`, plus the structural keys `key`, `players_array`, `doc_count` (bare, unwrapped).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_aggregation import lineup_stats_bucket

bucket = lineup_stats_bucket(enriched_event, doc_count=7)
bucket["off_ppp"]["value"]
```

### lineup_stats_buckets {#lineup_stats_buckets}

`lineup_stats_buckets(evs: 'list[LineupEvent]', *, avg_eff: 'float' = 100.0, opponent_baselines: 'Optional[dict[str, float]]' = None) -> 'list[LineupStatSet]'`

Group events by lineup, fold each group's stats, and mint one bucket per lineup.

Python entry point for the ES `terms` aggregation over `key` (grouping by
bucket_key`) that feeds each lineup's docs into
`commonLineupAggregations.ts`'s `sum` aggs -- see
`cbb-on-off-analyzer/src/utils/es-queries/commonLineupAggregations.ts`. This
is the list-form producer the `LineupStatSet` consumers (`mbb_lineup_stats`)
read from; `lineup_stats_bucket` handles a single already-folded event.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `evs` | `list[LineupEvent]` |  | Raw per-possession-chunk lineup events (`team_stats`/`opponent_stats` populated by stage 1, `~sportsdataverse.mbb.mbb_ncaa_lineup_enrich .enrich_lineup`), one lineup's floor time possibly split across many events. |
| `avg_eff` | `float` | `100.0` | League-average efficiency passed through to each bucket. |
| `opponent_baselines` | `Optional[dict[str, float]]` | `None` | SOS baseline lookup passed through to each bucket; only `None` (no baselines) is implemented (see adj_fields`). |

**Returns**

One `LineupStatSet` per distinct lineup (bucket_key`), in first-seen order, with `doc_count` set to that lineup's event count.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_aggregation import lineup_stats_buckets

buckets = lineup_stats_buckets(enriched_events)
buckets[0]["off_poss"]["value"]
```

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

### log_loss_score {#log_loss_score}

`log_loss_score(y_true: 'np.ndarray', p_pred: 'np.ndarray', eps: 'float' = 1e-15) -> 'float'`

Binary cross-entropy loss between predicted probabilities and outcomes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |
| `eps` | `float` | `1e-15` | Clipping bound to avoid `log(0)`. |

**Returns**

The mean log loss.

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import log_loss_score
log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
```

### logistic_fit {#logistic_fit}

`logistic_fit(X: 'np.ndarray', y: 'np.ndarray', lam: 'float' = 1.0) -> 'np.ndarray'`

L2-penalized logistic regression via L-BFGS (intercept unpenalized).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix `(n, d)`. |
| `y` | `ndarray` |  | Binary outcomes (0/1). |
| `lam` | `float` | `1.0` | L2 penalty on the non-intercept coefficients. |

**Returns**

Coefficient vector of length `d + 1` (intercept first).

**Example**

```python
coef = logistic_fit(X, drafted, lam=1.0)
```

### mae {#mae}

`mae(a: 'np.ndarray', b: 'np.ndarray') -> 'float'`

Mean absolute error between two arrays.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `ndarray` |  | First array of values. |
| `b` | `ndarray` |  | Second array of values (same length as `a`). |

**Returns**

The mean absolute error.

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import mae
mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
```

### matching_player {#matching_player}

`matching_player(shot: 'ShotEvent', pbp_event: 'MiscGameEvent', tidy_ctx: 'TidyPlayerContext', code_match: 'bool') -> 'bool'`

Whether the player in `pbp_event` matches `shot`'s shooter

(`ShotEnrichmentUtils.matching_player`, `PlayByPlayUtils.scala:638-652`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot` | `ShotEvent` |  | The shot being enriched. |
| `pbp_event` | `MiscGameEvent` |  | The candidate play-by-play event. |
| `tidy_ctx` | `TidyPlayerContext` |  | The name-resolution context. |
| `code_match` | `bool` |  | If `True`, compare on player *code* only (looser -- lets a name that resolves to the wrong identity but the right code match); if `False`, require full `~sportsdataverse.mbb .mbb_ncaa_models.PlayerCodeId` equality. |

**Returns**

`True` if the resolved player matches `shot.player` under the selected comparison, else `False`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import matching_player
matching_player(shot, pbp_event, tidy_ctx, code_match=False)
```

### misspellings {#misspellings}

`misspellings(team: 'Optional[TeamId]') -> 'dict[str, str]'`

Team-scoped misspelling map, falling back to the generic map

(`DataQualityIssues.misspellings`, `DataQualityIssues.scala:165-322`
-- see the module docstring's "fallback semantics" note for why this is
a precomputed merge, not a runtime two-level lookup).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `Optional[TeamId]` |  | The team to look up team-specific corrections for. `None` (like any team absent from the table) falls back to `generic_misspellings`. |

**Returns**

A fresh dict -- the team's misspelling map merged with `generic_misspellings`, or a copy of `generic_misspellings` if `team` has no specific entries.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_data_quality import misspellings
from sportsdataverse.mbb.mbb_ncaa_models import TeamId

misspellings(TeamId("NJIT"))["Lewal, Levi"]  # 'Lawal, Levi'
misspellings(TeamId("Some Unlisted Team"))  # {} (generic fallback)
misspellings(None)  # {} (generic fallback)
```

### name_in_v0_box_format {#name_in_v0_box_format}

`name_in_v0_box_format(v1_name: 'str') -> 'str'`

Switch a v1-box-format name (`"first_name names"`) to v0-box format

(`"names, first_name"`) (`ExtractorUtils.scala:59-81`).

Handles a v0-PbP-style all-caps input (`"SURNAME,NAME"`, still seen in
older files even in v1-format seasons) by first flipping it to guaranteed
v1 shape, then splits on the first space to get `first`/`last`. A
`last` starting with `"("` is treated as a nickname parenthetical
(e.g. `"Russell (Deuce) Dean"`) and re-split via
COMPLEX_V0_CASE_RE` -- **ported verbatim including its literal
quirk**: the regex's second capture group keeps the leading space before
the trailing surname (e.g. yields `" Dean, Russell (Deuce)"`, not
`"Dean, Russell Deuce"` as the Scala source comment's stated *intent*
describes) and the parenthesis characters are not stripped. This is
upstream behavior, not a Python-side bug -- the Scala's own pattern-match
reproduces exactly this, so faithful porting keeps it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `v1_name` | `str` |  | The player name as it appears in a v1 (2018+) NCAA roster or box-score row. |

**Returns**

The name in v0 (`"names, first_name"`) format, or `v1_name` (via the guaranteed-v1-format intermediate) unchanged if it has no space to split on.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import name_in_v0_box_format
name_in_v0_box_format("Daniel Akin")  # "Akin, Daniel"
name_in_v0_box_format("AKIN,DANIEL")  # "AKIN, DANIEL" (old PbP form, flipped then re-split)
```

### name_is_initials {#name_is_initials}

`name_is_initials(name: 'str') -> 'Optional[tuple[str, str]]'`

Detect a 2-initial name shorthand, e.g. `"A B"` or `"B, A"`

(`ExtractorUtils.name_is_initials`, `ExtractorUtils.scala:94-102`).
Ported here (rather than into `mbb_ncaa_stints.py`) since
`convert_from_initials` was this module's original consumer;
promoted from private to public in Task 5e.1 for
`mbb_ncaa_roster_parser.py`'s `parse_roster` (a second consumer,
which only needs `.nonEmpty` -- whether a match exists at all -- to
reject initials-only roster rows).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The candidate initials string. |

**Returns**

`(p1, p2)` -- `p1` is the leading initial in a `"A B"`-style string, or the trailing initial in a `"B, A"`-style string; `None` if `name` doesn't fit either 3- or 4-character shape.

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

### phase1_shot_event_enrichment {#phase1_shot_event_enrichment}

`phase1_shot_event_enrichment(sorted_very_raw_events: 'list[tuple[int, ShotEvent]]', second_half_override: 'Optional[set[int]]' = None) -> 'list[ShotEvent]'`

The court-geometry enrichment pass: ascending time, coordinate

transform + geo synthesis, and the self-correcting side-flip re-run
(`ShotEventParser.phase1_shot_event_enrichment`, `:415-528`).

For each shot: compute the ascending game time, decide (from
`is_team_shooting_left_to_start` + which half the period falls in)
whether the shot's side needs flipping, run `transform_shot_location`
to get both the believed-correct and alternative (mirrored) locations,
keep whichever is closer to the basket (a >1.2x distance advantage for
the "alternative" wins, or ANY shot taken with <0.1 min left on the
clock always keeps the original -- a half-court heave near the buzzer
is plausible, so the tie-break favors trusting the raw geometry there),
then synthesize a lat/lon.

After all shots are processed, if any period had >=6 shots AND more than
75% of them came back implausibly long-distance (>50ft), the whole pass
re-runs ONCE with those periods' orientation flipped (the self-correcting
part) -- `second_half_override` is `None` on the initial call and a
non-`None` set on the one allowed retry, preventing infinite recursion.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_very_raw_events` | `list[tuple[int, ShotEvent]]` |  | The chronologically-sorted (period, shot) pairs from `parse_shot_html`, pre-geometry-transform. |
| `second_half_override` | `Optional[set[int]]` | `None` | The set of periods whose `second_half_switch` orientation should be inverted (the self-correction re-run's input); `None` on the first call. |

**Returns**

The fully court-geometry-enriched shots, in the same order as `sorted_very_raw_events`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import phase1_shot_event_enrichment
shots = phase1_shot_event_enrichment([(1, very_raw_shot)])
```

### pick_ridge_regression {#pick_ridge_regression}

`pick_ridge_regression(off_weights: 'NDArray[np.float64]', def_weights: 'NDArray[np.float64]', ctx: 'RapmPlayerContext', adaptive_correl_weights: 'list[float] | None', diag_mode: 'bool', agg_value_key: 'ValueKey' = 'value', lineup_value_keys: 'tuple[ValueKey, ValueKey]' = ('value', 'value')) -> 'tuple[RapmProcessingInputs, RapmProcessingInputs]'`

Adaptively pick a ridge-regression lambda and blend in the RAPM priors.

Faithful port of `RapmUtils.pickRidgeRegression` (`RapmUtils.ts:1001-1540`)
-- the top-level driver that, per off/def side: scales a dimensionless
`lambda_range` by the design matrix's mean singular value
(`avg_eigen_val`) into an actual ridge strength, solves via
`slow_regression`/`calculate_rapm`, blends in each player's
strong prior (get_strong_weight`), reconciles the possession
-weighted team total against the actual team efficiency
(`[IMPORTANT-EQUATION-01]`, see below), nudges the result back towards
the weak priors on any remaining error (`apply_weak_priors`), and
decides whether to keep sweeping `lambda` upward, roll back to the
previous step, or stop.

**`[IMPORTANT-EQUATION-01]`** (`RapmUtils.ts:1306-1314`/`:1325-1333`):
`combined_adj_eff = sum(pct_by_player[i] * rapm[i] for i) +
add_low_volume_adj_rtg`, compared against `actual_eff[off_or_def]`
(the team's actual, prior-basis-adjusted efficiency, including
bench/removed-player possessions) to derive `adj_eff_err` -- the error
signal both the weak-prior nudge and the stopping rule react to.

**Stopping rule** (checked once per `lambda` step, in order): (1) once a
*second* step has run (`not_first_step`) and, unless in `diag_mode`,
the current step is past `lambda_range_to_use[3]`, roll back to the
*previous* step's `soln_matrix`/`ridge_lambda` (but **not**
`rapm_adj_ppp`/`rapm_raw_adj_ppp`/`sd_rapm`, which stay at the
current, over-threshold step's values -- a faithful, non-obvious TS
asymmetry, `RapmUtils.ts:1443-1448` vs `:1483-1484`) when
`adj_eff_err >= error_exit_thresh` (`1.35` for the low-possession
-count offense special case, else `1.05`) **and** the error is still
increasing (`>= last_error`); else (2) stop in place once
`mean_diff` (the mean per-player RAPM change since the previous step)
drops below `pick_ridge_thresh` (`0.061` off / `0.091` def --
"more confident in offensive priors"); else (3) keep sweeping.

**Adaptive-weight / prior asymmetry** (the deep-equality oracle's load
-bearing behavior): the per-player strong-prior blend
(get_strong_weight(ctx["prior_info"], adaptive_correl_weights[i])`)
only consults `adaptive_correl_weights` when
`ctx["prior_info"]["strong_weight"] < 0` (adaptive mode) -- a fixed,
non-negative `strong_weight` always wins. A fixture whose
`players_strong` entries carry no `def_adj_ppp` key makes the blend's
`stat.get(f"{off_or_def}_adj_ppp") or 0.0` term (and, transitively,
`calc_lineup_outputs`'s own `strong_val` term) contribute exactly
`0` on the def side regardless of `strong_weight` or
`adaptive_correl_weights` -- see the oracle test's `def_results1`/
`def_results2` invariance assertions.

**`svd` is `numpy.linalg.svd(..., compute_uv=False)`, singular values
only.** Upstream's `SVD(weights[side].valueOf())` (`svd-js`) also
computes `u`/`v`, but only `svd.q` (the singular values, via
`mean(svd.off.q)`/`mean(svd.def.q)` at `avg_eigen_val`,
`RapmUtils.ts:1077`) is ever read -- `u`/`v` are dead. Skipping them
is an efficiency-only deviation with an identical result (singular
values are unique to a matrix regardless of the underlying SVD
implementation).

**Dead-debug computation promoted to a real output (Python-side
addition, not upstream's own shape):** upstream also computes
`residuals`/`errSq`/`paramErrs`/`sdRapm` at this point
(`RapmUtils.ts:1363-1394`) purely to feed a `console.log` gated
behind the same hardcoded-`False` `debugMode` as
`apply_weak_priors` -- none of the four is ever stored on
`acc.output` upstream (`RapmProcessingInputs` has no `sdRapm`
field there either). Since Task 3.4 built
`calculate_predicted_out`/`calculate_residual_error`/
`calc_slow_pseudo_inverse`/`calculate_sd_rapm` specifically
so this task could surface real standard errors, this port keeps
calling all four (matching TS's actual computation, which reuses the
exact same `XᵀX + ridge_lambda·I` inverse `slow_regression`
already computed -- so no *new* failure mode is introduced by keeping
this) and additionally stores the result on `sd_rapm` -- a superset
of, not a divergence from, the upstream return shape.

**`soln_matrix`/`sd_rapm` are nested Python `list`s, not
`NDArray`s.** Every field on the returned `RapmProcessingInputs`
is a plain (possibly nested) Python `list`/`float` specifically so
the whole dict stays comparable via plain `==` -- the oracle's deep
-equality assertions (e.g. `off_results1 == off_results`) would
otherwise raise `ValueError: truth value of an array with more than one
element is ambiguous` the moment Python's dict/list equality machinery
tried to `bool()` a multi-element `ndarray` comparison.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `off_weights` | `NDArray[float64]` |  | The offensive design matrix (e.g. `calc_player_weights`'s first return value). |
| `def_weights` | `NDArray[float64]` |  | The defensive design matrix. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`. |
| `adaptive_correl_weights` | `list[float] \| None` |  | Optional per-player adaptive-correlation weights (index-aligned with `ctx["col_to_player"]`) -- see the "adaptive-weight / prior asymmetry" note above. |
| `diag_mode` | `bool` |  | If `True`, keeps sweeping every remaining `lambda` step (collecting `prev_attempts` diagnostics for all of them) even after a stopping condition has already fired, and relaxes the rollback/pick eligibility guards for the first few (`< lambda_range_to_use[3]`) diagnostic-only steps. **Not exercised by this task's oracle** (always called with `False`) -- ported faithfully from TS, uncovered by test. |
| `agg_value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- which key team/aggregate-level reads (`actual_eff`, the low-volume player adjustment) prefer when present. |
| `lineup_value_keys` | `tuple[ValueKey, ValueKey]` | `('value', 'value')` | `(off_key, def_key)` -- forwarded to `calc_lineup_outputs` as its `use_old_val_if_possible` flag (translated: `key == "old_value"`). |

**Returns**

`(off_results, def_results)` -- two `RapmProcessingInputs`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import pick_ridge_regression

off_results, def_results = pick_ridge_regression(
    off_weights, def_weights, ctx, None, False
)
print(off_results["ridge_lambda"], off_results["rapm_adj_ppp"][:3])
```

### player_per100_features {#player_per100_features}

`player_per100_features(season_stats: 'pl.DataFrame') -> 'pl.DataFrame'`

Per-100 / rate features for every (player_id, season).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season_stats` | `DataFrame` |  | One row per player-season with the canonical counting columns (`minutes, field_goals_made, field_goals_attempted, three_point_field_goals_made, free_throws_attempted, turnovers, points, fga_rim, fga_mid, fga_three, offensive_rebounds, defensive_rebounds, assists, blocks, steals`) -- built from the player-boxscore aggregation (see the Phase-0 fitters). |

**Returns**

One row per (player_id, season): ids as `Utf8` plus the 17 rate / per-100 features. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import player_per100_features
feats = player_per100_features(season_stats)
```

### playwright_transport {#playwright_transport}

`playwright_transport(*, headless_new: 'bool' = True, challenge_wait_ms: 'int' = 8000, nav_timeout_ms: 'int' = 45000, user_agent: 'Optional[str]' = None, solve_attempts: 'int' = 3, relaunch_backoff: 'float' = 2.0) -> "'_PlaywrightTransport'"`

Build the **suggested** stats.ncaa.org game-detail scraping transport.

Drives a real Chromium via Playwright in Chrome's new-headless mode
(`--headless=new`) to clear the Akamai `bm-verify` challenge that
`curl_cffi` cannot, then serves raw server HTML for the 5a-5e parsers.
Playwright is a **lazy optional import** (not a hard dependency); a clear
`ImportError` fires on first use if it is missing.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `headless_new` | `bool` | `True` | Use `--headless=new` (real-GPU render, no window) -- the default and the proven-working mode. `False` runs old headless (`headless_shell`), which Akamai flags -- avoid. |
| `challenge_wait_ms` | `int` | `8000` | Milliseconds to let the bm-verify sensor run after the first navigation. |
| `nav_timeout_ms` | `int` | `45000` | Per-navigation timeout. |
| `user_agent` | `Optional[str]` | `None` | Override the Chrome UA string. |
| `solve_attempts` | `int` | `3` |  |
| `relaunch_backoff` | `float` | `2.0` |  |

**Returns**

A stateful, callable `FetchTransport` reusing one browser for the session. Close it when done (it is a context manager, has `close()`, and registers an `atexit` safety net).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher
with NcaaFetcher.with_browser() as fetcher:
    pbp = fetcher.fetch_game_pbp("1613299")               # raw PBP HTML
    box = fetcher.fetch_game_individual_stats("1613299")  # raw box HTML
# -> feed to get_box_lineup / create_lineup_data (mbb_ncaa_*_parser)
```

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

### poss_calc_fragment_sum {#poss_calc_fragment_sum}

`poss_calc_fragment_sum(a: 'PossCalcFragment', b: 'PossCalcFragment') -> 'PossCalcFragment'`

Field-wise add two `PossCalcFragment`\ s

(`PossCalcFragment.sum`, `PossessionUtils.scala:146-153`).

The Scala original uses `shapeless.Generic` to zip the two case
classes' fields and sum pairwise; since every field is a plain `Int`,
a plain `zip` over `dataclasses.astuple` reproduces the same
behavior without the generic-programming machinery.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `PossCalcFragment` |  | The left-hand fragment. |
| `b` | `PossCalcFragment` |  | The right-hand fragment. |

**Returns**

A new `PossCalcFragment` with each field summed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import (
    PossCalcFragment,
    poss_calc_fragment_sum,
)

frag1 = PossCalcFragment(1, 2, 3, 4, 5, 6, 7, 8)
frag2 = PossCalcFragment(1, 3, 5, 7, 9, 11, 13, 15)
poss_calc_fragment_sum(frag1, frag2)
# PossCalcFragment(2, 5, 8, 11, 14, 17, 20, 23)
```

### predict_margin {#predict_margin}

`predict_margin(home_adj_em: 'float', away_adj_em: 'float', neutral: 'bool' = False, *, league: 'str' = 'mens') -> 'float'`

Expected home-minus-away margin from two adjusted efficiency margins.

The AdjEM difference is scaled by the league's fitted `em_scale` (AdjEM
is per-100-possessions; a game margin scales by roughly tempo/100, further
attenuated for as-of estimation noise) before the home-court advantage is
added.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_em` | `float` |  | Home team's adjusted efficiency margin (points / 100 poss). |
| `away_adj_em` | `float` |  | Away team's adjusted efficiency margin. |
| `neutral` | `bool` | `False` | True for a neutral-site game (no home-court advantage). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the fitted em_scale / HFA). |

**Returns**

Expected margin in points (positive favors the home team).

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import predict_margin
predict_margin(20.0, 10.0)
```

### predict_total {#predict_total}

`predict_total(home_adj_o: 'float', home_adj_d: 'float', away_adj_o: 'float', away_adj_d: 'float', home_tempo: 'float', away_tempo: 'float', *, league: 'str' = 'mens') -> 'float'`

Expected total points from adjusted efficiencies and tempos.

Expected possessions are `home_tempo * away_tempo / avg_tempo`; each
side's expected points per 100 possessions blend its offense with the
opponent's defense (`0.5 * (off + opp_def)`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_o` | `float` |  | Home adjusted offensive efficiency (points / 100 poss). |
| `home_adj_d` | `float` |  | Home adjusted defensive efficiency. |
| `away_adj_o` | `float` |  | Away adjusted offensive efficiency. |
| `away_adj_d` | `float` |  | Away adjusted defensive efficiency. |
| `home_tempo` | `float` |  | Home adjusted tempo (possessions / game). |
| `away_tempo` | `float` |  | Away adjusted tempo. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the tempo anchor). |

**Returns**

Expected combined points scored by both teams.

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import predict_total
predict_total(110.0, 95.0, 105.0, 100.0, 68.0, 66.0)
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

**Example**

```python
from sportsdataverse.mbb.mbb_bracketology import project_bracket
field = project_bracket(resume, auto_bids)
```

### rank_corr {#rank_corr}

`rank_corr(a: 'np.ndarray', b: 'np.ndarray') -> 'float'`

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

### raw_game_efficiency {#raw_game_efficiency}

`raw_game_efficiency(schedule: 'pl.DataFrame', team_box: 'pl.DataFrame') -> 'pl.DataFrame'`

Per-team, per-game possessions + raw offensive/defensive efficiency.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedule` | `DataFrame` |  | Frame with `game_id, season, date, home_team_id, away_team_id, neutral_site` (ids as strings or ints; cast to `Utf8` here). |
| `team_box` | `DataFrame` |  | Per-team boxscore with `game_id, team_id, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted, team_score`. When it also carries `total_turnovers` / `team_turnovers` (the loaders do), a row whose `turnovers` and `total_turnovers` are both 0 takes its count from `team_turnovers` -- where ESPN's 2009-2012 women's box files it. |

**Returns**

One row per (game_id, team_id): `game_id, season, date, team_id, opp_team_id, is_home, neutral_site, poss, off_eff, def_eff`. Empty input returns that schema with zero rows. Team-game rows whose possession estimate is non-positive (an all-zero ESPN boxscore shell) are dropped with a `UserWarning` -- their efficiency is undefined, and one of them poisons the whole season's fixed point. Games in which either team still has 0 turnovers are dropped the same way: the possession estimate would miss its turnover term.

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_mbb_team_boxscore
from sportsdataverse.mbb.mbb_team_ratings import raw_game_efficiency
eff = raw_game_efficiency(load_mbb_schedule([2024]), load_mbb_team_boxscore([2024]))
```

### refresh_ncaa_team_ids {#refresh_ncaa_team_ids}

`refresh_ncaa_team_ids(season: 'str', season_division_id: 'int', dates: 'Sequence[str]', *, league: 'str' = 'mbb', fetcher: "Optional['NcaaFetcher']" = None, prior_season: 'Optional[str]' = None, conference_overrides: 'Optional[Dict[str, str]]' = None, extra_teams: 'Optional[Iterable[Dict[str, object]]]' = None) -> 'pl.DataFrame'`

Extend the bundled crosswalk with a new season (update_team_ids recipe).

Port of wbigballR's `R/update_team_ids.R` maintainer scratch script:
scrape early-season scoreboard pages, harvest the `/teams/{id}` anchor
links, keep teams known from the prior season, carry each team's prior
conference forward, hand-patch realignments, and append the stamped rows
to the historical table.

Network path -- hits stats.ncaa.org via `~sportsdataverse.mbb.
mbb_ncaa_fetch.NcaaFetcher`. The result is returned (NOT written); a
maintainer overwrites `sportsdataverse/<league>/data/ncaa_teamids_
<league>.csv` with it after review.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season being added, e.g. `"2026-27"`. |
| `season_division_id` | `int` |  | The `season_divisions/{id}/scoreboards` id for that season + division (league-specific; see get_date_games' season table). |
| `dates` | `Sequence[str]` |  | Scoreboard dates to sweep, `"MM/DD/YYYY"` -- the recipe uses ~9 early-November dates so every D-I team appears at least once. |
| `league` | `str` | `'mbb'` | `"mbb"` or `"wbb"`. |
| `fetcher` | `Optional['NcaaFetcher']` | `None` | Injectable `NcaaFetcher`; defaults to a fresh one (which requires proxy/browser configuration at fetch time). |
| `prior_season` | `Optional[str]` | `None` | Season whose team list + conferences seed the join; defaults to the max season already in the bundled table. |
| `conference_overrides` | `Optional[Dict[str, str]]` | `None` | `{team: new_conference}` hand-patches for realignments (the recipe's `Conference[Team == p] <- ...` block). |
| `extra_teams` | `Optional[Iterable[Dict[str, object]]]` | `None` | Rows for brand-new programs the prior-season filter drops, e.g. `[{"team": "St. Thomas (MN)", "conference": "Summit League", "id": 529315}]` (`season` is stamped). |

**Returns**

The full refreshed crosswalk (historical rows + the new season), deduplicated and sorted by season/team.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher
from sportsdataverse.mbb.mbb_ncaa_team_ids import refresh_ncaa_team_ids
dates = [f"11/{d:02d}/2026" for d in range(9, 18)]
df = refresh_ncaa_team_ids("2026-27", 18823, dates,
                           fetcher=NcaaFetcher.with_browser())
df.write_csv("sportsdataverse/mbb/data/ncaa_teamids_mbb.csv")
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

### remove_diacritics {#remove_diacritics}

`remove_diacritics(fragment: 'str') -> 'str'`

Strip diacritical marks, e.g. `"Juhász"` -> `"Juhasz"`

(`ExtractorUtils.scala:38-43`: NFD normalization then removal of the
combining-diacritical-marks block).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fragment` | `str` |  | Any string (a full player name or a name fragment). |

**Returns**

The string with combining marks removed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import remove_diacritics
print(remove_diacritics("Dorka Juhász"))  # "Dorka Juhasz"
```

### remove_html_encoding {#remove_html_encoding}

`remove_html_encoding(html_str: 'str') -> 'str'`

Undo a handful of literal HTML entity escapes (``ExtractorUtils

.remove_html_encoding`, `ExtractorUtils.scala:25-33`). **Scope
addition, Task 5e.5** -- the first consumer is
`mbb_ncaa_shot_parser.parse_shot_html` (the `player` name / shooting
team name extracted from an SVG shot's `<title>` text).

In practice bs4/lxml already decode standard HTML entities (`&#39;`,
`&quot;`, `&amp;``) while parsing text nodes, so this is usually a
no-op by the time it runs on already-parsed text -- ported anyway for
exact behavioral parity with any double-escaped input the upstream
Scala guards against (JSoup has the same auto-decoding behavior, so the
Scala original is equally a defensive no-op in the common case).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `html_str` | `str` |  | Any string, typically already-parsed element text. |

**Returns**

`html_str` with `&#39;`/`&quot;`/`&amp;` replaced by their literal characters, only if `"&"` appears at all (short-circuit matching the Scala's `if (html_str.indexOf("&") >= 0)` guard).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import remove_html_encoding
remove_html_encoding("De&#39;Shayne")  # "De'Shayne"
remove_html_encoding("Plain Name")  # "Plain Name" (unchanged)
```
