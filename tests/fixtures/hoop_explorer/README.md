<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [hoop-explorer oracle fixtures](#hoop-explorer-oracle-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# hoop-explorer oracle fixtures

Vendored from Alex-At-Home/cbb-on-off-analyzer (hoop-explorer.com SPA), local
clone `GitHub-Data/cbb-on-off-analyzer` @ `0252725cd94bf54dd5384d0d9af3f2382367c057`
on 2026-07-03, via `tools/vendor_hoop_explorer_fixtures.py`.

Regenerate all seven fixture files (after bumping the upstream clone) via:

```sh
uv run python tools/vendor_hoop_explorer_fixtures.py
```

- `lineup_utils_snap.json` — jest snapshots of LineupUtils (aggregation,
  on/off reports). Oracle for `sportsdataverse/mbb/mbb_lineup_stats.py`.
- `rating_utils_snap.json` / `luck_utils_snap.json` / `rapm_utils_snap.json`
  — oracles for phases 2-3.
- `lineup_utils_inputs.json` — the *input* objects `LineupUtils.test.ts`
  feeds into `LineupUtils.calculateAggregatedLineupStats` /
  `.lineupToTeamReport` / `.getGameInfo`, so Phase-1 Python tests can
  replay input -> output against `lineup_utils_snap.json`. Three top-level
  keys, one per source constant:
  - `sampleLineupStatsResponse` — full raw ES-aggregation-shaped payload
    (`src/sample-data/sampleLineupStatsResponse.ts`, imported by the test
    file). `responses[0].aggregations.lineups.buckets` holds 3 lineup docs
    (`AaWiggins_AnCowan_DaMorsell_ErAyala_JaSmith`,
    `AaWiggins_AnCowan_DaMorsell_DoScott_JaSmith`,
    `AaWiggins_AnCowan_DoScott_ErAyala_JaSmith`), each with `off_poss` /
    `def_poss` (`{"value": N}`) and a `players_array` ES-hits payload
    carrying player `code`/`id` membership. Feeds both
    `test("LineupUtils - calculateAggregatedLineupStats")` and
    `test("LineupUtils - lineupToTeamReport")` — the test file builds
    `lineupReport.lineups` from these buckets via `.map(insertOldValues)`
    (adds `old_value`/`override` to every stat whose key is in
    `LuckUtils.affectedFieldSet`) and additionally sets
    `lineups[1].rapmRemove = true` before calling
    `calculateAggregatedLineupStats`; that test-local mutation is jest
    logic, not sample data, so it is NOT re-derived here — replay it in the
    Python test the same way the jest test does.
  - `testIn` — inline `const testIn = {...}` literal declared directly in
    `LineupUtils.test.ts` (an ES `terms`-aggregation-shaped payload keyed by
    team, `H:Nebraska` / `A:Penn St.` / `H:Minnesota` / `H:Purdue`, each with
    a `game_info` date-histogram of `num_pts_for` / `num_off_poss` /
    `num_pts_against` / `num_def_poss`). Feeds
    `test("LineupUtils - gameGameInfo")` as the first arg to
    `LineupUtils.getGameInfo(testIn, mutableOpponents)`.
  - `mutableOpponents` — the second (initially-empty, mutated-in-place) arg
    to `getGameInfo` in the same test; vendored for completeness even
    though it's `{}`.

  Not vendored: the test file's own `lineupReport` const (an expression —
  `sampleLineupStatsResponse.responses[0]....map(insertOldValues)` — not an
  object/array literal, so it isn't json5-parseable and isn't "input data"
  in the vendoring sense). Python tests reconstructing `lineupReport`
  (`tests/mbb/test_mbb_lineup_stats.py::_build_lineup_report`) must hardcode
  its two scalar companions verbatim from `LineupUtils.test.ts`, since they
  aren't part of the non-literal expression above: `avgOff = 100.0` and
  `error_code = "test"`.

- `rating_utils_inputs.json` (Task 2.1) — the *input* objects
  `RatingUtils.test.ts` feeds into `RatingUtils.buildORtg` /
  `.buildNetPoints` / `.adjustOffRatingStats` / `.buildOffOverrides` /
  `.buildDRtg` / `.injectUncatOnBallDefenseStats` /
  `.buildOnBallDefenseAdjustmentsPhase1` /
  `.injectOnBallDefenseAdjustmentsPhase2`, plus the literal expected-value
  consts the test asserts against inline (`toEqual`) rather than via
  `toMatchSnapshot`. 14 top-level keys:
  - `samplePlayerStatsResponse` (`src/sample-data/samplePlayerStatsResponse.ts`)
    — the shared ES player-payload. `responses[0].aggregations.tri_filter
    .buckets.baseline.player.buckets` holds 2 player docs (`Cowan, Anthony`
    index 0, `Wiggins, Aaron` index 1). Every `RatingUtils` test builds its
    `playerInfo` via `_.cloneDeep(...buckets[0])` (deep-copy before mutating)
    except `injectOnBallDefenseAdjustmentsPhase2`, which clones the whole
    2-player `buckets` array into `playersToMutate`. **Also the source for
    `basePlayers` / `samplePlayersOn` in `LuckUtils.test.ts`** (via the same
    property path, just different `tri_filter.buckets.<baseline|on>` key) —
    it lives here rather than being duplicated into `luck_utils_inputs.json`;
    replay the same property-path lookup against this file's copy.
  - `samplePlayerStatsTemplate` — an internal helper const in the same
    source file (feeds `samplePlayerStatsResponseOld` via
    `SampleDataUtils.buildResponseFromTemplatePlayer(...)`, which is *not*
    vendored — a function-call value embedded in an otherwise-literal
    object fails json5). Vendored incidentally as a side effect of parsing
    the whole module; no `RatingUtils` test reads it directly.
  - `sampleOrtgDiagnostics` / `sampleDrtgDiagnostics` — hand-built
    `ORtgDiagnostics`/`DRtgDiagnostics` oracle objects
    (`src/sample-data/sample{Ortg,Drtg}Diagnostics.ts`). Fed as a direct
    3rd positional arg to `buildOnBallDefenseAdjustmentsPhase1`
    (`sampleDrtgDiagnostics`) and compared via `toMatchObject`
    (`sampleOrtgDiagnostics`, `buildORtg` test) / `toEqual`
    (`sampleDrtgDiagnostics`, `buildDRtg` test).
  - `sampleOnBallDefenseStats` (`src/sample-data/sampleOnBallDefenseStats.ts`)
    — a 2-element tuple `[teamStats, [8 playerStats]]`. Index `[0]` is the
    team `OnBallDefenseModel`, index `[1]` is the 8-player array. Feeds
    `injectUncatOnBallDefenseStats`, `buildOnBallDefenseAdjustmentsPhase1`,
    and `injectOnBallDefenseAdjustmentsPhase2` (all three re-derive
    `modOnBallStats` by calling `injectUncatOnBallDefenseStats(teamStats,
    onBallStats)` themselves — a call-chain, not fresh input data).
  - `outputs` — inline `const outputs = {...}` in the
    `"buildOffOverrides"` test (9 keys, one `undefined` folded to `null`
    per the vendoring convention). Fed to `RatingUtils.buildOffOverrides`
    only as part of `testStatSet` (see below).
  - `expORtg` / `expORtgAdj` / `expORtg2` / `expORtgAd2` / `expORtg3` /
    `expORtgAd3` / `expDRtg` / `expDRtgAdj` — inline `const exp* = {value:
    N}` literals the test asserts its return values against (`buildORtg`
    3x-call and `buildDRtg` 1st-call assertions). Not function *inputs*,
    but load-bearing oracle values with no `toMatchSnapshot` counterpart
    (`buildORtg` never snapshots `oRtg`/`adjORtg`, only `oRtgDiags`), so
    they're vendored here rather than left to bit-rot in the upstream file.

  **Not vendored — replay recipes (all in `RatingUtils.test.ts`):**
  - `testStatSet` / `testStatSet2` (`"buildOffOverrides"` test) — both
    contain `...outputs` (object spread), which json5 can't parse, so the
    *whole* const fails to vendor even though most of its keys are literal.
    Reconstruct in Python as the merge of `outputs` (vendored above) with
    these explicit literal keys (copied verbatim from the test file):
    `testStatSet = {"total_off_3p_attempts": {"value": 10},
    "total_off_2p_attempts": {"value": 20}, "total_off_fta": {"value": 20},
    "total_off_to": {"value": 20}, "off_poss": {"value": 100}, "off_3p":
    {"value": 0.5, "old_value": 0.4}, "off_2p": {"value": 0.6, "old_value":
    0.4}, "off_ft": {"value": 0.9, "old_value": 0.7}, "off_to": {"value":
    0.25, "old_value": 0.2}, **outputs}`; `testStatSet2` is the same base
    dict with the four override fields replaced by their bare `value`s
    (`off_3p={"value":0.4}`, `off_2p={"value":0.4}`, `off_ft={"value":0.7}`,
    `off_to={"value":0.2}`, no `old_value`). The two `expect(...).toEqual(
    {...})` oracle blocks for this test are themselves inline literals (not
    `const`-declared, so the vendoring regex can't reach them either) —
    hardcode them verbatim from `RatingUtils.test.ts` lines 213-228 and
    239-244.
  - `ortgWithFactors` (`"buildNetPoints"` test) — `{...oRtgDiags,
    adjPtsFactor: 1.1, adjPossFactor: 0.9}`, a spread over the `oRtgDiags`
    returned by a `buildORtg` call made earlier in the *same* test (on
    `playerInfo` with `off_team_poss_pct`/`def_team_poss_pct` both hardcoded
    to `{"value": 0.25}`) — replay the call chain
    (`buildORtg`/`buildDRtg` -> override 2 keys on the result) rather than
    vendoring a static object.
  - `mutableDiag` / `maybeRawORtg` (`"adjustOffRatingStats"` test) — a
    deep-copy of a fresh `buildORtg(playerInfo, ...)` call's `oRtgDiags`
    (no overrides on `playerInfo` this time) and that same call's `rawORtg`
    return position, `?.value`-accessed (evaluates to `None`/`null` for
    this arg tuple, matching the `buildORtg` test's own `rawORtg ==
    undefined` assertion for identical args).
  - Per-test in-place field overrides on the shared `playerInfo`/`sampleTeamOn`-
    style clone (not separate consts, just property assignment) —
    hardcode each verbatim: `buildORtg` override 2
    (`playerInfo.off_3p = {value: <off_3p.value - 0.1>, old_value:
    <original off_3p.value>}`) and override 3 (`playerInfo.off_team_poss_pct
    = {value: 0.5}`); `buildDRtg` override (`playerInfo.oppo_def_3p =
    {value: 0.3, old_value: 0.4}`, with the post-override expected
    `dRtg2`/`adjDRtg2` = `{value: 90.04849177213895}` /
    `{value: -3.3304841564964764}` hardcoded inline in the test, not a
    vendorable const); `buildOnBallDefenseAdjustmentsPhase1` /
    `injectOnBallDefenseAdjustmentsPhase2` override
    (`playerInfo.def_team_poss_pct = {value: 0.2}`); the latter also sets
    `p.def_rtg = {value: dRtgDiag.dRtg}` per player (a formula, not a
    literal — `dRtgDiag.dRtg` comes from the cloned `sampleDrtgDiagnostics`).

- `luck_utils_inputs.json` (Task 2.1) — the *input* objects
  `LuckUtils.test.ts` feeds into `LuckUtils.calcOffTeamLuckAdj` /
  `.calcOffPlayerLuckAdj` / `.calcDefTeamLuckAdj` / `.calcDefPlayerLuckAdj` /
  `.injectLuck`. 6 top-level keys:
  - `sampleTeamStatsResponse` (`src/sample-data/sampleTeamStatsResponse.ts`)
    — the ES team-payload. `baseTeam` =
    `...aggregations.global.only.buckets.team`; `sampleTeamOn` /
    `sampleTeamOff` = `...aggregations.tri_filter.buckets.on` / `.off`.
  - `sampleTeamStatsTemplate` — same "internal helper, vendored
    incidentally" situation as `samplePlayerStatsTemplate` above (feeds the
    non-vendorable `sampleTeamStatsResponseOld`); no `LuckUtils` test reads
    it directly.
  - `sampleOffOnOffLuckDiagnostics` / `sampleDefOnOffLuckDiagnostics`
    (`src/sample-data/sampleOnOffLuckDiagnostics.ts`) — the oracle objects
    `calcOffTeamLuckAdj` / `calcDefTeamLuckAdj`'s first-call outputs are
    compared against via `toEqual` (in addition to `toMatchSnapshot`).
  - `overrides` — inline `const overrides = [{rowId: "Cowan, Anthony",
    statName: "off_3p", newVal: 0.5, use: true}]` in the
    `"calcOffTeamLuckAdj (+manual overrides)"` test; fed as the 7th
    (`manualOverrides`) positional arg.
  - `samplePlayerDef` — inline `const samplePlayerDef = {key: "test",
    oppo_total_def_3p_attempts: {value: 100}, oppo_total_def_3p_made:
    {value: 25}} as IndivStatSet;` in the `"injectLuck"` test (the
    trailing TS cast previously defeated the vendoring regex — see the
    `CONST_RE` note below). Fed to `LuckUtils.injectLuck` via a
    `_.cloneDeep` (`testPlayerDef`); the post-`injectLuck` expected
    `oppo_def_3p` is `{value: 0.32379827978580994, old_value: 0.25,
    override: "Luck adjusted"}` (hardcode — it's an inline `toEqual`
    literal, not a separate const), and the post-reset
    (`injectLuck(testPlayerDef, undefined, undefined)`) expected value is
    `{value: 0.25}`.

  `samplePlayersOn` / `basePlayers` (used throughout) are derived from
  `samplePlayerStatsResponse`, which is vendored in
  **`rating_utils_inputs.json`**, not duplicated here (see that file's
  entry above) — replay via
  `...aggregations.tri_filter.buckets.<on|baseline>.player.buckets`.

  **Not vendored — replay recipes (all in `LuckUtils.test.ts`):**
  - `basePlayersMap` — `_.fromPairs(basePlayers.map(p => [p.key, p]))`;
    replay as `{p["key"]: p for p in basePlayers}`.
  - `offTeamLuckAdjWithOverride`'s first arg (`"calcOffPlayerLuckAdj"` test)
    — `{...samplePlayersOn[0], total_off_3p_attempts: {value: 0}}` (object
    spread fails json5); replay as `samplePlayersOn[0]` with that one key
    overridden, alongside the extra scalar arg
    `samplePlayersOn[0].total_off_3p_attempts.value` passed positionally.
  - `adjSampleTeamOn` (`"+manual overrides"` test) — a deep clone of
    `sampleTeamOn` with 3 fields overridden, each pulling its `old_value`
    from the *pre-override* base: `off_to = {value: 0.1, old_value:
    sampleTeamOn.off_to.value}`, `off_2p = {value: 0.8, old_value:
    sampleTeamOn.off_2p.value}`, `off_ft = {value: 0.0, old_value:
    sampleTeamOn.off_ft.value}`.
  - `samplePlayerWithExtraStats` / `basePlayerWithExtraStats`
    (`"calcDefPlayerLuckAdj"` test) — `_.assign(cloneDeep(samplePlayersOn[0]
    / basePlayers[0]), {def_3p: {value: oppo_total_def_3p_made.value /
    oppo_total_def_3p_attempts.value}, def_3p_opp: oppo_def_3p_opp,
    def_poss: oppo_total_def_poss, total_def_3p_attempts:
    oppo_total_def_3p_attempts})` — a ratio-derived field plus 3 renamed
    passthroughs, all read off the same player doc.
  - `samplePlayerNeedingOverride` — `{...samplePlayerWithExtraStats,
    total_def_3p_attempts: {value: 0}}` (object spread fails json5); replay
    as the dict above with that one key overridden.
  - `mutableEmpty` — `StatModels.emptyIndiv()`
    (`src/utils/StatModels.ts:594-596`) — not a vendorable top-level
    const (it's a static-method call), but trivial to hardcode:
    `{"key": "empty", "doc_count": 0}`.
  - `savedSampleTeamOn` / `savedMutatedSampleTeamOn` — plain
    `_.cloneDeep` snapshots taken before/after mutating `sampleTeamOn`
    in place via `injectLuck`; no independent data, just deep-copy the
    vendored `sampleTeamStatsResponse`-derived slice at the right point
    in the call sequence.

`CONST_RE` in `tools/vendor_hoop_explorer_fixtures.py` gained three
generalizations to reach the RatingUtils/LuckUtils sources (all
backward-compatible with the LineupUtils sources above): an optional `//`
line comment between `=` and the literal's opening bracket
(`sampleOnBallDefenseStats.ts`), a TS cast generalized from literally
`as const` to `as (unknown as )?\w+` (`samplePlayerDef`'s `as
IndivStatSet`), and ASI support — terminate a match at a literal `;` *or*,
via a non-consuming lookahead, at the next top-level `const`/`export const`
or end-of-file, since `samplePlayerStatsResponse.ts` never writes the
closing `;` at all and relies on automatic semicolon insertion.

Parse rate (jest snapshot entries -> JSON via `json5`, `undefined` folded to
`null`): 15/15 (LineupUtils), 7/7 (RatingUtils), 4/4 (LuckUtils), 2/2
(RapmUtils) — 100% across all four files (well above the >=80% acceptance
bar; no entries fell back to raw-string).

Parse rate (test-input `const` object/array literals -> JSON via `json5`,
same `undefined` -> `null` folding): 3/4 across the two `lineup_utils_inputs.json`
sources (`sampleLineupStatsResponse.ts` + `LineupUtils.test.ts`) — the one
failure is the expected non-literal `lineupReport` const documented above,
not a parser gap. `rating_utils_inputs.json`: 14/18 across its five sources
— 4 failures are all genuinely non-literal (`testStatSet`, `testStatSet2`,
`ortgWithFactors` all contain object spreads; `samplePlayerStatsResponseOld`
embeds a function-call value), each documented as a replay recipe above.
`luck_utils_inputs.json`: 6/8 across its three sources — 2 failures
(`sampleTeamStatsResponseOld`, `samplePlayerNeedingOverride`) are the same
two failure modes, likewise documented above. No vendored entry in either
new file fell back to a raw string (`parse_sample_module` has no raw-string
fallback at all — a failed parse is omitted, never silently mis-typed).

NOTE: the upstream repo's `LICENSE` file is **Apache License 2.0**, not MIT
as originally assumed when this vendoring task was scoped — verify
attribution/compatibility against Apache-2.0 (not MIT) before release
packaging; fixtures are test-only and not shipped in the wheel regardless.
