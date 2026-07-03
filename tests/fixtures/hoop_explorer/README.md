<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [hoop-explorer oracle fixtures](#hoop-explorer-oracle-fixtures)
  - [`RapmUtils.test.ts` assertion classification map (Task 3.1)](#rapmutilstestts-assertion-classification-map-task-31)
    - [Note for Tasks 3.2-3.6: the `PlayerOnOffStats` build chain](#note-for-tasks-32-36-the-playeronoffstats-build-chain)

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

- `rapm_utils_inputs.json` (Task 3.1) — the *input* objects
  `RapmUtils.test.ts` feeds into `RapmUtils.buildPlayerContext` /
  `.calcPlayerWeights` / `.calcLineupOutputs` / `.pickRidgeRegression` /
  `.injectRapmIntoPlayers` / `.calcCollinearityDiag`. Single top-level key
  (the file's only purely-literal top-level `const`):
  - `reducedFilteredLineups` — a 31-row semi-synthetic lineup array
    (`off_adj_ppp`/`def_adj_ppp`/`off_to`/`def_to`/`off_poss`/`def_poss`
    stat objects, no player-membership fields — this is a hand-built
    micro-dataset, unrelated to the 3-lineup `sampleLineupStatsResponse`
    buckets used by the `buildPlayerContext`/`calcPlayerWeights`/
    `calcLineupOutputs` tests). It's the return value of the
    `semiRealRapmResults.testContext.filteredLineups` closure, so it's
    load-bearing wherever a ported function calls `ctx.filteredLineups(...)`
    internally: `calcLineupOutputs` calls it directly (`RapmUtils.ts:652`),
    and `pickRidgeRegression` calls it transitively via its own internal
    `calcLineupOutputs` call (`RapmUtils.ts:1212`) — so it's exercised by
    the `pickRidgeRegression` (Task 3.5), `injectRapmIntoPlayers`
    (Task 3.6), and pseudo-real-data `calcCollinearityDiag` (Task 3.6)
    tests even though none of them reference it by name.

  **Not vendored — replay recipes (all in `RapmUtils.test.ts`):**
  - `semiRealRapmResults` — the top-level `export const` wrapping
    `testOffWeights` / `testDefWeights` / `reducedFilteredLineups` (see
    above) / `testContext`. Fails json5 whole-object parse because
    `testContext.removedPlayers` values embed `StatModels.emptyIndiv()`
    calls and `testContext.filteredLineups` / `.config` embed an arrow
    function and an object spread (`...defaultRapmConfig`) respectively.
    Hand-transcribe for the Task 3.4+ oracle tests:
    - `testOffWeights` / `testDefWeights` — 32-row × 8-col literal number
      matrices (rows 0-30 = per-lineup player-weight rows, row 31 = an
      extra unbiasing-observation row) — copy verbatim from
      `RapmUtils.test.ts` lines 283-351.
    - `testContext` — `unbiasWeight: 2`; `removedPlayers`: 6 entries keyed
      by player name, each value a 3-tuple `[pct, pct2, StatModels.emptyIndiv()]`
      (the third element replays to `{"key": "empty", "doc_count": 0}`,
      the same recipe already documented for `mutableEmpty` above);
      `playerToCol` / `colToPlayer`: 8 players (`Smith, Jalen` .. `Smith Jr., Serrel`);
      `avgEfficiency: 102.4`; `numPlayers: 8`; `numOffLineups`/`numDefLineups: 31`;
      `offLineupPoss: 1351`; `defLineupPoss: 1349`; `priorInfo`: `strongWeight: 0.5`,
      `noWeakPrior: false`, `useRecursiveWeakPrior: false`, `includeStrong: {}`,
      `playersStrong`: 8 entries with **only** `off_adj_ppp` (no `def_adj_ppp` —
      this is exactly why the def branch is invariant to the adaptive
      correlation weights in the `pickRidgeRegression` test's identity
      assertions, see the classification map below), `playersWeak`: 8
      entries with both `off_adj_ppp`/`def_adj_ppp`, `keyUsed: "value"`,
      `basis: {off: 0, def: 0}`; `filteredLineups: (prefix) => reducedFilteredLineups`
      (replay as a Python callable/lambda returning the vendored array
      above, or a materialized `{"off": [...], "def": [...]}` dict per
      Task 3.2's documented shape choice); `teamInfo`: a literal
      `LineupStatSet`-shaped dict (`key: "teamInfo"`, `doc_count: 1`,
      `off_adj_ppp: {value: 112.4}`, `def_adj_ppp: {value: 82.4}`,
      `off_poss: {value: 101}`, `def_poss: {value: 99}`); `config`:
      `{...defaultRapmConfig, removalPct: 0.1}` i.e.
      `{prior_mode: -1, removal_pct: 0.1, fixed_regression: -1}`.
  - `lineupReport` — same non-vendorable expression pattern as
    `LineupUtils.test.ts`'s own `lineupReport` (documented above):
    `{lineups: (sampleLineupStatsResponse.responses[0].aggregations.lineups.buckets
    || []).map(insertOldValues), avgOff: 100.0, error_code: "test"}`. This
    is a **file-scope const built once** (not per-test), reused across the
    `buildPlayerContext`/`calcPlayerWeights`/`calcLineupOutputs`/
    `injectRapmIntoPlayers` tests — the `insertOldValues` closure here is
    identical in effect to `tests/mbb/_hoop_explorer_replay.py`'s
    `insert_old_values` (both stamp `old_value = value` +
    `override = "Test override"` for every stat whose key is in the
    luck-affected field set), so Python tests should call the existing
    helper rather than reimplementing it.
  - `playersInfoByKey` — `_.chain(samplePlayerStatsResponse.responses[0]
    .aggregations.tri_filter.buckets.baseline.player.buckets).map((p, ii) => ({
    ...p, off_adj_rtg: {value: 5.0 - 0.5*ii}, def_adj_rtg: {value: -5.0 + ii*0.5}
    })).keyBy("key").value()` — replay as: take the (already-vendored, in
    `rating_utils_inputs.json`) `samplePlayerStatsResponse` baseline-bucket
    player list, overlay `off_adj_rtg`/`def_adj_rtg` per the index-based
    formula above, key by each player's `key` field.
  - `insertOldValues` — a `const`-declared arrow function, not data; it's
    the file-local jest helper already replayed by
    `tests/mbb/_hoop_explorer_replay.py::insert_old_values` (see above).
  - Per-test inline expressions with no independent oracle value: the
    `buildPlayerContext` test's `dummyLineup1`/`dummyLineup2` (built via
    `JSON.parse(JSON.stringify(lineupReport.lineups[0]).replace(...))`
    string-substitution hacks that rename a real player to a synthetic
    one and override `off_poss`/`def_poss` — replay as a deep-copy of
    `lineupReport.lineups[0]` with the same key renames
    (`JaSmith`→`DuData` + `Smith, Jalen`→`Data, Dummy`, and separately
    `ErAyala`→`OtPlayer` + `Ayala, Eric`→`Player, Other`) and
    `off_poss`/`def_poss` set to `{"value": 50}` (`dummyLineup1`) /
    `{"value": 100}` (`dummyLineup2`); the `calcCollinearityDiag`
    (non-pseudo-real) test's `test` mathjs matrix (`[[1,0,1],[-1,-2,0],
    [0,1,-1],[0.5,0.5,0.5]]`) and `dummyContext` (a 3-player, hand-built
    `RapmPlayerContext`-shaped dict, `numPlayers: 3`, `numOffLineups:
    numDefLineups: 4`, `offLineupPoss: 10`, `defLineupPoss: 9`,
    `filteredLineups: () => []` unused by this test, `priorInfo` mostly
    empty defaults, `config: {...defaultRapmConfig, removalPct: 0.0}`) —
    both are small enough to hardcode verbatim from
    `RapmUtils.test.ts` lines 878-912.

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
two failure modes, likewise documented above. `rapm_utils_inputs.json`
(Task 3.1): 1/7 across its single source (`RapmUtils.test.ts`) — the 6
failures (`semiRealRapmResults`, `lineupReport`, `playersInfoByKey`,
`insertOldValues`, plus the `dummyContext`/`test` locals the regex also
matches even though they're declared inside a `test()` body, not at module
scope) are all genuinely non-literal (function calls / object spread /
arrow functions / expressions referencing other consts), each documented
as a replay recipe above; the one purely-literal const
(`reducedFilteredLineups`) is real, load-bearing oracle data (see above),
not incidental. No vendored entry in any input file fell back to a raw
string (`parse_sample_module` has no raw-string fallback at all — a failed
parse is omitted, never silently mis-typed).

NOTE: the upstream repo's `LICENSE` file is **Apache License 2.0**, not MIT
as originally assumed when this vendoring task was scoped — verify
attribution/compatibility against Apache-2.0 (not MIT) before release
packaging; fixtures are test-only and not shipped in the wheel regardless.

## `RapmUtils.test.ts` assertion classification map (Task 3.1)

`RapmUtils.test.ts` is 976 LOC and defines **7 `test()` blocks** inside
`describe("RapmUtils")` (`:440`) — the Phase-3 plan's brief cites "5 test
blocks"; the actual count is 7 because `calcCollinearityDiag` has **two**
separate `test()` blocks (a hand-computed-matrix oracle and a
smoke-only "pseudo-real data" pass), both correcting/extending the plan's
oracle anchors for Tasks 3.2-3.6. Unlike Phases 1-2 (mostly
`toMatchSnapshot`), **only one `toMatchSnapshot()` call site exists in the
whole file** (`:517`, inside `buildPlayerContext`'s `[0.0, 0.2].forEach`
loop, executed twice -> the 2 `rapm_utils_snap.json` entries). Every other
assertion across the other 6 test blocks is an inline `.toFixed`/`toEqual`
literal or an identity (`===`/`!==`) check — these must be hand-transcribed
into the Python tests, not looked up via `load_rapm_snap()`.

1. **`RapmUtils - buildPlayerContext`** (`:476-525`) — 2× `toMatchSnapshot()`
   (loop over `threshold` in `[0.0, 0.2]`) → `rapm_utils_snap.json` keys
   `"RapmUtils RapmUtils - buildPlayerContext 1"` (threshold `0.0`) and
   `"...2"` (threshold `0.2`), each `_.omit(results, ["filteredLineups",
   "teamInfo"])`. Plus 2 inline literals per iteration (both loop
   iterations assert the *same* value — the comment `//(filtering now
   v rare)` explains why the ternary collapses):
   `results.filteredLineups("off").length === 5` and
   `results.teamInfo.off_poss.value === 959`.
   Builds `lineupReportWithExtra` (base `lineupReport.lineups` +
   2 synthetic renamed-player lineups, see the replay-recipe note above)
   and calls `LineupUtils.lineupToTeamReport(...)` before
   `RapmUtils.buildPlayerContext(...)` — i.e. it exercises the same
   `LineupUtils` (Phase-1) → `RapmUtils` chain described in item 4 below.

2. **`RapmUtils - calcPlayerWeights`** (`:527-573`) — **no snapshot**, all
   inline. Loop over `unbiasWeight` in `[0.0, 2.0]`; builds `context` via
   `buildPlayerContext(removalPct=0.0)` off the base `lineupReport` (not
   `lineupReportWithExtra`), then mutates `context.unbiasWeight` post-hoc.
   `results = RapmUtils.calcPlayerWeights(context)` returns a
   `[offWeights, defWeights]` pair (mathjs `Matrix`es); the test-local
   `tidyResults` helper (`resMatrix.map(val => val.toFixed(3)).valueOf()`)
   relies on mathjs `Matrix.map` applying element-wise and preserving
   shape — Python replay: format every scalar to `%.3f` recursively over
   the row-major array, no special matrix-map behavior needed.
   `tidyResults(results[0])` (off) `toEqual`
   `_.filter([4 rows], (r,i) => unbiasWeight != 0 || i < 3)` — i.e. rows
   0-2 always assert, row 3 (an "extra row if adding unbiasing obs") only
   asserts when `unbiasWeight == 2.0`:

   ```text
   ["0.704","0.704","0.704","0.704","0.704","0.000"]
   ["0.511","0.511","0.511","0.511","0.000","0.511"]
   ["0.493","0.493","0.493","0.000","0.493","0.493"]
   ["2.000","2.000","2.000","1.513","1.478","1.009"]   # unbiasWeight==2.0 only
   ```

   `tidyResults(results[1])` (def), same row-gating:

   ```text
   ["0.699","0.699","0.699","0.699","0.699","0.000"]
   ["0.518","0.518","0.518","0.518","0.000","0.518"]
   ["0.493","0.493","0.493","0.000","0.493","0.493"]
   ["2.000","2.000","2.000","1.514","1.463","1.023"]   # unbiasWeight==2.0 only
   ```

3. **`RapmUtils - calcLineupOutputs`** (`:575-629`) — **no snapshot**, all
   inline. Loop over `strongWeight` in `[-1, 0.5]` (**both values are
   JS-truthy**, so the `strongWeight ? X : []` ternaries in the
   assertions below always take the `X` branch — despite reading like a
   real branch test, both iterations assert identical literals).
   `context.priorInfo.basis toEqual {off: 0, def: 0}` (both iterations).
   `results = RapmUtils.calcLineupOutputs("adj_ppp", adjustedBasisOffEff=100,
   adjustedBasisDefEff=100, context, strongWeight<0 ? adaptiveWeights : undefined)`
   — here `resMatrix` is a **plain JS array of arrays** (not a mathjs
   `Matrix`; the test-local `tidyResults` here is `resMatrix.map(arr =>
   arr.map(val => val.toFixed(2)))`, no `.valueOf()`). `tidyResults(results)
   toEqual [["13.07","5.84","7.70"], ["-8.48","-10.69","-8.83"]]`. The
   `oldValResults` variant (`calcLineupOutputs(..., [false, true])` — the
   luck `old_value` read-key hook) asserts the **same** literal array,
   because `lineupReport` was built via `.map(insertOldValues)` which
   stamps `old_value = value` everywhere, so reading `old_value` vs
   `value` is a no-op on this fixture — **this test does not actually
   exercise a value/old_value divergence**, only that the plumbing doesn't
   break; Task 3.3 should note this as a coverage gap (the luck-divergence
   path itself is validated by the pipeline's Phase-2 `mbb_luck` tests,
   not here).

4. **`RapmUtils - pickRidgeRegression`** (`:631-772`) — **no snapshot**,
   all inline; the single strongest oracle gate in the file, run twice
   (loop over `luckAdjusted` in `[true, false]` — **both iterations
   assert identical numeric literals**, per the comment "3 iterations
   (both branches now produce same prevAttempts ex values)" — this test
   does not exercise a luck divergence either, only that passing
   `"old_value"` vs `"value"` as the `valueKey`/`oldValueKeys` args
   doesn't crash/diverge on `semiRealRapmResults.testContext`, which has
   **no `old_value` fields at all** on `reducedFilteredLineups` — the
   `|| default` JS falsy-coalescing landmine the plan's Global
   Constraints calls out; confirm the Python port's fallback matches).
   Uses `semiRealRapmResults.testOffWeights/testDefWeights/testContext`
   (hand-transcribe per the replay recipe above) directly as pre-computed
   weight matrices — `buildPlayerContext`/`calcPlayerWeights` are **not**
   called in this test, but `ctx.filteredLineups` (→
   `reducedFilteredLineups`) **is** exercised transitively (`pickRidgeRegression`
   calls its own internal `calcLineupOutputs`, which calls
   `ctx.filteredLineups(prefix)` — `RapmUtils.ts:1212`/`:652`).
   - **Identity assertions** (adaptive-correlation-weight mechanism,
     "Parity risk #2" in the plan): `offResults1 === offResults`
     (`testContext1` clones `testContext`, sets `priorInfo.strongWeight =
     -1`, passes `adaptiveWeights1 = [0.5]*8` — same effective weight as
     the default/`undefined` case); `offResults2 !== offResults`
     (`adaptiveWeights2 = [0.2]*8` diverges); `defResults1 === defResults`
     and `defResults2 === defResults` (**defense is invariant to the
     adaptive weights in this fixture** — because
     `testContext.priorInfo.playersStrong` entries have **only**
     `off_adj_ppp`, no `def_adj_ppp`, so the strong-prior blend / adaptive
     weighting never activates for defense — port `getStrongWeight` to
     replicate this exact conditional, don't assume off/def are
     symmetric).
   - `offResults.playerPossPcts.map(toFixed(2))` (8 players) `toEqual`
     `["0.97","0.93","0.82","0.74","0.73","0.54","0.15","0.12"]`.
   - `defResults.playerPossPcts...` `toEqual`
     `["0.97","0.92","0.82","0.74","0.73","0.54","0.15","0.13"]`.
   - `offResults.prevAttempts.map(o => ({l: o?.ridgeLambda?.toFixed(2),
     ex: o?.results?.[0]?.toFixed(2)}))` `toEqual`
     `[{l:"1.10",ex:"2.83"}, {ex:"2.87",l:"1.32"}, {ex:"2.89",l:"1.54"}]`
     (3 adaptive-λ-loop iterations before it stabilizes).
   - `offResults.ridgeLambda.toFixed(3) === "1.536"`.
   - `_.take(offResults.rapmAdjPpp.map(toFixed(2)), 3) toEqual
     ["2.89","2.79","2.67"]`.
   - `_.take(offResults.rapmRawAdjPpp.map(toFixed(2)), 3) toEqual
     ["4.81","4.71","4.59"]`.
   - `defResults.prevAttempts...` `toEqual`
     `[{l:"1.10",ex:"-5.86"}, {l:"1.32",ex:"-5.73"}, {l:"1.54",ex:"-5.64"}]`.
   - `defResults.ridgeLambda.toFixed(3) === "1.536"`.
   - `_.take(defResults.rapmAdjPpp.map(toFixed(2)), 3) toEqual
     ["-5.64","-4.22","-4.94"]`.
   - `_.take(defResults.rapmRawAdjPpp.map(toFixed(2)), 3) toEqual
     ["-5.06","-3.70","-4.48"]`.

5. **`RapmUtils - injectRapmIntoPlayers`** (`:774-872`) — **no snapshot**,
   all inline. Loop over `luckAdjusted` in `[true, false]`. Calls
   `pickRidgeRegression` again (same args/results as item 4) to get
   `[offResults, defResults]`, then `players = [{playerId: "Mitchell,
   Makhel"}].concat(onOffReport.players)` (prepends a player absent from
   `offResults`/`defResults` to exercise the "no RAPM data for this
   player" branch). **Sequencing matters**: when `luckAdjusted`, calls
   `injectRapmIntoPlayers` **twice** on the same mutable `players` array —
   first with `["value","old_value"]`/`readKeyValue="value"`, **then**
   with `["old_value","old_value"]`/`readKeyValue="old_value"` (comment:
   "needs to be run in normal mode first") — Python replay must preserve
   this two-call mutate-in-place order, not just call it once with the
   final args. `resultsToExamine` = first 2 players' `.rapm` fields
   (`{noRapm: true}` fallback when absent), picked down to 8 keys and
   value-formatted via `keyToCheck = luckAdjusted ? "old_value" : "value"`.
   Final `toEqual`:

   ```text
   [luckAdjusted,
    {noRapm: true},
    {def_adj_ppp: "-4.94",
     def_poss: luckAdjusted ? "0.00" : "99.00",   # these don't get an old_value
     def_to: luckAdjusted ? "0.00" : "0.01",
     key: "RAPM Wiggins, Aaron",
     off_adj_ppp: "2.67",
     off_poss: luckAdjusted ? "0.00" : "101.00",
     off_to: "0.00"}]
   ```

6. **`RapmUtils - calcCollinearityDiag`** (`:874-952`) — **no snapshot**,
   all inline; a clean, hand-computed 4×3 micro-case **independent of the
   full RAPM pipeline** (mathjs `matrix([[1,0,1],[-1,-2,0],[0,1,-1],
   [0.5,0.5,0.5]])` + a 3-player `dummyContext`, hardcode both verbatim
   per the replay-recipe note above — good for TDD-ing
   `calc_collinearity_diag` in isolation before wiring it into the real
   pipeline). `tidyResults` formats every field to fixed-decimal strings.
   Final `toEqual`:

   ```text
   {lineupCombos: ["9.4618", "1.4154", "1.0000"],
    playerCombos: {PlayerA: ["0.9852","0.0103","0.0045"],
                   PlayerB: ["0.9401","0.0081","0.0519"],
                   PlayerC: ["0.9524","0.0476","0.0000"]},
    correlMatrix: [["1.0000","0.6865","0.5429"],
                   ["0.6865","1.0000","-0.2041"],
                   ["0.5429","-0.2041","1.0000"]],
    adaptiveCorrelWeights: ["0.25","0.07","0.06"]}
   ```

   (comment cites MATLAB `svd`/`corr` as the source-of-truth for this
   hand-computed matrix — useful if a Python numpy parity check needs an
   independent cross-check).

7. **`RapmUtils - calcCollinearityDiag (pseudo-real data)`** (`:953-975`)
   — **no snapshot, no `toEqual`, no assertion of any kind** — a
   smoke-only "doesn't crash" test (a `logResults = false`-gated
   `console.log` is the only observable, permanently disabled). Runs the
   **full pipeline**: `lineupReportFake` (base `lineupReport.lineups`
   concatenated with itself) → `LineupUtils.lineupToTeamReport` →
   `RapmUtils.buildPlayerContext(..., 100.0)` (called with only 5
   positional args, relying on the `keyUsed`/`config` defaults) →
   `RapmUtils.calcPlayerWeights(context)` → `RapmUtils.calcCollinearityDiag
   (weights[0], context)`. Port as a Python test that simply calls the
   chain and asserts no exception + basic shape sanity (e.g. matrix
   dimensions) — there is no upstream literal to transcribe.

**Item 4 = the "IMPORTANT-EQUATION-01" gate** for Task 3.5 (`pickRidgeRegression`):
the `"1.536"` λ and the 4 `.toFixed(2)` RAPM arrays are the load-bearing
parity numbers the plan's Global Constraints and Task 3.5 both cite — this
map confirms they're exactly as the plan states, plus documents the
*mechanism* (identity assertions + the off/def prior asymmetry) the plan
only gestured at.

### Note for Tasks 3.2-3.6: the `PlayerOnOffStats` build chain

Every `RapmUtils` test that needs real (non-`semiRealRapmResults`) player
data builds its `PlayerOnOffStats` input via the **same Phase-1 pipeline**,
not a fresh RAPM-specific loader:

```
sampleLineupStatsResponse.responses[0].aggregations.lineups.buckets   (vendored, lineup_utils_inputs.json)
  -> .map(insertOldValues)                                            (tests/mbb/_hoop_explorer_replay.py::insert_old_values)
  -> lineupReport = {lineups: [...], avgOff: 100.0, error_code: "test"}
  -> LineupUtils.lineupToTeamReport(lineupReport)                     (sportsdataverse.mbb.mbb_lineup_stats, Phase 1)
  -> onOffReport.players                                              (List[PlayerOnOffStats])
  -> RapmUtils.buildPlayerContext(onOffReport.players, lineupReport.lineups,
                                  playersInfoByKey, {}, 100.0, "value", config)
```

`playersInfoByKey` is a second, independent input built from
`samplePlayerStatsResponse` (vendored, `rating_utils_inputs.json`) via the
index-based `off_adj_rtg`/`def_adj_rtg` formula documented above — it is
**not** derived from `mbb_ratings.build_productivity`'s real output in this
test file (the plan's Task 3.2 description implies `build_priors` consumes
`build_productivity`'s output in the *production* code path, which is
correct — but the *test* fixture hand-rolls a synthetic `off_adj_rtg`/
`def_adj_rtg` overlay rather than calling `build_productivity` itself, so
Task 3.2's oracle test should replay the same hand-rolled overlay, not
invoke `build_productivity`). Later tasks (3.2-3.3) replaying
`buildPlayerContext`/`calcPlayerWeights`/`calcLineupOutputs` must reproduce
this exact chain (through `LineupUtils.lineupToTeamReport`, i.e. Phase 1's
`calculate_aggregated_lineup_stats`-derived on/off report) rather than
inventing their own `PlayerOnOffStats` fixtures. Tasks 3.4-3.6
(`pickRidgeRegression`/`injectRapmIntoPlayers`/`calcCollinearityDiag`)
instead consume the standalone `semiRealRapmResults.testContext` (hand-
transcribed per the replay recipe above) and largely bypass this chain —
except the pseudo-real `calcCollinearityDiag` test (item 7), which goes
through the full chain again with a doubled-up `lineupReport`.
