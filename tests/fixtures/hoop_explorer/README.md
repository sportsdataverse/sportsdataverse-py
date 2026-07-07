<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [hoop-explorer oracle fixtures](#hoop-explorer-oracle-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# hoop-explorer oracle fixtures

Vendored from Alex-At-Home/cbb-on-off-analyzer (hoop-explorer.com SPA), local
clone `GitHub-Data/cbb-on-off-analyzer` @ `0252725cd94bf54dd5384d0d9af3f2382367c057`
on 2026-07-03, via `tools/vendor_hoop_explorer_fixtures.py`.

Regenerate all four fixture files (after bumping the upstream clone) via:

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

Parse rate (jest snapshot entries -> JSON via `json5`, `undefined` folded to
`null`): 15/15 (LineupUtils), 7/7 (RatingUtils), 4/4 (LuckUtils), 2/2
(RapmUtils) — 100% across all four files (well above the >=80% acceptance
bar; no entries fell back to raw-string).

Parse rate (test-input `const` object/array literals -> JSON via `json5`,
same `undefined` -> `null` folding): 3/4 across the two `lineup_utils_inputs.json`
sources (`sampleLineupStatsResponse.ts` + `LineupUtils.test.ts`) — the one
failure is the expected non-literal `lineupReport` const documented above,
not a parser gap.

NOTE: the upstream repo's `LICENSE` file is **Apache License 2.0**, not MIT
as originally assumed when this vendoring task was scoped — verify
attribution/compatibility against Apache-2.0 (not MIT) before release
packaging; fixtures are test-only and not shipped in the wheel regardless.
