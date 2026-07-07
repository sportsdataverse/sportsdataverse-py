<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Third-Party Notices](#third-party-notices)
  - [cbb-on-off-analyzer (`LineupUtils.ts`)](#cbb-on-off-analyzer-lineuputilsts)
  - [cbb-on-off-analyzer (`RatingUtils.ts`)](#cbb-on-off-analyzer-ratingutilsts)
  - [cbb-on-off-analyzer (`LuckUtils.ts`)](#cbb-on-off-analyzer-luckutilsts)
  - [cbb-on-off-analyzer (`RapmUtils.ts`)](#cbb-on-off-analyzer-rapmutilsts)
  - [cbb-on-off-analyzer (`PositionUtils.ts`)](#cbb-on-off-analyzer-positionutilsts)
  - [cbb-explorer (`EventUtils.scala` / `PossessionUtils.scala` / `StateUtils.scala` / `LineupEvent.scala` / `LineupEventStats.scala` / `Game.scala`)](#cbb-explorer-eventutilsscala--possessionutilsscala--stateutilsscala--lineupeventscala--lineupeventstatsscala--gamescala)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Third-Party Notices

`sportsdataverse-py` is licensed under the MIT License (see `LICENSE` at the
repository root). This file lists third-party code that has been ported or
vendored into this repository under a different license, as required by
that license's own attribution obligations.

## cbb-on-off-analyzer (`LineupUtils.ts`)

- **Project:** [Alex-At-Home/cbb-on-off-analyzer](https://github.com/Alex-At-Home/cbb-on-off-analyzer)
  (the hoop-explorer.com single-page app).
- **License:** Apache License, Version 2.0 -- full text at
  <http://www.apache.org/licenses/LICENSE-2.0>, and vendored verbatim in the
  upstream repository's `LICENSE` file.
- **Copyright:** Copyright (c) Alex-At-Home
  (<https://github.com/Alex-At-Home>) and contributors. Licensed under the
  Apache License, Version 2.0.
- **What was derived:** `src/utils/stats/LineupUtils.ts` was ported
  line-for-line (including documented bug-for-bug behavior) into
  [`sportsdataverse/mbb/mbb_lineup_stats.py`](sportsdataverse/mbb/mbb_lineup_stats.py).
  [`sportsdataverse/wbb/wbb_lineup_stats.py`](sportsdataverse/wbb/wbb_lineup_stats.py)
  re-exports the same functions by reference (no separate copy of the
  logic). The jest snapshot fixtures and their input literals used as an
  offline correctness oracle for this port are vendored under
  [`tests/fixtures/hoop_explorer/`](tests/fixtures/hoop_explorer/) -- these
  are test-only fixtures and are not shipped in the distributed wheel or
  sdist.
- **Modifications:** Translated from TypeScript to Python, following this
  repository's own conventions (typing, docstrings). No changes were made
  to the original TypeScript source itself; the Python port is a faithful
  (including bug-for-bug, where explicitly documented in the module
  docstring) translation of the upstream logic, not a functional rewrite.

## cbb-on-off-analyzer (`RatingUtils.ts`)

- **Project:** [Alex-At-Home/cbb-on-off-analyzer](https://github.com/Alex-At-Home/cbb-on-off-analyzer)
  (the hoop-explorer.com single-page app).
- **License:** Apache License, Version 2.0 -- full text at
  <http://www.apache.org/licenses/LICENSE-2.0>, and vendored verbatim in the
  upstream repository's `LICENSE` file.
- **Copyright:** Copyright (c) Alex-At-Home
  (<https://github.com/Alex-At-Home>) and contributors. Licensed under the
  Apache License, Version 2.0.
- **What was derived:** `src/utils/stats/RatingUtils.ts`'s individual
  offensive-rating surface (`buildORtg`, `buildOffOverrides`,
  `buildProductivity`, and the `Replacement_Level` /
  `retainPossWithReboundRate` constants) was ported line-for-line into
  [`sportsdataverse/mbb/mbb_ratings.py`](sportsdataverse/mbb/mbb_ratings.py),
  along with the `OverrideUtils.diff` helper from
  `src/utils/stats/OverrideUtils.ts`. Further `RatingUtils.ts` surface
  (`buildDRtg`, `buildNetPoints`, `adjustOffRatingStats`, the
  on-ball-defense adjustment family) is being ported incrementally into the
  same module; this entry covers the module as a whole.
  [`sportsdataverse/wbb/wbb_ratings.py`](sportsdataverse/wbb/wbb_ratings.py)
  re-exports the same functions by reference (no separate copy of the
  logic). The jest snapshot
  fixtures and their input literals used as an offline correctness oracle
  are vendored under
  [`tests/fixtures/hoop_explorer/`](tests/fixtures/hoop_explorer/) -- these
  are test-only fixtures and are not shipped in the distributed wheel or
  sdist.
- **Modifications:** Translated from TypeScript to Python, following this
  repository's own conventions (typing, docstrings). No changes were made
  to the original TypeScript source itself; the Python port is a faithful
  translation of the upstream logic (documented divergences are limited to
  Python-vs-JS division semantics -- `ZeroDivisionError` instead of
  NaN/Infinity propagation -- itemized in the module docstring), not a
  functional rewrite.

## cbb-on-off-analyzer (`LuckUtils.ts`)

- **Project:** [Alex-At-Home/cbb-on-off-analyzer](https://github.com/Alex-At-Home/cbb-on-off-analyzer)
  (the hoop-explorer.com single-page app).
- **License:** Apache License, Version 2.0 -- full text at
  <http://www.apache.org/licenses/LICENSE-2.0>, and vendored verbatim in the
  upstream repository's `LICENSE` file.
- **Copyright:** Copyright (c) Alex-At-Home
  (<https://github.com/Alex-At-Home>) and contributors. Licensed under the
  Apache License, Version 2.0.
- **What was derived:** `src/utils/stats/LuckUtils.ts`'s full 3P
  luck-adjustment surface -- both the offensive half (`calcOffTeamLuckAdj`,
  `calcOffPlayerLuckAdj`, `build3PShotInfo`, `buildAdjusted3P`, `buildExp3P`,
  the generalized `buildShotInfo` / `buildAdjustedFG` they wrap) and the
  defensive half (`calcDefTeamLuckAdj`, `calcDefPlayerLuckAdj`), plus the
  mutate-in-place `injectLuck` application glue and the `affectedFieldSet`
  constant -- was ported line-for-line into
  [`sportsdataverse/mbb/mbb_luck.py`](sportsdataverse/mbb/mbb_luck.py).
  Alongside `injectLuck`, a scoped port of
  `src/utils/stats/OverrideUtils.ts`'s `overrideMutableVal` primitive (plus
  its two small dependencies, `getOriginalVal`/`getIgnoreNil`) was added to
  the same module -- the only `OverrideUtils` member `injectLuck` calls; the
  shot-quality-override-UI-specific remainder of `OverrideUtils.ts` was not
  ported (see `mbb_luck.py`'s module docstring for the exact scope).
  [`sportsdataverse/wbb/wbb_luck.py`](sportsdataverse/wbb/wbb_luck.py)
  re-exports the same functions and the `LUCK_AFFECTED_FIELDS` constant by
  reference (no separate copy of the logic). The jest snapshot fixtures and
  their input literals used as an offline
  correctness oracle are vendored under
  [`tests/fixtures/hoop_explorer/`](tests/fixtures/hoop_explorer/) -- these
  are test-only fixtures and are not shipped in the distributed wheel or
  sdist.
- **Modifications:** Translated from TypeScript to Python, following this
  repository's own conventions (typing, docstrings). No changes were made
  to the original TypeScript source itself; the Python port is a faithful
  translation of the upstream logic (documented divergences are limited to
  Python-vs-JS division/truthiness semantics -- `ZeroDivisionError` instead
  of NaN/Infinity propagation, and explicit `is not None` checks in place of
  JS's array/empty-object truthiness -- itemized in the module docstring),
  not a functional rewrite.

## cbb-on-off-analyzer (`RapmUtils.ts`)

- **Project:** [Alex-At-Home/cbb-on-off-analyzer](https://github.com/Alex-At-Home/cbb-on-off-analyzer)
  (the hoop-explorer.com single-page app).
- **License:** Apache License, Version 2.0 -- full text at
  <http://www.apache.org/licenses/LICENSE-2.0>, and vendored verbatim in the
  upstream repository's `LICENSE` file.
- **Copyright:** Copyright (c) Alex-At-Home
  (<https://github.com/Alex-At-Home>) and contributors. Licensed under the
  Apache License, Version 2.0.
- **What was derived:** `src/utils/stats/RapmUtils.ts`'s initialization
  layer -- the `RapmPriorInfo`/`RapmPlayerContext`/`RapmConfig` types,
  `defaultRapmConfig`, `buildPriors` (incl. the `getPriorBasis` closure), and
  `buildPlayerContext` -- was ported line-for-line (including documented
  bug-for-bug behavior) into
  [`sportsdataverse/mbb/mbb_rapm.py`](sportsdataverse/mbb/mbb_rapm.py). The
  module's remaining matrix-solve / ridge-regression / collinearity-
  diagnostics surface (`calcPlayerWeights`, `calcLineupOutputs`,
  `pickRidgeRegression`, `injectRapmIntoPlayers`, `calcCollinearityDiag`) was
  ported incrementally into the same module across subsequent tasks; this
  entry covers the module as a whole.
  [`sportsdataverse/wbb/wbb_rapm.py`](sportsdataverse/wbb/wbb_rapm.py)
  re-exports the same functions, `TypedDict` types, and constants by
  reference (no separate copy of the logic). The jest snapshot fixtures and
  their input literals used as an offline correctness oracle are vendored
  under
  [`tests/fixtures/hoop_explorer/`](tests/fixtures/hoop_explorer/) -- these
  are test-only fixtures and are not shipped in the distributed wheel or
  sdist.
- **Modifications:** Translated from TypeScript to Python, following this
  repository's own conventions (typing, docstrings). No changes were made
  to the original TypeScript source itself; the Python port is a faithful
  (including bug-for-bug, where explicitly documented in the module
  docstring) translation of the upstream logic, not a functional rewrite.

## cbb-on-off-analyzer (`PositionUtils.ts`)

- **Project:** [Alex-At-Home/cbb-on-off-analyzer](https://github.com/Alex-At-Home/cbb-on-off-analyzer)
  (the hoop-explorer.com single-page app).
- **License:** Apache License, Version 2.0 -- full text at
  <http://www.apache.org/licenses/LICENSE-2.0>, and vendored verbatim in the
  upstream repository's `LICENSE` file.
- **Copyright:** Copyright (c) Alex-At-Home
  (<https://github.com/Alex-At-Home>) and contributors. Licensed under the
  Apache License, Version 2.0.
- **What was derived:** `src/utils/stats/PositionUtils.ts`'s positional
  classifier surface -- the LDA constant tables (`positionFeatureInit`,
  `tradPosList`, `positionFeatureWeights`, `positionFeatureAverages`,
  `heightMeanStds`), the memoized `averageScoresByPos` derivation,
  `regressShotQuality`, `buildPositionConfidences`, `incorporateHeight`,
  `buildPosition` (incl. `idToPosition`), `usingRosterPos`,
  `posClassToScore`, `orderLineup` (incl. the private
  `applyRelativePositionalOverrides` recursive helper),
  `buildPositionalAwareFilter`, and `testPositionalAwareFilter` -- was ported
  line-for-line (including documented bug-for-bug behavior) into
  [`sportsdataverse/mbb/mbb_positions.py`](sportsdataverse/mbb/mbb_positions.py).
  The tested subset of `src/utils/stats/PositionalManualFixes.ts`'s
  `absolutePositionFixes` and `relativePositionFixes` data tables was ported
  alongside it (see the module docstring for the exact deferred-rows
  scope). [`sportsdataverse/wbb/wbb_positions.py`](sportsdataverse/wbb/wbb_positions.py)
  re-exports the same functions and constants by reference (no separate copy
  of the logic). The jest test fixtures and their input literals used as an
  offline correctness oracle are vendored under
  [`tests/fixtures/hoop_explorer/`](tests/fixtures/hoop_explorer/) -- these
  are test-only fixtures and are not shipped in the distributed wheel or
  sdist.
- **Modifications:** Translated from TypeScript to Python, following this
  repository's own conventions (typing, docstrings). No changes were made
  to the original TypeScript source itself; the Python port is a faithful
  (including bug-for-bug, where explicitly documented in the module
  docstring) translation of the upstream logic, not a functional rewrite.

## cbb-explorer (`EventUtils.scala` / `PossessionUtils.scala` / `StateUtils.scala` / `LineupEvent.scala` / `LineupEventStats.scala` / `Game.scala`)

- **Project:** [Alex-At-Home/cbb-explorer](https://github.com/Alex-At-Home/cbb-explorer)
  (the hoop-explorer.com NCAA play-by-play ingest engine, Scala 2.12, package
  `org.piggottfamily.cbb_explorer`). This is a **distinct upstream repository**
  from `cbb-on-off-analyzer` (the TypeScript single-page app covered by the
  entries above) -- same author/org, separate codebase.
- **License:** Apache License, Version 2.0 -- full text at
  <http://www.apache.org/licenses/LICENSE-2.0>, and vendored verbatim in the
  upstream repository's `LICENSE` file.
- **Copyright:** Copyright (c) Alex-At-Home
  (<https://github.com/Alex-At-Home>) and contributors. Licensed under the
  Apache License, Version 2.0.
- **What was derived:** the NCAA possession-core data-model and possession-
  calculator layer was ported line-for-line (including documented
  bug-for-bug behavior) into three modules:
  - `LineupEvent.scala`'s `RawGameEvent` record and possession-accessor
    companions, `Game.scala`'s `LocationType`, and `LineupEventStats.scala`'s
    full nested stat-tree shape, plus the identity/value types and
    `PossessionUtils.scala`'s `PossCalcFragment`/`poss_calc_fragment_sum`/
    `score_to_tuple`, were ported into
    [`sportsdataverse/mbb/mbb_ncaa_models.py`](sportsdataverse/mbb/mbb_ncaa_models.py).
  - `EventUtils.scala`'s full PBP-line extractor surface (one `parse_x`
    function per Scala `ParseX` object) was ported into
    [`sportsdataverse/mbb/mbb_ncaa_events.py`](sportsdataverse/mbb/mbb_ncaa_events.py).
  - `PossessionUtils.scala`'s concurrent-event batching, per-clump
    possession-fragment algorithm (`calculate_stats`), and
    lineup-assignment/balancing/clamping pass were ported into
    [`sportsdataverse/mbb/mbb_ncaa_possessions.py`](sportsdataverse/mbb/mbb_ncaa_possessions.py).
    `StateUtils.scala`'s generic `foldLeft` clumping machinery was **not**
    ported as a reusable abstraction -- this port's single clumper
    (`Concurrency.concurrent_event_handler`) is inlined as a direct loop
    (see the module docstring for the rationale); the resulting behavior is
    byte-for-byte verified against the upstream oracle regardless.
  - `DataQualityIssues.scala`'s curated misspelling/alias tables plus the
    minimal error-reporting scaffolding from `ParseError.scala` /
    `ParseUtils.scala` (`ParseError`, `build_sub_error`) were ported into
    [`sportsdataverse/mbb/mbb_ncaa_data_quality.py`](sportsdataverse/mbb/mbb_ncaa_data_quality.py).
  - The name-resolution half of `LineupErrorAnalysisUtils.scala` (the
    `tidy_player` fallback chain, `NameFixer`'s fuzzy single-candidate
    scoring, and `fuzzy_box_match`) was ported into
    [`sportsdataverse/mbb/mbb_ncaa_names.py`](sportsdataverse/mbb/mbb_ncaa_names.py).
    The stint-**validation** half of the same Scala object --
    `ValidationError` / `ALLOWED_ERRORS` / `validate_lineup`
    (`LineupErrorAnalysisUtils.scala:18-26,181-218`), `BadLineupClump` /
    `clump_bad_lineups` / `categorize_bad_lineups` (`:223-263,617-633`), and
    the self-healing fixers `handle_common_sub_bug` / `find_missing_subs` /
    `add_missing_players` / `analyze_and_fix_clumps`
    (`:269-298,315-401,406-514,556-610`) -- was ported in Phase 5d into
    [`sportsdataverse/mbb/mbb_ncaa_stint_validation.py`](sportsdataverse/mbb/mbb_ncaa_stint_validation.py).
  - `ExtractorUtils.scala`'s player-code generator, team-name parser, the
    play-by-play event ADT, event reordering, and the substitution-tracking
    stint builder itself were ported into
    [`sportsdataverse/mbb/mbb_ncaa_stints.py`](sportsdataverse/mbb/mbb_ncaa_stints.py).
  - `LineupUtils.scala`'s raw-events -> `LineupEventStats` stat-tree
    population, scramble/transition tagging, assist pairing, the
    score-swap fixup, and per-player event splitting were ported into
    [`sportsdataverse/mbb/mbb_ncaa_lineup_enrich.py`](sportsdataverse/mbb/mbb_ncaa_lineup_enrich.py).
    `models/ncaa/PlayerEvent.scala`'s per-player event record (the one
    type this port's `create_player_events` returns) was additionally
    ported -- as an additive append, not touching any existing class --
    into
    [`sportsdataverse/mbb/mbb_ncaa_models.py`](sportsdataverse/mbb/mbb_ncaa_models.py).
  [`sportsdataverse/wbb/wbb_ncaa_models.py`](sportsdataverse/wbb/wbb_ncaa_models.py),
  [`sportsdataverse/wbb/wbb_ncaa_events.py`](sportsdataverse/wbb/wbb_ncaa_events.py),
  [`sportsdataverse/wbb/wbb_ncaa_possessions.py`](sportsdataverse/wbb/wbb_ncaa_possessions.py),
  [`sportsdataverse/wbb/wbb_ncaa_data_quality.py`](sportsdataverse/wbb/wbb_ncaa_data_quality.py),
  [`sportsdataverse/wbb/wbb_ncaa_names.py`](sportsdataverse/wbb/wbb_ncaa_names.py),
  [`sportsdataverse/wbb/wbb_ncaa_stints.py`](sportsdataverse/wbb/wbb_ncaa_stints.py),
  [`sportsdataverse/wbb/wbb_ncaa_lineup_enrich.py`](sportsdataverse/wbb/wbb_ncaa_lineup_enrich.py), and
  [`sportsdataverse/wbb/wbb_ncaa_stint_validation.py`](sportsdataverse/wbb/wbb_ncaa_stint_validation.py)
  re-export the same types and functions by reference (no separate copy of
  the logic). Unlike the cbb-on-off-analyzer (TypeScript/jest) entries above,
  no fixture file is vendored for this port -- every `utest` oracle value
  transliterated from `EventUtils.scala` / `PossessionUtils.scala` /
  `DataQualityIssues.scala` / `LineupErrorAnalysisUtils.scala` /
  `ExtractorUtils.scala` / `LineupUtils.scala` (and their `*Tests.scala`
  twins) is a short inline literal reproduced directly in the test modules
  (`tests/mbb/test_mbb_ncaa_models.py`, `tests/mbb/test_mbb_ncaa_events.py`,
  `tests/mbb/test_mbb_ncaa_possessions.py`,
  `tests/mbb/test_mbb_ncaa_data_quality.py`,
  `tests/mbb/test_mbb_ncaa_names.py`, `tests/mbb/test_mbb_ncaa_stints.py`,
  `tests/mbb/test_mbb_ncaa_lineup_enrich.py`,
  `tests/mbb/test_mbb_ncaa_stint_validation.py`), which are test-only and
  not shipped in the distributed wheel or sdist. **`test_mbb_ncaa_stint_validation.py`
  is only partly a transliteration.** Its `ValidationError`/`validate_lineup`
  cases are transliterated inline literals from the upstream oracle
  (`LineupErrorAnalysisUtilsTests.scala:55-120`), like the other test modules
  above -- but its `BadLineupClump`/`clump_bad_lineups`/`categorize_bad_lineups`
  and `handle_common_sub_bug`/`find_missing_subs`/`add_missing_players`/
  `analyze_and_fix_clumps` cases are validated against hand-derived fixtures:
  hand-built inputs with expected outputs hand-derived from the Scala
  algorithm, because the upstream ships no tests for those functions (the
  Scala's own doc comments read "TODO test"). Those cases are not
  transliterations of an existing suite.
  - **Phase 5e (HTML-parser layer + pbp/shot glue) additions.** The
    JSoup-selector-translation helpers, and `RosterParser.scala` /
    `BoxscoreParser.scala` / `PlayByPlayParser.scala` / `TeamIdParser.scala` /
    `TeamScheduleParser.scala` / `ShotEventParser.scala` /
    `PlayByPlayUtils.scala`, were ported into:
    - [`sportsdataverse/mbb/mbb_ncaa_html.py`](sportsdataverse/mbb/mbb_ncaa_html.py)
      (the shared JSoup->bs4 selector/text helper layer every parser below
      reuses -- has no dedicated upstream Scala file of its own).
    - [`sportsdataverse/mbb/mbb_ncaa_roster_parser.py`](sportsdataverse/mbb/mbb_ncaa_roster_parser.py)
      (`RosterParser.scala`; also the port of `models/ncaa/RosterEntry.scala`,
      appended to `mbb_ncaa_models.py`).
    - [`sportsdataverse/mbb/mbb_ncaa_boxscore_parser.py`](sportsdataverse/mbb/mbb_ncaa_boxscore_parser.py)
      (`BoxscoreParser.scala`; also closes the Phase 5b deferral of
      `DataQualityIssues.scala`'s `players_missing_from_boxscore` table,
      appended to `mbb_ncaa_data_quality.py`).
    - [`sportsdataverse/mbb/mbb_ncaa_pbp_parser.py`](sportsdataverse/mbb/mbb_ncaa_pbp_parser.py)
      (`PlayByPlayParser.scala`, including `create_lineup_data` -- the
      orchestrator that chains the Phase 5a-5d surface end to end).
    - [`sportsdataverse/mbb/mbb_ncaa_team_parsers.py`](sportsdataverse/mbb/mbb_ncaa_team_parsers.py)
      (`TeamIdParser.scala` + `TeamScheduleParser.scala`; also the port of
      `models/ConferenceId.scala`, appended to `mbb_ncaa_models.py`).
    - [`sportsdataverse/mbb/mbb_ncaa_shot_parser.py`](sportsdataverse/mbb/mbb_ncaa_shot_parser.py)
      (`ShotEventParser.scala`; also the port of `models/ncaa/ShotEvent.scala`
      -- `ShotLocation` / `ShotGeo` / `ShotEvent` / `CutdownShotEvent` --
      appended to `mbb_ncaa_models.py`).
    - [`sportsdataverse/mbb/mbb_ncaa_pbp_glue.py`](sportsdataverse/mbb/mbb_ncaa_pbp_glue.py)
      (`PlayByPlayUtils.scala`, including its `ShotEnrichmentUtils` companion
      object, flattened to module level per this port's established
      nested-object-flattening convention).

    [`sportsdataverse/wbb/wbb_ncaa_html.py`](sportsdataverse/wbb/wbb_ncaa_html.py),
    [`sportsdataverse/wbb/wbb_ncaa_roster_parser.py`](sportsdataverse/wbb/wbb_ncaa_roster_parser.py),
    [`sportsdataverse/wbb/wbb_ncaa_boxscore_parser.py`](sportsdataverse/wbb/wbb_ncaa_boxscore_parser.py),
    [`sportsdataverse/wbb/wbb_ncaa_pbp_parser.py`](sportsdataverse/wbb/wbb_ncaa_pbp_parser.py),
    [`sportsdataverse/wbb/wbb_ncaa_team_parsers.py`](sportsdataverse/wbb/wbb_ncaa_team_parsers.py),
    [`sportsdataverse/wbb/wbb_ncaa_shot_parser.py`](sportsdataverse/wbb/wbb_ncaa_shot_parser.py), and
    [`sportsdataverse/wbb/wbb_ncaa_pbp_glue.py`](sportsdataverse/wbb/wbb_ncaa_pbp_glue.py)
    re-export the same functions by reference (no separate copy of the
    logic); the Phase 5e model additions
    (`RosterEntry`/`ConferenceId`/`ShotLocation`/`ShotGeo`/`ShotEvent`/
    `CutdownShotEvent`) are re-exported by the existing
    `wbb_ncaa_models.py` shim.

    **Unlike Phase 5a-5d above, Phase 5e's oracle is a mix of vendored
    fixtures and inline literals -- both wordings apply, per module.** The 5
    HTML pages `RosterParserTests.scala` / `BoxscoreParserTests.scala` /
    `PlayByPlayParserTests.scala` / `TeamScheduleParserTests.scala` read from
    disk (`sample_roster.html`, `test_lineup.html`, `test_play_by_play.html`,
    `test_schedule.html`, `test_attendance_list.html`) are vendored
    byte-exact from the upstream clone's `src/test/resources/ncaa/` under
    [`tests/fixtures/ncaa/`](tests/fixtures/ncaa/) (see that directory's
    `README.md` for per-file provenance) -- these are test-only fixtures and
    are not shipped in the distributed wheel or sdist; `mbb_ncaa_roster_parser.py`,
    `mbb_ncaa_boxscore_parser.py`, `mbb_ncaa_pbp_parser.py`, and
    `mbb_ncaa_team_parsers.py`'s test modules
    (`tests/mbb/test_mbb_ncaa_roster_parser.py`,
    `tests/mbb/test_mbb_ncaa_boxscore_parser.py`,
    `tests/mbb/test_mbb_ncaa_pbp_parser.py`,
    `tests/mbb/test_mbb_ncaa_team_parsers.py`) read them as the end-to-end
    oracle. `TeamIdParserTests.scala`'s own fixture test is ported but marked
    skipped, mirroring its **upstream-disabled** state (disabled in the
    Scala source since 04/2021) rather than "fixing" it. By contrast,
    `ShotEventParserTests.scala` / `PlayByPlayUtilsTests.scala` (the Task
    5e.5/5e.6 oracles) ship no fixture of their own -- every case is a short
    inline literal, transliterated directly into
    `tests/mbb/test_mbb_ncaa_shot_parser.py` /
    `tests/mbb/test_mbb_ncaa_pbp_glue.py`, matching the inline-literal
    wording used for Phase 5a-5d above.
- **Modifications:** Translated from Scala to Python, following this
  repository's own conventions (typing, docstrings, dataclasses in place of
  case classes). No changes were made to the original Scala source itself;
  the Python port is a faithful (including bug-for-bug, where explicitly
  documented in the module docstrings) translation of the upstream logic,
  not a functional rewrite.

No modifications beyond the port itself are claimed against the upstream
project, and no upstream `NOTICE` file exists to reproduce (the upstream
repository does not ship one).
