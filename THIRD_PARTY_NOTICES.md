<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Third-Party Notices](#third-party-notices)
  - [cbb-on-off-analyzer (`LineupUtils.ts`)](#cbb-on-off-analyzer-lineuputilsts)
  - [cbb-on-off-analyzer (`RatingUtils.ts`)](#cbb-on-off-analyzer-ratingutilsts)
  - [cbb-on-off-analyzer (`LuckUtils.ts`)](#cbb-on-off-analyzer-luckutilsts)

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
  same module; this entry covers the module as a whole. The jest snapshot
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
- **What was derived:** `src/utils/stats/LuckUtils.ts`'s offensive 3P
  luck-adjustment surface (`calcOffTeamLuckAdj`, `calcOffPlayerLuckAdj`,
  `build3PShotInfo`, `buildAdjusted3P`, `buildExp3P`, the generalized
  `buildShotInfo` / `buildAdjustedFG` they wrap, and the `affectedFieldSet`
  constant) was ported line-for-line into
  [`sportsdataverse/mbb/mbb_luck.py`](sportsdataverse/mbb/mbb_luck.py). The
  defensive counterpart (`calcDefTeamLuckAdj`, `calcDefPlayerLuckAdj`) and
  `injectLuck` are being ported incrementally into the same module; this
  entry covers the module as a whole. The jest snapshot fixtures and their
  input literals used as an offline correctness oracle are vendored under
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

No modifications beyond the port itself are claimed against the upstream
project, and no upstream `NOTICE` file exists to reproduce (the upstream
repository does not ship one).
