<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [hoop-explorer oracle fixtures](#hoop-explorer-oracle-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# hoop-explorer oracle fixtures

Vendored from Alex-At-Home/cbb-on-off-analyzer (hoop-explorer.com SPA), local
clone `GitHub-Data/cbb-on-off-analyzer` @ `0252725cd94bf54dd5384d0d9af3f2382367c057`
on 2026-07-03, via `tools/vendor_hoop_explorer_fixtures.py`.

- `lineup_utils_snap.json` — jest snapshots of LineupUtils (aggregation,
  on/off reports). Oracle for `sportsdataverse/mbb/mbb_lineup_stats.py`.
- `rating_utils_snap.json` / `luck_utils_snap.json` / `rapm_utils_snap.json`
  — oracles for phases 2-3.

Parse rate (jest snapshot entries -> JSON via `json5`, `undefined` folded to
`null`): 15/15 (LineupUtils), 7/7 (RatingUtils), 4/4 (LuckUtils), 2/2
(RapmUtils) — 100% across all four files (well above the >=80% acceptance
bar; no entries fell back to raw-string).

NOTE: the upstream repo's `LICENSE` file is **Apache License 2.0**, not MIT
as originally assumed when this vendoring task was scoped — verify
attribution/compatibility against Apache-2.0 (not MIT) before release
packaging; fixtures are test-only and not shipped in the wheel regardless.
