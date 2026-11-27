<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [reep team fixtures](#reep-team-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# reep team fixtures

Real rows of the reep football id register (CC0 1.0), release `data_version` 2026.25
(generated 2026-06-21), from `data/teams.csv` and `data/names.csv` of
<https://github.com/withqwerty/reep> at commit `0ec59fa`. Rows are copied verbatim, not edited.

Kept: every reep team whose normalized name or alias equals a normalized name of a team in
`tests/fixtures/espn/soccer/{eng.1,usa.1}/site-v2/teams.json` (in any country, so the
out-of-country namesakes the country filter must drop are present), every team the bundled
crosswalk maps those ESPN teams to, and every team carrying a `key_espn`. With that set,
`match()` in `tools/build_soccer_espn_reep_crosswalk.py` gives the same answer for those ESPN
teams as it does on the full register, which `tests/soccer/test_soccer_reep_crosswalk.py` asserts.

Used by `tests/soccer/test_soccer_reep_crosswalk.py`.
