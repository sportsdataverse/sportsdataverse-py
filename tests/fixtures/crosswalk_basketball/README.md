<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Basketball crosswalk golden fixtures](#basketball-crosswalk-golden-fixtures)
  - [Re-capturing](#re-capturing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Basketball crosswalk golden fixtures

These are the **R producers' committed outputs**, copied unmodified. They are
the oracle for `tests/test_basketball_crosswalk_parity.py`, which asserts the
ported Python assemblers reproduce them row-for-row (design section 9.3: "a
crosswalk that is 97% right is a silent data defect, not a passing port").

| Fixture | Source repo | Path in that repo | Rows |
|---|---|---|---:|
| `mbb_team_crosswalk_2026.parquet` | `hoopR-mbb-data` | `mbb/crosswalk/parquet/` | 362 |
| `mbb_player_crosswalk_2026.parquet` | `hoopR-mbb-data` | `mbb/crosswalk/parquet/` | 5,442 |
| `nba_team_crosswalk_2026.parquet` | `hoopR-nba-data` | `nba/crosswalk/parquet/` | 30 |
| `nba_player_crosswalk_2026.parquet` | `hoopR-nba-data` | `nba/crosswalk/parquet/` | 553 |
| `wbb_team_crosswalk_2026.parquet` | `wehoop-wbb-data` | `wbb/crosswalk/parquet/` | 361 |
| `wbb_schedule_crosswalk_2026.parquet` | `wehoop-wbb-data` | `wbb/crosswalk/parquet/` | 6,521 |
| `wbb_player_crosswalk_2026.parquet` | `wehoop-wbb-data` | `wbb/crosswalk/parquet/` | 5,018 |
| `wnba_team_crosswalk_2026.parquet` | `wehoop-wnba-data` | `wnba/crosswalk/parquet/` | 15 |
| `wnba_schedule_crosswalk_2026.parquet` | `wehoop-wnba-data` | `wnba/crosswalk/parquet/` | 1,416 |
| `wnba_player_crosswalk_2026.parquet` | `wehoop-wnba-data` | `wnba/crosswalk/parquet/` | 168 |

Produced by `hoopR::{mbb,nba}_*_crosswalk()` / `wehoop::{wbb,wnba}_*_crosswalk()`
via each repo's `<lg>_1{1,2,3}_*_crosswalk_creation.R` stage, 2026 season.

`hoopR-nba-data` also carries an **untracked, in-progress**
`nba_schedule_crosswalk` CSV/parquet; it is deliberately excluded — only
committed R output is used as an oracle.

Note the `<lg>/crosswalk/*.csv` files in those repos are per-season **manifests**
(`season,row_count,generated_at_utc,source_endpoint`), not the crosswalk data.
The parquets above are the actual outputs.

## Re-capturing

Copy the file again from the sibling checkout; do not edit in place. If the R
producer's schema changes, update the parity test in the same commit and say
which columns moved.
