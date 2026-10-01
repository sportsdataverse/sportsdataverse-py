<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CFB transfer-move roster fixture](#cfb-transfer-move-roster-fixture)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CFB transfer-move roster fixture

Real rows from the `espn_cfb_rosters` release (`load_cfb_rosters`), read
**2026-10-01**, backing
`tests/cfb/test_cfb_transfer_impact.py::test_transfer_moves_keys_rosters_by_espn_team_id`.
The test monkeypatches `load_cfb_rosters` to return these rows, so
`cfb_transfer_moves` runs **through** `_load_roster_keys` offline.

| File | Rows | Source | Columns |
|---|---:|---|---|
| `rosters_2023_2024.parquet` | 724 | `load_cfb_rosters(2023)` + `load_cfb_rosters(2024)`, filtered to `team_id` in {265, 2390, 38}, only the columns `_load_roster_keys` reads (+ `division`) | `season Int64, team_id Int64, athlete_id Int64, first_name String, last_name String, division String` |

Per team-season: 38 (Colorado) 115 / 111, 265 (Washington State) 127 / 122,
2390 (Miami) 125 / 124 rows for 2023 / 2024. No `(season, team_id, athlete_id)`
duplicates.

The one move inside the fixture is athlete **4688380** (Cam Ward): team 265 in
2023 -> team 2390 in 2024. ESPN athlete ids survive a transfer, which is what
makes the roster diff work.

The full release frame has 85 columns and **no `team` column**. That is the bug
this fixture guards: the old `_load_roster_keys` selected `pl.col("team")` and
raised `ColumnNotFoundError` on every live call.

Re-create (network):

```python
import polars as pl
from sportsdataverse.cfb import load_cfb_rosters

cols = ["season", "team_id", "athlete_id", "first_name", "last_name", "division"]
pl.concat(
    load_cfb_rosters(s).filter(pl.col("team_id").is_in([265, 2390, 38])).select(cols) for s in (2023, 2024)
).sort("season", "team_id", "athlete_id").write_parquet("tests/fixtures/cfb_transfer/rosters_2023_2024.parquet")
```
