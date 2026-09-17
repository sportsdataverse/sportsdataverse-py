<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [`nfl_loaders` fixtures](#nfl_loaders-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# `nfl_loaders` fixtures

Real (not synthetic) slices of the nflverse release parquets that
`sportsdataverse/nfl/nfl_loaders.py` reads, captured so the multi-season
schema-drift regression test can run offline.

| File | Provenance | Captured |
|---|---|---|
| `pbp_participation_2016_head3.parquet` | `pl.scan_parquet(NFL_PBP_PARTICIPATION_URL.format(season=2016)).head(3)` — `github.com/nflverse/nflverse-data` releases, `pbp_participation/pbp_participation_2016.parquet` | 2026-08-11 |
| `pbp_participation_2023_head3.parquet` | same, `season=2023` | 2026-08-11 |
| `db_playerids_slice.csv` | header + 4 verbatim data lines (0-based data rows 0, 634, 5545, 7667) of `NFL_FF_PLAYERIDS_URL` — `github.com/dynastyprocess/data/raw/master/files/db_playerids.csv` | 2026-09-17 |

Why this pair: the two seasons of the same release dataset differ in **both**
ways a `pl.concat(..., how="vertical")` cannot survive.

- **Column set** — 2016 ships 20 columns, 2023 ships 26. The six added in
  2023 are `offense_names`, `defense_names`, `offense_positions`,
  `defense_positions`, `offense_numbers`, `defense_numbers`.
- **Join-key dtype** — `play_id` is `Int32` through 2022 and `Float64` from
  2023 on. `load_nfl_pbp` ships `play_id` as `Float64`, so the loader pins the
  column to `Float64` at the read boundary instead of letting the
  `diagonal_relaxed` supertype rule decide (which would make the dtype depend
  on which seasons happen to be in the requested span).

Only the first three rows of each season are kept — the test asserts on the
schema union and the null-fill, never on values.

`db_playerids_slice.csv` backs the `load_nfl_ff_playerids` id-dtype pin test.
The rows were picked so every id column the documented schema types as
`character` is populated with purely numeric text somewhere in the slice
(`yahoo_id`, `fleaflicker_id`, `rotoworld_id` and `swish_id` are null in the
first 100 lines upstream, so the head alone would not exercise them), and the
last row carries `nfl_id` `038666`, whose leading zero only survives a `Utf8`
read.
