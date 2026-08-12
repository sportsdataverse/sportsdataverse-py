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
