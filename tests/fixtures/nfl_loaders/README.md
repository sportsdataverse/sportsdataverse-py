<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [`nfl_loaders` fixtures](#nfl_loaders-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# `nfl_loaders` fixtures

Real (not synthetic) slices of the upstream files that
`sportsdataverse/nfl/nfl_loaders.py` reads -- nflverse release parquets and
DynastyProcess CSVs -- captured so the schema-drift and id-dtype regression
tests can run offline.

| File | Provenance | Captured |
|---|---|---|
| `pbp_participation_2016_head3.parquet` | `pl.scan_parquet(NFL_PBP_PARTICIPATION_URL.format(season=2016)).head(3)` — `github.com/nflverse/nflverse-data` releases, `pbp_participation/pbp_participation_2016.parquet` | 2026-08-11 |
| `pbp_participation_2023_head3.parquet` | same, `season=2023` | 2026-08-11 |
| `players_crosswalk_slice.parquet` | `load_nfl_players()` (nflverse `players/players.parquet`) filtered to gsis ids `00-0033873`, `00-0038124`, `00-0039406`, `00-0022888`, `00-0039808` | 2026-09-17 |
| `ff_playerids_crosswalk_slice.parquet` | `load_nfl_ff_playerids()` (DynastyProcess `db_playerids.csv`) filtered to the same gsis ids. `00-0022888` appears twice upstream (Jake Schum and Bobby McCray); `00-0039808` is absent | 2026-09-17 |
| `db_playerids_slice.csv` | header + 6 verbatim data lines (0-based data rows 0, 634, 5545, 7667, 3032, 11014, in that order) of `NFL_FF_PLAYERIDS_URL` — `github.com/dynastyprocess/data/raw/master/files/db_playerids.csv` | 2026-09-17 |
| `db_fpecr_latest_slice.csv` | header + 3 verbatim data lines (0-based data rows 211, 505, 560) of `NFL_FF_RANKINGS_DRAFT_URL` — `.../files/db_fpecr_latest.csv` | 2026-09-17 |
| `fp_latest_weekly_slice.csv` | header + 2 verbatim data lines (0-based data rows 0, 550) of `NFL_FF_RANKINGS_WEEK_URL` — `.../files/fp_latest_weekly.csv` | 2026-09-17 |

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

The two `*_crosswalk_slice.parquet` files back `nfl_players_crosswalk`'s
`yahoo_id` / `cbs_id` join test (`tests/nfl/test_nfl_players.py`).
The three DynastyProcess slices back the `load_nfl_ff_playerids` /
`load_nfl_ff_rankings` id-dtype pin tests.

- `db_playerids_slice.csv`: every id column pinned `Utf8` is populated with
  purely numeric text somewhere in the slice (`yahoo_id`, `fleaflicker_id`,
  `rotoworld_id` and `swish_id` are null in the first 100 lines upstream, so
  the head alone would not exercise them). Anthony Smith carries `nfl_id`
  `038666` and James Allen carries `mfl_id` `0156` (one of 225 zero-padded MFL
  ids); both leading zeros only survive a `Utf8` read. Josh Allen
  (`fantasypros_id` `17298`) and Fernando Mendoza (`28013`) are the join
  targets for the rankings slices.
- `db_fpecr_latest_slice.csv`: a DST row plus the Josh Allen and Fernando
  Mendoza superflex rows, so `id` (the FantasyPros id) infers `Int64` and
  matches two playerids rows. `sportsdata_id`, `yahoo_id` and `cbs_id` are
  null in every row of the live capture, so no real row exercises them.
- `fp_latest_weekly_slice.csv`: the Josh Allen QB row and a DST row;
  `fantasypros_id` infers `Int64` and matches one playerids row.
