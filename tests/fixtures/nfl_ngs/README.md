<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NFL Next Gen Stats fixtures](#nfl-next-gen-stats-fixtures)
  - [Provenance](#provenance)
  - [Files](#files)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NFL Next Gen Stats fixtures

Committed real-data slices used by the offline NGS over-expected oracle tests
(`tests/nfl/test_nfl_ngs_oracle.py` and friends).

## Provenance

- **Loader:** `sportsdataverse.nfl.load_nfl_nextgen_stats` (nflverse
  `nextgen_stats` release parquet, mirrored via `sportsdataverse-data`).
- **Capture date:** 2026-07-08 (script: `dev/nfl_ngs/capture_fixtures.py`).
- **Seasons:** 2022 and 2023.
- **Grain:** the `*_2022_2023.parquet` files carry season-level rows only
  (`week == 0`); NGS season rows include only qualified players, so counts
  are modest (receiving 122 + 115 rows, rushing 48 + 49 rows). The
  `*_weekly_2022_2023.parquet` files carry the weekly rows (`week > 0`,
  receiving 2702, rushing 1143) — they identify the sampling variance
  `sigma2` for the empirical-Bayes prior via within-player across-week
  variation; the season-only panel cannot identify it (all qualified
  rushers carry similar attempt counts, so the tau2/sigma2 OLS collapses).
- **Dtypes pinned at capture:** `player_gsis_id` → `Utf8`, `season` → `Int64`.
- All contract columns were present upstream at capture time — nothing was
  dropped from the planned column contract.

## Files

| File | `stat_type` | Grain | Columns |
|---|---|---|---|
| `ngs_receiving_2022_2023.parquet` | `receiving` | season (`week == 0`) | season, week, player_gsis_id, player_display_name, player_position, team_abbr, avg_cushion, avg_separation, avg_intended_air_yards, receptions, targets, avg_yac, avg_expected_yac, avg_yac_above_expectation |
| `ngs_rushing_2022_2023.parquet` | `rushing` | season (`week == 0`) | season, week, player_gsis_id, player_display_name, player_position, team_abbr, rush_attempts, rush_yards, expected_rush_yards, rush_yards_over_expected, rush_yards_over_expected_per_att, percent_attempts_gte_eight_defenders |
| `ngs_receiving_weekly_2022_2023.parquet` | `receiving` | weekly (`week > 0`) | same as receiving above |
| `ngs_rushing_weekly_2022_2023.parquet` | `rushing` | weekly (`week > 0`) | same as rushing above |
