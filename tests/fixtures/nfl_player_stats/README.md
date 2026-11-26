<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [nflverse `stats_player` weekly release slice](#nflverse-stats_player-weekly-release-slice)
  - [2024 fumbles-lost slice (captured 2026-11-26)](#2024-fumbles-lost-slice-captured-2026-11-26)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# nflverse `stats_player` weekly release slice

Captured 2026-09-20 from
<https://github.com/nflverse/nflverse-data/releases/download/stats_player/stats_player_week_2025.parquet>
(the release `load_nfl_player_stats` reads since nflverse froze
`player_stats/player_stats.parquet` in 2025-05).

| File | Rows | Notes |
|---|---:|---|
| `stats_player_week_2025_slice.parquet` | 14 | Season 2025, week 1, **raw upstream shape** (150 columns) |

The slice is deliberately un-reconciled so the tests exercise
`_player_stats_to_legacy` rather than a pre-baked answer. It is chosen to cover
every branch of that reconciliation:

- 4 QB rows with a non-zero `sack_yards_lost` — the negative-upstream column the
  loader negates into the legacy positive `sack_yards`.
- 4 WR/RB rows — the ordinary receiving/rushing path.
- 3 K rows with `fg_att > 0` — the `kicking=True` contract (44/44 legacy kicking
  columns survive in the weekly release).
- 3 defense-only rows (`def_tackles_solo > 0`, no offensive or kicking
  attempts) — dropped by the non-zero-stat row filter, because the legacy
  contract has no column to hold their stats.

Re-capture by downloading that asset and re-running the selection documented
above; no other file depends on the exact rows.

## 2024 fumbles-lost slice (captured 2026-11-26)

| File | Rows | Notes |
|---|---:|---|
| `stats_player_week_2024_fumbles_slice.parquet` | 45 | Season 2024, every regular-season week of three players, **raw upstream shape** (150 columns), from <https://github.com/nflverse/nflverse-data/releases/download/stats_player/stats_player_week_2024.parquet> |
| `rosters_2024_fumbles_slice.parquet` | 3 | Those three players' rows of `load_nfl_rosters([2024])`, as the loader returns them |

The players were chosen so each play type of fumble lost appears: Drake Maye (QB, `00-0039851`; 4 sack + 2
rushing), Josh Jacobs (RB, `00-0035700`; 3 rushing) and DK Metcalf (WR, `00-0035640`; 2 receiving). The
loader emits only those three split columns, never a single `fumbles_lost`; `tests/nfl/test_nfl_projection_fumbles.py`
checks the projection sums them, and that the sum reproduces the release's own `fantasy_points_ppr` every week.

Re-capture: read the asset, keep `season_type == "REG"` rows whose `player_id` is one of the three ids above,
sort by `player_id, week`; filter `load_nfl_rosters([2024])` on `gsis_id` for the same ids.

Dtypes: the stats slice keeps the release's own (`player_id` String, `season` Int32); the rosters slice is the
loader's shape (`gsis_id` String, `season` Int32, `birth_date` Date), which carries no `player_id` or `age` column.
