<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [nflverse `stats_player` weekly release slice](#nflverse-stats_player-weekly-release-slice)

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
