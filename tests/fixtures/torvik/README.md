<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Bart Torvik (T-Rank) fixtures](#bart-torvik-t-rank-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Bart Torvik (T-Rank) fixtures

Real captures from `barttorvik.com` (auth-free data files), captured
2026-08-07 and truncated to the header row + first 15 data rows.
The three headerless files (`*_getgamestats_head.json`, `*_getadvstats_head.csv`,
`*_super_sked_head.json`) were captured 2026-10-05 (full 2025 files: 11,528 /
5,060 / 6,293 rows, all with the documented field counts) and keep the first 15
rows (the game-stats capture also keeps the 2 rows whose `margin` is null). Used by
`tests/test_torvik_codegen.py`.

| File | URL |
|---|---|
| `2025_team_results_head.csv` | `https://barttorvik.com/2025_team_results.csv` (men's T-Rank ratings) |
| `2025_fffinal_head.csv` | `https://barttorvik.com/2025_fffinal.csv` (men's four factors) |
| `2025_getgamestats_head.json` | `https://barttorvik.com/getgamestats.php?year=2025&json=1` (men's per-team-game log; headerless JSON, 31 fields) |
| `2025_getadvstats_head.csv` | `https://barttorvik.com/getadvstats.php?year=2025&csv=1` (men's player advanced stats; headerless CSV, 67 fields) |
| `2025_super_sked_head.json` | `https://barttorvik.com/2025_super_sked.json` (men's season schedule/results; headerless JSON, 55 fields) |
| `ncaaw_2025_team_results_head.csv` | `https://barttorvik.com/ncaaw/2025_team_results.csv` (women's T-Rank ratings) |

Endpoint map + column schemas: `sdv-internal-refs/barttorvik/`
(`README.md`, `barttorvik.openapi.yaml`). The interactive `.php` pages
(`trank.php`, `team-history.php`, `teamsheets.php`, `resume-compare*.php`)
sit behind a JS browser challenge and are deliberately not wrapped.
