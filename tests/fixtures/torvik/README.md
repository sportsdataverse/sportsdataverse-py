<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Bart Torvik (T-Rank) fixtures](#bart-torvik-t-rank-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Bart Torvik (T-Rank) fixtures

Real captures from `barttorvik.com` (auth-free data files), captured
2026-08-07 and truncated to the header row + first 15 data rows. Used by
`tests/test_torvik_codegen.py`.

| File | URL |
|---|---|
| `2025_team_results_head.csv` | `https://barttorvik.com/2025_team_results.csv` (men's T-Rank ratings) |
| `2025_fffinal_head.csv` | `https://barttorvik.com/2025_fffinal.csv` (men's four factors) |
| `ncaaw_2025_team_results_head.csv` | `https://barttorvik.com/ncaaw/2025_team_results.csv` (women's T-Rank ratings) |

Endpoint map + column schemas: `sdv-internal-refs/barttorvik/`
(`README.md`, `barttorvik.openapi.yaml`). The interactive `.php` pages
(`trank.php`, `team-history.php`, `teamsheets.php`, `resume-compare*.php`)
sit behind a JS browser challenge and are deliberately not wrapped.
