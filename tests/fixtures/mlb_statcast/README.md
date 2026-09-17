# MLB Statcast (Baseball Savant) fixtures

| File | Source | Notes |
|---|---|---|
| `search_2024-06-15_head.csv` | `GET https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details&player_type=batter&game_date_gt=2024-06-15&game_date_lt=2024-06-15` (the request `mlb_statcast_search("2024-06-15", "2024-06-15")` sends), captured 2026-09-17 | The header plus the first 46 data rows of the real 4,145-row response, byte-for-byte (UTF-8 BOM and Savant's quoting kept). Real 119-column header. Rows span 7 games and include empty-base pitches (blank `on_1b`/`on_2b`/`on_3b`) and a bases-loaded pitch (game 745329, AB 44), so the runner-id columns carry both blanks and ids. Guards the MLBAM id columns parsing as `Int64`, not `Float64`. |

Regenerate by re-running that request and keeping the first 47 lines; do not hand-edit.
The other files in this directory predate this README and are not documented here.
