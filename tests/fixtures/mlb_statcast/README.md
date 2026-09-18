# MLB Statcast (Baseball Savant) fixtures

| File | Source | Notes |
|---|---|---|
| `search_2024-06-15_head.csv` | `GET https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details&player_type=batter&game_date_gt=2024-06-15&game_date_lt=2024-06-15` (the request `mlb_statcast_search("2024-06-15", "2024-06-15")` sends), captured 2026-09-17 | The header plus the first 46 data rows of the real 4,145-row response, byte-for-byte (UTF-8 BOM and Savant's quoting kept). Real 119-column header. Rows span 7 games and include empty-base pitches (blank `on_1b`/`on_2b`/`on_3b`) and a bases-loaded pitch (game 745329, AB 44), so the runner-id columns carry both blanks and ids. Guards the MLBAM id columns parsing as `Int64`, not `Float64`. |
| `search_minors_2024-06-01_head.csv` | `GET https://baseballsavant.mlb.com/statcast-search-minors/csv?all=true&type=details&player_type=batter&game_date_gt=2024-06-01&game_date_lt=2024-06-01&minors=true&wbc=false` (the request `mlb_statcast_search_minors("2024-06-01", "2024-06-01")` sends), captured 2026-09-17 | Header plus the first 5 data rows of the real 6,310-row response, byte-for-byte. MiLB affiliates (ROC/STP, NOR/GWN); without `minors=true` the same route returned 4,398 rows of MLB games. |
| `search_wbc_2023-03-11_head.csv` | `GET https://baseballsavant.mlb.com/statcast-search-world-baseball-classic/csv?all=true&type=details&player_type=batter&game_date_gt=2023-03-11&game_date_lt=2023-03-11&minors=false&wbc=true` (the request `mlb_statcast_search_wbc("2023-03-11", "2023-03-11")` sends), captured 2026-09-17 | Header plus the first 5 data rows of the real 2,288-row response, byte-for-byte. WBC pool play (JPN vs CZE, `game_type` F); without `wbc=true` the same route returned 3,198 rows of MLB spring training (`game_type` S). |

Regenerate by re-running each request and keeping the first 47 (search) / 6 (minors, WBC) lines; do not hand-edit.
The other files in this directory predate this README and are not documented here.
