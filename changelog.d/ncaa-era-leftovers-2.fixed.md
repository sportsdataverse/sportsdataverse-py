- **WBB:** `period_and_sec_left` (the NCAA shots' `period` / `sec_left`) reads `season=2015.0` as 2015
  (halves); it became `"2015.0"` and labelled a halves-era shot with the quarters model.
- **WBB:** `ncaa_wbb_game_pbp` / `ncaa_wbb_play_by_play` infer the era from the page for a NaN or blank `season`,
  as for `None`; they bound the quarters model and a halves-era game returned 0 rows.
