- **WBB:** `period_and_sec_left` (the NCAA shots' `period` / `sec_left`) reads `season=2015.0` as 2015
  (halves); it became `"2015.0"` and labelled a halves-era shot with the quarters model. (#887)
- **WBB:** `ncaa_wbb_game_pbp` / `ncaa_wbb_play_by_play` infer the era from the page for a NaN or blank `season`,
  as for `None`; they bound the quarters model and a halves-era game returned 0 rows. (#887)
- **MBB / WBB:** `ncaa_mbb_play_by_play` / `ncaa_wbb_play_by_play` fetch a page the format guard rejects once and
  warn once; they re-fetched it as an empty scrape, a second stats.ncaa.org request and a second warning. (#887)
