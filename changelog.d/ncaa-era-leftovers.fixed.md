- **MBB / WBB:** `parse_ncaa_bb_game_pbp` on a 2018-format page and `parse_ncaa_bb_team_schedule` on the
  older team page (2016) return the zero-row frame with an `errors.UnsupportedPageFormatWarning` naming the
  format; they returned 0 rows silently, or crashed / mis-paired half the games. (#853)
- **MBB / WBB:** `ncaa_mbb_play_by_play` / `ncaa_wbb_play_by_play` raise an `UnsupportedPageFormatWarning` the
  caller escalated to an error; they swallowed it, re-fetched the page and returned 0 rows. (#853)
- **MBB / WBB:** the stats.ncaa.org pbp / shots / box drivers turn a float contest id into `"6470186"`, never
  `contests/6470186.0/`; box-score failures are logged, not printed; one owner for the WBB period model. (#853)
- **WBB:** `ncaa_wbb_game_pbp` / `ncaa_wbb_shot_locations` read `season=2015.0` as 2015 (halves); it became
  `"2015.0"`, bound the quarters model and a halves-era game returned 0 rows. (#853)
