- **ESPN schedules:** the six copies that sent `seasonType` now send `seasontype`. ESPN ignored the camelCase key, so
  `season_type=` with `dates=YYYY&week=N` (NFL, NHL, WNBA, NBA, MBB, WBB) never filtered; it does now, and those calls
  return different rows. (#855)
