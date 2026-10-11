- **NBA / WNBA / MBB / WBB crosswalks:** `{nba,wnba,mbb,wbb}_schedule_crosswalk(fox=True)` fills
  `fox_game_id` (and the NBA/WNBA `fox_home_team_id` / `fox_away_team_id`) from Fox's per-date
  scoreboard, matched on US-Eastern date and teams in either home/away order (a late West-coast tip
  ESPN files at 23:59 ET matches Fox's 00:00 ET a day apart); `nba_crosswalks_from_archive` /
  `wnba_crosswalks_from_archive(fox_games=True)` fill them for any season Fox serves. `yahoo=True` on the
  WNBA / MBB / WBB team and schedule crosswalks fills `yahoo_team_id` (Yahoo's listing joined to ESPN
  by full name, short name, then nickname where the school names agree; unmatched Yahoo teams are logged) and
  `yahoo_game_id` = `{lg}.g.{YYYYMMDD ET}{home Yahoo id}`, checked against the ids Yahoo lists so a
  neutral-site game takes Yahoo's home side. Both legs are opt-in and fail-soft: any Fox or Yahoo failure logs a
  warning and leaves the columns null.
