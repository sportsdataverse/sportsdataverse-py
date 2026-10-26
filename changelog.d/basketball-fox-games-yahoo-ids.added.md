- **NBA / WNBA / MBB / WBB crosswalks:** `{nba,wnba,mbb,wbb}_schedule_crosswalk(fox=True)` fills `fox_game_id` (and the
  NBA / WNBA `fox_home_team_id` / `fox_away_team_id`) from Fox's per-date scoreboard, matched on US-Eastern date and
  teams in either home/away order; a late tip ESPN files at 23:59 ET matches Fox's 00:00 ET a day apart. (#793)
- **NBA / WNBA crosswalks:** `nba_crosswalks_from_archive` / `wnba_crosswalks_from_archive(fox_games=True)` fill the
  Fox game and team ids for any season Fox serves. (#793)
- **WNBA / MBB / WBB crosswalks:** `yahoo=True` on the team and schedule crosswalks fills `yahoo_team_id` (Yahoo's
  listing joined to ESPN by full name, short name, then nickname where school names agree; unmatched teams are logged).
  (#793)
- **WNBA / MBB / WBB crosswalks:** `yahoo_game_id` = `{lg}.g.{YYYYMMDD ET}{home Yahoo id}`, checked against the ids
  Yahoo lists so a neutral-site game takes Yahoo's home side. (#793)
- **Basketball crosswalks:** the Fox and Yahoo legs are opt-in and fail-soft: any Fox or Yahoo failure logs a warning
  and leaves the columns null. (#793)
