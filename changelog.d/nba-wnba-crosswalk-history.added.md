- **NBA / WNBA:** `nba_crosswalks_from_archive()` / `wnba_crosswalks_from_archive()` build a past season's team,
  schedule and player crosswalks offline from the ESPN schedule and game-roster parquet plus the local stats box-score
  archive; the live builders only reach the current season. (#773)
- **NBA:** the team and schedule crosswalks now compute `yahoo_team_id` (`nba.t.{espn id}`) and `yahoo_game_id`
  (`nba.g.{YYYYMMDD ET}{home id}`) from 2014 on, where they were null. (#773)
