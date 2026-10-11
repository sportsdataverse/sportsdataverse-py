- **NBA / WNBA crosswalks:** `nba_crosswalks_from_archive` / `wnba_crosswalks_from_archive` take `fox=True`:
  `fox_team_id` by team name for any season, and the Fox player columns for the current season
  only (Fox rosters are today's). A failed Fox fetch warns and leaves them null. (#786)
