- **NBA / WNBA stats:** `nba_stats_leagueseasonmatchups()` and `wnba_stats_leagueseasonmatchups()`
  return the league's matchups again; their team filters defaulted to `"0"`, which this endpoint
  reads as a team and answered with 0 rows. They now default to blank. (#1244)
