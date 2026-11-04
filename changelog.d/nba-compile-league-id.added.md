- **NBA season compile:** `compile_nba_season(..., league_id=)` takes the stats `LeagueID` (`"00"` NBA, the
  default; `"10"` WNBA; `"20"` G League) and sends it to game discovery and to every per-game fetch. For the
  WNBA the season is the calendar year and discovery reads `wnba_stats_leaguegamelog`. (#1090)
