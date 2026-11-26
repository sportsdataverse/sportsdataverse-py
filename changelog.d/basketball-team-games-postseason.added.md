- **NBA/WNBA:** `nba_team_game_possessions` / `wnba_team_game_possessions` return a `season_type` column on every call
  and take a list of season types (`["regular-season", "playoffs"]`), reading the releases once. (#1323)
