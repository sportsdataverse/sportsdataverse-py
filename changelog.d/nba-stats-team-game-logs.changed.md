- **NBA:** `load_nba_stats_player_game_logs()` returns one row per player-game (`player_id`, `player_name`,
  `fantasy_pts` added): the `nba_stats_player_game_logs` assets held team rows until 2026-11-13. Those team rows
  moved to the new `load_nba_stats_team_game_logs()` (`nba_stats_team_game_logs`, 1996-97 onward). (#1185)
