- **NBA / WNBA:** `load_nba_stats_{pbp,possessions,lineups}_v3` and the four `load_wnba_stats_*` shims
  (`lineups`, `player_season_stats`, `standings`, `team_season_stats`) now say they will be removed in
  0.2.0; they named 0.1.0, which has shipped. A test fails if a removal version falls behind again. (#726)
