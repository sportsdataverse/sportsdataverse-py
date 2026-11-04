- **Basketball:** `nba_game_player_impact`, `wnba_game_player_impact`, `mbb_game_player_impact` and
  `wbb_game_player_impact` give the game impact box (per player and game: minutes, on-court
  possessions, +/- and ORtg/DRtg/net with `_n`), and `*_game_lineups_used` the five-man units of
  each game. Points are credited per scoring action, free throws to the lineup at the foul: the
  +/- equals stats.nba.com's box score on 91% of 2025-26 NBA and 98.5% of 2026 WNBA player-games,
  the rest being on-court lineup errors in the source (BB-D6). (#1103)
