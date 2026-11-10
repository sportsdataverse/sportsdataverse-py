- **NBA / WNBA / MBB / WBB:** `nba_game_lines.game_lines` (BB-D33) joins the odds store to ESPN games: opening and
  closing spread, total and moneylines, the no-vig market win probability, and the result against both. (#1163)
- **NBA / WNBA / MBB:** `nba_game_lines.player_props` gives the closing player lines with ESPN athlete ids, and
  `wp_variants` a naive (0.5 prior) and a market-prior per-play home WP from `score_win_prob` (all four). (#1163)
