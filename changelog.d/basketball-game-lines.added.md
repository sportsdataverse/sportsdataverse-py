- **NBA / WNBA / MBB / WBB:** `nba_game_lines` (BB-D33) joins the odds store to ESPN games: `game_lines` gives the
  opening and closing spread, total and moneylines, the no-vig market win probability and the result against the
  spread and total; `player_props` the closing player lines with athlete ids (NBA, WNBA, MBB); `wp_variants` a naive
  (0.5 prior) and a market-prior per-play home WP from `score_win_prob`. (#1163)
