- **Prediction markets:** `join_espn_kalshi` and `join_espn_polymarket` join a parsed ESPN scoreboard to
  Kalshi winner markets or Polymarket Gamma events (nba, nfl, nhl) and say how in `matched_by`.
  `wexp.market.fair_win_probs` returns null with a reason when a draw is possible but unpriced. (#PR)
