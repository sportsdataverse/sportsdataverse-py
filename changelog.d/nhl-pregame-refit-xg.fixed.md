- **NHL:** pregame constants refitted on the retrained xG (`avg_xgf` 2.55 -> 2.00), with `hfa` and `margin_sd`
  fitted jointly: mean home win probability is 0.52, was 0.57 against 0.52 actual. The in-game WP model is
  retrained on the new pregame anchors. (#1200)
