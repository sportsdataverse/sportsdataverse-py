- **MBB / WBB in-game WP:** refit on the NBA / WNBA contract, so it converges as the clock runs out (a 1-point
  lead read 0.68 at 0.1 s); `in_game_features` adds `margin_per_sqrt_sec` and `sqrt_sec_left_60`. Holdout 2026
  Brier MBB 0.1332 -> 0.1327, WBB 0.1212 -> 0.1204; fit by `tools/models/mbb_prediction/train_in_game_wp.py`. (#PR)
