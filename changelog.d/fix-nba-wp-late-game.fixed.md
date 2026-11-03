- **NBA / WNBA in-game WP:** the refit boosters converge as the clock runs out at a fixed lead (they were flat from
  60 s to 0.1 s, so `score_win_prob` jumped 30-60 points at the horn); `in_game_features` adds `margin_per_sqrt_sec`
  and `sqrt_sec_left_60`. Holdout Brier NBA 2026 0.152 -> 0.145, WNBA 2025 0.190 -> 0.165. (#1075)
