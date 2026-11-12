- **NHL:** `nhl_predict_games`, `predict_total` and `nhl_game_total` no longer run about 2 goals low. The
  even-strength raw total was stretched around `avg_total_goals` (6.05) instead of its own mean, `2 * avg_xgf`
  (5.10). Walk-forward holdout bias goes from -2.07 / -1.88 / -2.13 to -0.25 / -0.07 / -0.31 goals for
  2023-24 / 2024-25 / 2025-26, and MAE from 2.49 to 1.87. PWHL totals move by +0.6 goals for the same reason.
