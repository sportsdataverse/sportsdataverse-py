- **NFL:** `nfl_adjusted_epa_ledger` lays out a team's season adjusted EPA game by game (raw net, schedule term, weight,
  contribution and leave-one-game-out influence) from nflverse or `NFLPlayProcess` frames; on a 2014-2023 walk-forward
  it beat the `method="pre598"` fit nfl-data uses by 0.088 points of margin MAE, 95% CI [0.015, 0.170]. (#1372)
