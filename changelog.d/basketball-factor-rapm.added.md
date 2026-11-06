- **Basketball:** `{nba,wnba}_player_factor_rapm` split each player-season's plain RAPM into Dean Oliver's four factors,
  offense and defense (shooting, turnovers, rebounding, free throws) plus a bookkeeping remainder, with ranks and
  game-clustered SEs; the parts add up to `o_rapm` and `d_rapm` up to floating-point error (BB-D10). (#1117)
