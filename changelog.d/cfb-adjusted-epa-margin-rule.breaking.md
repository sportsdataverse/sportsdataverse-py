- **CFB:** `cfb_adjusted_epa` / `_by_game`, `efficiency_ratings` and `cfb_ratings` leave garbage time (score-margin
  rule) out of both the opponent fit and a team's own EPA; they now need `period` and `pos_score_diff_start`,
  `net_adj_epa` / `adj_net` widen, and `CFB_CONSTANTS` are refit again. (#1300)
