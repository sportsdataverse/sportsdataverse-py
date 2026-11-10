- **NBA/WNBA/MBB/WBB:** `{nba,wnba,mbb,wbb}_player_shot_zones` and `_team_shot_zones` give each season's shot profile
  by zone (rim, mid, corner 3, above-the-break 3): attempts, makes, field-goal percentage, share of attempts and assisted
  share, for each player and for each team and its opponents, with `_n`, `_rank` and `_pct`. Ranks need a split-half
  sample gate fitted by `tools/fit_shot_zone_gates.py`. A three-point make rate never reaches it within one season, so it
  is not ranked. `add_shot_zones` gains `corner_min_abs_x` (default unchanged). (BB-D35, #1159)
