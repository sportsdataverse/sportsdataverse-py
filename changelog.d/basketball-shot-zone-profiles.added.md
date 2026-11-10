- **NBA/WNBA/MBB/WBB:** `{nba,wnba,mbb,wbb}_player_shot_zones` and `_team_shot_zones` give each season's shot profile
  by zone (rim, mid, corner 3, above the break): attempts, makes, FG%, share of attempts and assisted share, for each
  player and each team and its opponents, with `_n` / `_rank` / `_pct`. (#1159)
- **NBA/WNBA/MBB/WBB:** the zone ranks need a split-half sample gate fitted by `tools/fit_shot_zone_gates.py`; a
  three-point make rate never reaches it within a season, so it is not ranked. (#1159)
- **NBA:** `add_shot_zones` gains `corner_min_abs_x` (default unchanged). (#1159)
