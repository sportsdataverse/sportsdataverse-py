- **Basketball:** `mbb_team_strength` / `wbb_team_strength` add `adj_off_rtg_ridge` / `adj_def_rtg_ridge` /
  `adj_net_rtg_ridge` (with `_rank` / `_pct` / `_n`): the NBA / WNBA possession ridge on the stats.ncaa.org
  possessions, beside the unchanged fixed-point `adj_*_rtg`, which SOS, SOV and bracketology keep. (#1127)
- **Ratings:** `opponent_adjusted_ridge(weight_col=)` fits collapsed rows; `possession_ridge_ratings` uses it (NBA /
  WNBA output unchanged), and `tools/compare_college_team_strength.py` compares the two college methods. (#1127)
