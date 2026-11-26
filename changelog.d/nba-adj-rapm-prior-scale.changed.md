- **NBA/WNBA:** `nba_adj_rapm` scales its SPM prior by 1.5 (`ADJ_RAPM_PRIOR_SCALE`, was 2), refit for the
  minute-weighted SPM by `tools/fit_adj_rapm_penalty.py`; the penalties are unchanged. Every season's
  `player_impact` `spm` and `adj_rapm` move with it. (#1309)
