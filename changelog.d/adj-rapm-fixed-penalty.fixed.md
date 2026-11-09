- **NBA/WNBA:** `nba_adj_rapm` fits one ridge penalty per league (`ADJ_RAPM_PENALTY`, NBA 3,728, WNBA 2,683) toward
  the SPM prior scaled by 2 (`ADJ_RAPM_PRIOR_SCALE`), both fitted by `tools/fit_adj_rapm_penalty.py`. No season's
  spread collapses (WNBA 2004 sd 0.68 -> 1.63), and next-season R^2 rises (NBA 0.154 -> 0.196). (#1150)
