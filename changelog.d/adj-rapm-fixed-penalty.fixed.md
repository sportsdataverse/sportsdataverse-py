- **NBA/WNBA:** `nba_adj_rapm` fits at a frozen ridge penalty per league (`ADJ_RAPM_PENALTY`, NBA 3,728, WNBA 2,683),
  not one picked per call, toward the SPM prior scaled by 2 (`ADJ_RAPM_PRIOR_SCALE`; both from
  `tools/fit_adj_rapm_penalty.py`). No season's spread collapses (WNBA 2004 sd 0.68 -> 1.63). (#1150)
