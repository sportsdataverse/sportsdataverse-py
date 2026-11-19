- **Multi-season RAPM:** `nba_multiseason_rapm` pools several seasons' possessions into one plain ridge fit at the
  league's fixed penalty, with `n_seasons` per player. `tools/gate_multiseason_rapm.py` checks the 2019-20 to 2026-27
  regular-season window against Jeremias Engelmann's 8-season RAPM: all 1,224 players match, and Spearman on the total is
  0.967 at 10,000+ possessions (0.911 over all players) with a spread 0.90 of his.
