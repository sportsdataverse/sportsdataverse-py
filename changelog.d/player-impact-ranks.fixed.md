- **NBA/WNBA:** `nba_adj_rapm` fits its ridge with an unpenalized intercept, so `o_adj_rapm` / `d_adj_rapm` are
  centred near 0 (2025-26 NBA +0.32 / +0.06); they read about +12 / -11 before. The fix also removes a
  playing-time lean from `adj_rapm`, and `adj_rapm = o + d` still holds exactly. (#1137)
