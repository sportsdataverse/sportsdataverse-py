- **Cricket:** `cricket_win_probability` selects the ODI fielding-restriction regime from the match date
  (`cricket_model_constants.ODI_REGIMES` / `cricket_regime`: pre-2005, 2005 powerplays, 2011 window, 2012 two new balls,
  2015+); `cricket_match_state` carries `match_date` and the output gains `regime`. (#743)
- **Cricket:** a regime applies whole (its own resource grid and `REGIME_CONSTANTS` par/sigma), or falls back to the
  pooled 2002-2026 fit when its grid has fewer than 100 cells of 30+ states or it has no fitted constants (the 2011
  window). (#743)
