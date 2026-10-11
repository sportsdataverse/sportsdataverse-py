- **Cricket:** `cricket_win_probability` selects the ODI fielding-restriction regime from the match date
  (`cricket_match_state` now carries `match_date`; `cricket_model_constants.ODI_REGIMES` / `cricket_regime`:
  pre-2005, 2005 powerplays, 2011 window, 2012 two new balls, 2015+) and applies a regime whole -- its own
  resource grid and `REGIME_CONSTANTS` par/sigma -- or falls back to the pooled 2002-2026 fit (a grid with
  fewer than 100 cells of 30+ states, or no fitted constants: the 13-month 2011 window). Output gains `regime`.
