- **Cricket:** `cricket_win_probability` selects the ODI fielding-restriction regime from the match date
  (`cricket_match_state` now carries `match_date`; `cricket_model_constants.ODI_REGIMES` / `cricket_regime`:
  pre-2005, 2005 powerplays, 2011 window, 2012 two new balls, 2015+) and reads the regime-keyed resource
  surface, falling back to the pooled 2002-2026 grid where a regime has fewer than 100 cells with 30+ states.
  The bundled regime grids are pooled copies until the Cricsheet refit; the output gains a `regime` column.
