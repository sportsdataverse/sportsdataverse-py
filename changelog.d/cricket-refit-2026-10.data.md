- **Cricket:** the win-probability artifacts are refit on Cricsheet male internationals 2002-2026 (3,581 T20I +
  2,589 ODI): the surface is regime-keyed with per-cell support, the calibrator is fitted on the regime-aware raw
  probabilities, pooled constants move to T20 149.5/42/33 and ODI 249.3/76/59, and both holdout fixtures carry
  `match_date`. Holdout Brier 0.1898 -> 0.1882, max per-decile gap 0.0326 -> 0.0369 (gate 0.05).
