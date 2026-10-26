- **MLB:** `fit_zone_model` excludes 2026+ pitches with a `UserWarning` (Savant reports them against the ABS zone and
  the calls are challengeable), and `mlb_umpire_called_strike_prob` / `mlb_umpire_bias` / `mlb_catcher_framing` carry
  an `abs_era` flag, with per-umpire and per-catcher rows split by era. (#743)
