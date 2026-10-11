- **MLB:** `fit_zone_model` (the umpire-zone and catcher-framing called-strike logistic) excludes 2026+
  pitches with a `UserWarning` -- Savant reports their location at the middle of the plate against the ABS zone
  and the calls are challengeable -- and `mlb_umpire_called_strike_prob` / `mlb_umpire_bias` / `mlb_catcher_framing`
  carry an `abs_era` flag (per-umpire and per-catcher rows split by era).
