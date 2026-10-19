- **MLB umpire zone / catcher framing:** a pitch frame entirely from 2026 on is now fit on its own pitches.
  The 2026 plate-location guard dropped every row of such a frame, so `fit_zone_model` returned zero
  coefficients and every pitch scored P(called strike) = 0.5, which turned `mlb_catcher_framing` into a
  called-strike rate. A frame spanning 2025 and 2026 still fits on its pre-2026 rows only, and a degenerate
  fit (fewer than 2 called pitches) now warns.
