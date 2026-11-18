- **Basketball:** the NBA and WNBA EPV boosters are refit on an 8-second transition window (BB-D7's published
  `is_transition`), replacing the 10 seconds they were first fitted on; `EPV_TRANSITION_SECONDS` is 8.0. College keeps
  its own 10 seconds (`COLLEGE_EPV_TRANSITION_SECONDS`), and `epv_states` takes the window as `transition_seconds`.
  Both refits pass every gate on the 2025 holdout and the 2026 confirmation under unchanged gate rules (the WNBA's
  data-derived gate-2 threshold is recomputed, 0.0335 to 0.0333), so the WNBA's owner-accepted 2026 gate-3
  exception is retired. `add_epv` refuses a bundled booster fitted on another window. Served EPV values change for
  every NBA / WNBA season.
