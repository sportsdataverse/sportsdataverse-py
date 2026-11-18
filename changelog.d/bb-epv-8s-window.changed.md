- **Basketball:** the NBA and WNBA EPV boosters are refit on BB-D7's published 8-second transition window,
  replacing 10 s (`EPV_TRANSITION_SECONDS` is 8.0); college keeps 10 s (`COLLEGE_EPV_TRANSITION_SECONDS`). Served EPV
  values change for every NBA / WNBA season. (#1216)
- **Basketball:** both refits pass every gate under unchanged gate rules (the WNBA gate-2 threshold is recomputed
  from the data, 0.0335 to 0.0333), so the WNBA's owner-accepted 2026 gate-3 exception is retired. (#1216)
- **Basketball:** `epv_states` takes the window as `transition_seconds`, and `add_epv` refuses a bundled booster fitted
  on another window. (#1216)
