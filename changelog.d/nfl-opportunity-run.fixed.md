- **NFL pbp:** `opportunity_run` marks a carry that gained 4 or more yards; it was inverted (`<= 4`), so the box's
  `rushing_opportunity_rate` ran about 0.62 instead of about 0.46. (#739)
- **NFL pbp:** `opportunity_run`, `highlight_run` and `adj_rush_yardage` gate on `rush`, as the CFB processor does, so
  rushing touchdowns and fumbled rushes are no longer dropped. (#739)
