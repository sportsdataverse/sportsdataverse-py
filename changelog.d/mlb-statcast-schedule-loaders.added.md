- **MLB:** `load_mlb_statcast(seasons)` reads the `mlb_statcast` release (Baseball Savant pitch-level data, one
  parquet per season, 2015+; spring training and exhibition pitches included). The `on_1b` / `on_2b` / `on_3b`
  runner ids are published as floats and cast to Int64 at the loader boundary. (#794)
- **MLB:** `load_mlb_schedule(seasons)` reads the `mlb_schedules` release (statsapi schedule, every game type,
  1988+). (#794)
