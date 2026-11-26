- **MLB:** `build_we_table` and `mlb_win_expectancy` keep one results row per game, so a suspended game listed on
  two dates no longer counts twice in win expectancy; `run_value` returns NaN for a state missing from the matrix,
  as for a null one (it scored 0.0). (#1293)
