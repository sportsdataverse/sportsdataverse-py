- **NFL box score:** `off_yards`, `total_off_yards`, `yards_per_play` and `total_yards` now leave out an interception's
  return yardage, as CFB does. The rusher lines' `Car` / `Yds` / `YPC` now count kneel-downs as the kneeler's carries,
  as nflverse and ESPN's box do; `EPA` / `WPA` / `SR` stay on designed rushes. (#1247)
