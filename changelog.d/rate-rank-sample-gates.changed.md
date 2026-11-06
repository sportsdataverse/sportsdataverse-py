- **Basketball:** team-season-rate, lineup and on/off ranks now need the rate's own `<m>_n` to reach its half-signal
  sample (`rate_gates.TEAM_MIN_N` / `ON_OFF_MIN_N`, the split-half r = 0.5 point per league); clutch splits, most
  five-man units and college on/off differences rank nothing now, and their league averages follow. (#NNN)
- **Basketball:** `pace_n` counts the clock in regulation games, not games, and `{nba,wnba}_team_ratings`
  `adj_net_rtg_n` counts both ends (twice the box possessions), as `net_rtg_n` does elsewhere. (#NNN)
