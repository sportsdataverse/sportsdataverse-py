- **NHL:** `build_stints` gave intervals with no event a null `strength_state`, so ~23% of game time fell out of
  RAPM, WAR TOI, special-teams and unit ratings, and the 5v5 xG rate read 3.5/60 for ~2.7. They now keep the previous
  interval's strength and goalies. (#1213)
