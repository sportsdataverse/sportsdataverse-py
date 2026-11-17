- **NHL:** `build_stints` gave intervals with no event a null `strength_state`, so ~23% of game time fell out of
  RAPM, WAR TOI, special-teams and unit ratings, and the 5v5 xG rate read 3.5/60 for ~2.7. They now keep the strength
  and goalies of the latest event before them, except across a goalie pull or change or a skater-count change no
  event explains, and `as_of` now clips a stint that straddles it. (#1213)
