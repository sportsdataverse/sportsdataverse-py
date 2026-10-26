- **MLB:** `pitch_features` standardizes velocity, spin, movement and release within pitcher AND season
  (`game_year`, else the year of `game_date`), so PITCHf/x speeds (2008-16) and Statcast out-of-hand speeds
  (2017+, about +0.5-1 mph) never share one mean. (#743)
