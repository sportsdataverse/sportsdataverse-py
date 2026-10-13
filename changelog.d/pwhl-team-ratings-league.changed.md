- **PWHL:** `pwhl_team_ratings` forwards to `pwhl_ratings_from_proxy` (its `league` argument is ignored);
  `nhl_player_props` takes `load_skater_boxscores=` and `team_ratings=`, and the NHL impact engines take
  `xg_scored=`, so another league binds its own data and xG model. (#846)
