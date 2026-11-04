- **Basketball:** the WNBA `functools.partial` bindings (`wnba_team_clutch`, `wnba_player_props`, `wnba_team_ratings`, the six
  `wnba_game_predict` ones) show `league_id='10'` in their signatures and reference pages. (#1082)
- **Basketball:** league-average rows drop a null season, and the docstrings describe `n` as the cohort count;
  five NBA examples import polars, and two returns descriptions are corrected. (#1082)
