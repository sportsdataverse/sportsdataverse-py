- **NFL:** `nfl_player_projection` / `nfl_fantasy_projection` age players from live `load_nfl_rosters` frames
  (`gsis_id` + `birth_date`, aged as of about October 1 like the fitted corpus) instead of raising on null ages;
  availability reads live roster positions too. (#1344)
