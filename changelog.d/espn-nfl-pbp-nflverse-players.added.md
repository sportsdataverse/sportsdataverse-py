- **NFL:** `espn_nfl_pbp_nflverse` adds GSIS player ids (`participants=`, `players=`), nflfastR text, drive and series
  columns and model columns (`enrich=True`); from 2014 most ids match nflverse at 95%+, EP at r 0.99. `NFLPlayProcess`
  now stamps `nflverse_game_id`, so the view needs no schedule. (#1270)
