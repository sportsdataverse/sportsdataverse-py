- **NFL:** `espn_nfl_pbp_nflverse` adds GSIS player ids (`participants=`, `players=`), nflfastR text, drive and series
  columns, and model columns (`enrich=True`); 97%+ ids and EP r 0.99 vs nflverse from 2014. `NFLPlayProcess` stamps
  `nflverse_game_id`, so no schedule is needed. (#1270)
