- **NFL:** `espn_nfl_pbp_nflverse` reads ESPN play-by-play as nflverse `play_by_play` (keys, teams, clock, flags, yards,
  split PAT rows, schedule columns). On every game of 2007, 2014, 2019 and 2024 most columns match nflverse at 99%+;
  `desc`, the clock, timeouts and (2007) the text-derived columns match less. (#1266)
