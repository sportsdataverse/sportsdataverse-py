- **NFL:** both play-by-play processors emit nflverse's `play_type_nfl` and `aborted_play`: Shield from the feed's
  `playType` (an intercepted pass is `INTERCEPTION`), `NFLPlayProcess` from ESPN's play type and flags (98.5-99.5%
  agreement out of sample, 2007-2025); `aborted_play` is the description containing `"Aborted"`. (#1255)
