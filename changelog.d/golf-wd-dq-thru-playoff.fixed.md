- **Golf:** `parse_golf_leaderboard` adds `player_status_abbr` (`WD` / `DQ` / `CUT` / `F`; ESPN files WD and DQ under
  `STATUS_CUT`) and `playoff`, and `thru` counts holes completed (it read the last hole played, wrong on a split-tee
  start and null on LIV). (#1342)
