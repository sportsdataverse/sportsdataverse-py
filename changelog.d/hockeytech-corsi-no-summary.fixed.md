- **HockeyTech `<lg>_game_corsi` / `pwhl_game_corsi` without a game summary** (MJHL, whose `gc/gamesummary` is denied):
  the result is zero rows. It used to credit every shot to the away skaters, since the home side was unknown. The
  Int64 id cast made `home_team_id` null there, so the existing null guard skips those rows; a blank `""`
  `home_team_id` (a String frame) is now skipped too, as hardening. (#988, #989)
