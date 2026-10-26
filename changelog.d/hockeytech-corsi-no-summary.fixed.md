- **HockeyTech:** `<lg>_game_corsi` / `pwhl_game_corsi` without a game summary (MJHL, whose `gc/gamesummary` is
  denied) return zero rows instead of crediting every shot to the away skaters; a blank `""` `home_team_id` is skipped
  too. (#988, #989)
