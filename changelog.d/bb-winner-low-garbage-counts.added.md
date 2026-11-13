- **NBA / WNBA / MBB / WBB:** `season_game_excitement` adds each completed game's comeback low: `winner_team_id`,
  `winner_low_wp` (the winner's lowest WP after any play, 0-1) and that play's `winner_low_period` /
  `winner_low_clock`, from the WP series the index sums (GOP Q41). (#1190)
- **MBB / WBB:** `{mbb,wbb}_situational_team_games` carry `fga`, `fgm`, `fg3a`, `fg3m`, `fta`, `ftm`, `oreb`, `dreb`
  and `tov` on the `all` and `garbage` rows, summed from the `ncaa_{mbb,wbb}_pbp` events by the new
  `ncaa_team_game_counts`, so college shooting and four factors can leave garbage time out (GOP Q6). (#1190)
