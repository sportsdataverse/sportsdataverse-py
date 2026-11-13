- **Basketball:** `{nba,wnba,mbb,wbb}_team_game_four_factors` price each team-game's four factors in points
  against the season's league-average offense; with `residual_pts` they add up exactly to `pts_vs_exp`,
  the points above a league-average offense on the same possessions. (#1192)
- **Basketball:** `*_team_season_rates` carries `lg_ft_pct`, the league's free-throw percentage in the
  season and split, and the rate rows carry `ftm`. (#1192)
