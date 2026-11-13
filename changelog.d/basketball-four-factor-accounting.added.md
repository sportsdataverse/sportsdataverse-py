- **Basketball:** `{nba,wnba,mbb,wbb}_team_game_four_factors` price each team-game's four factors in points
  against the season's league-average offense (`shooting_pts`, `turnover_pts`, `rebounding_pts`,
  `free_throw_pts`), and the four plus `residual_pts` add up exactly to `pts_vs_exp`, the points above a
  league-average offense on the same possessions. The league values the prices use are on each row.
- **Basketball:** `*_team_season_rates` carries `lg_ft_pct`, the league's free-throw percentage in the
  season and split, and the rate rows carry `ftm` (`ncaa_team_box_count_rows` reads the team box's `ftm`).
