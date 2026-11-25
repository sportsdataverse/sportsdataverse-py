- **Basketball:** `{nba,wnba,mbb,wbb}_team_season_rates` and `_lineup_seasons` split `net_rtg` into four-factor points
  per 100 against the league average (`off_*_pts`, `def_*_pts` and their sums `efg_pts`, `tov_pts`, `orb_pts`,
  `ft_pts`), which add up to `net_rtg` exactly, plus `lg_rtg` (BB-D42). (#1287)
