- **CFB / NFL:** the advanced box's team rows carry `yards_per_dropback`, sack-inclusive yards per
  dropback: `(pass_yards + sack_yards) / passes`, over the same dropbacks as `yards_per_pass`, which
  is unchanged and still counts a sack as 0 yards. It is the basis of SDV's `yardsdropback`
  percentiles, so Game on Paper's Binion box can rank the number it shows against them. The
  `espn_{cfb,nfl}_adv_team` (and CFB `adv_team_gamelog`) releases pick it up on their next rebuild.
