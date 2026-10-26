- **CFB / NFL:** the advanced box's team rows carry `yards_per_dropback`, sack-inclusive yards per dropback
  (`(pass_yards + sack_yards) / passes`, over the same dropbacks as `yards_per_pass`, which still counts a sack as 0
  yards). It is the basis of SDV's `yardsdropback` percentiles. (#740)
- **CFB / NFL:** the `espn_{cfb,nfl}_adv_team` (and CFB `adv_team_gamelog`) releases pick up `yards_per_dropback` on
  their next rebuild. (#740)
