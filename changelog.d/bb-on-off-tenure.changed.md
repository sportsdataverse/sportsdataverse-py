- **Basketball:** the NBA / WNBA player on/off counts the off side only over the player's tenure with the team, as
  stats.nba.com does: `*_off`, `<rating>_diff`, their ranks, percentiles and sample sizes now use the span window
  (from his first game for the team, or the day after his last for an earlier team, to the day before his first for a
  later team). `poss_on + poss_off` is the team's possessions over that span. The `*_off_span` / `*_diff_span` columns
  stay and repeat them. College keeps the whole season (`player_on_off(tenure=False)`). (BB-D5)
