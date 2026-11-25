- **NFL:** the Shield pbp builder and `enrich_nfl_pbp` add 73 nflverse `play_by_play` columns (stat-id flags, post-play
  score, yard line, feed times, description flags, `*_wp_post`, `play`, player aliases), and a try now gets
  nflfastR's value (field-goal make probability or 2 * 0.49) instead of the EP model's 1st-and-10 stand-in. (#1265)
