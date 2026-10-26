- **NFL:** the season flips on the Wednesday after Labor Day and week 1 starts the Thursday after (nflreadr's rules;
  week read one high in 2015-16, 2020-22, 2026-27). `most_recent_nfl_season` and `get_current_week` share that rule, so
  from September 1 to that Wednesday they give the previous season (week 22), not the new one. (#791)
