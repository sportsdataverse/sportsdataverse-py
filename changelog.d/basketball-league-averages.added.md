- **NBA / WNBA:** `nba_team_ratings` / `wnba_team_ratings` add `<m>_rank`, `<m>_pct` and `<m>_n` for
  the seven ratings (BB-D2). The lowest DRtg ranks 1. Ranks are null under five qualified teams and for
  the All-Star teams. `_n` counts possessions (ratings) or games (pace). (#1072)
- **Basketball:** the internal `sportsdataverse._common.league_context` holds the shared season-table
  helpers: ranks, `_conf_pct`/`_pos_pct` cohort percentiles, `_n`, and the football F1
  league-average rows. Cohort floors are 5 teams and 10 players. (#1072)
