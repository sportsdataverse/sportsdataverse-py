- **NBA:** loaders for the multi-file `nba_stats` releases: `load_nba_stats_synergy`, `_hustle_player` / `_team`,
  `_matchups` / `_matchups_rollup` and five `_draft_combine_*` tables. Each unions a season's files, stamps
  `season_type` / `per_mode` from the file name where rows lack them, and takes START-year `seasons`. (#780)
- **NBA:** `load_nba_stats_draft(seasons)` reads the `nba_stats_draft` release; `seasons` is the draft year. (#780)
