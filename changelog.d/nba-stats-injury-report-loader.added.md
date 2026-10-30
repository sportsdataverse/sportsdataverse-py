- **NBA:** `load_nba_stats_injury_report(seasons)` reads the official injury report from the `nba_stats_injury_report`
  release (2018-19 on): every published version, one row per listing, with `game_id`, `team_id` and `person_id`
  joined and `match_method` saying how each player was matched. (#1043)
- **Codegen:** loader schemas accept a naive `Datetime(time_unit=..., time_zone=None)` column type. (#1043)
