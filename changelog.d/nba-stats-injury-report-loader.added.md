- **NBA:** `load_nba_stats_injury_report(seasons)` reads the official injury report from the new
  `nba_stats_injury_report` release (2018-19 on). It returns every published version, one row per listing, with
  `game_id`, `team_id` and `person_id` joined. `match_method` says how each player was matched to the printed
  name. This complements `nba_injury_report()`, which parses a single report live.
- **Codegen:** loader schemas accept a naive `Datetime(time_unit=..., time_zone=None)` column type.
