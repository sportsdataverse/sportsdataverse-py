- **NBA:** `load_nba_team_crosswalk`, `load_nba_schedule_crosswalk` and `load_nba_player_crosswalk` return the NBA Stats
  bridge ids (`nba_team_id`, `nba_home_team_id` / `nba_away_team_id`, `nba_player_id`) as Int64, as NBA Stats ships them
  and the WNBA crosswalks already did; the zero-padded `nba_game_id` stays String. (#1186)
- **WBB:** the `load_ncaa_wbb_pbp` / `_player_box` / `_team_box` / `_possessions` / `_shots` loaders cast their `espn_*`
  ids to Int32, as the `load_ncaa_mbb_*` ones do. (#1186)
- **MBB/WBB:** `ncaa_espn_team_crosswalk` returns `espn_team_id` as Int32, the dtype of the crosswalk and NCAA releases,
  and `return_as_pandas=True` keeps nullable Int32 ids as pandas `Int32` instead of float. (#1186)
