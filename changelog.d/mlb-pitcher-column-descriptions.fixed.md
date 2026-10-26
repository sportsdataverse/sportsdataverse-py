- **MLB docs:** `pitcher` in the `load_mlb_xera`, `load_mlb_stuff_plus`, `load_mlb_command_plus`, `mlb_statcast_search`
  (+ `_minors` / `_wbc`), `as_of_split` and `bip_trajectory_features` returns tables reads "MLBAM player id of the
  pitcher", not "Whether the position is a pitcher". (#799)
- **MLB docs:** on the Statcast-search, `as_of_split` and `bip_trajectory_features` pages, `home_score` / `away_score`
  are the score before the pitch and `player_name` follows `player_type` (the batter by default). (#799)
- **MLB docs:** on `espn_mlb_schedule`, `home_score` / `away_score` are described as the ESPN scoreboard score rather
  than a per-play run total. (#799)
