- **MLB docs:** the `pitcher` column in the `load_mlb_xera`, `load_mlb_stuff_plus`, `load_mlb_command_plus`,
  `mlb_statcast_search` (+ `_minors` / `_wbc`), `as_of_split` and `bip_trajectory_features` returns tables now reads
  "MLBAM player id of the pitcher" instead of the inherited "Whether the position is a pitcher". On the Statcast-search
  pages, `home_score` / `away_score` are the score before the pitch (not after the play) and `player_name` follows
  `player_type` (the batter by default).
