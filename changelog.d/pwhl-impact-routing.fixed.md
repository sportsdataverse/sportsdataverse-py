- **PWHL:** `pwhl_skater_rapm`, `pwhl_unit_ratings`, `pwhl_special_teams_value` and `pwhl_skater_war`
  run on `load_pwhl_pbp` + `load_pwhl_shifts` output; they raised `ColumnNotFoundError: "secondary_type"`.
  `pwhl_team_ratings` / `pwhl_player_props` read PWHL data; they returned NHL ratings and props. (#846)
