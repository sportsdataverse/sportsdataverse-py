- **`espn_mlb_game_rosters`:** each rostered athlete is now read from its Core v2 `$ref`, as the other Core v2
  roster chains do, so `athlete_id`, `athlete_full_name`, `athlete_jersey` and the three `athlete_position_*`
  columns come from the athlete record. ESPN's roster entry carries the athlete and the position as a bare `$ref`,
  so all six were null on every real game. An athlete whose `$ref` 404s is left out with a warning. (#1002)
