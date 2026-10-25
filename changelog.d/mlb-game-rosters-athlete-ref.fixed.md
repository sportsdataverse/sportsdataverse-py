- **`espn_mlb_game_rosters`:** `athlete_id`, `athlete_position_id` and `athlete_full_name` are filled. ESPN's
  Core v2 roster entry carries the athlete and the position as a bare `$ref` with no inline id, so they were null
  on every real game; the ids are now read from the `$ref` (Int64) and the name from the entry's `displayName`.
