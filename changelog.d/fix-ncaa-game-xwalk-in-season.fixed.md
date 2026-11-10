- **NCAA:** the `contest_id` -> `espn_game_id` crosswalk builds in season from the parsed contests, and
  `stamp_parsed()` reconciles contests parsed before it (2026-27 had built 0, so every 2027 `ncaa_{mbb,wbb}_*`
  row had a null `espn_game_id`). (#1160)
