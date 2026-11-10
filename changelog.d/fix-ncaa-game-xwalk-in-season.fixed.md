- **NCAA:** the `contest_id` -> `espn_game_id` crosswalk builds in season: it reads the parsed contests when the
  schedule pages lack ids, and `stamp_parsed()` reconciles contests parsed before it with the crosswalk (writing
  matched ids, clearing ids a rebuild now rejects). 2026-27 had built 0 contests, so every 2027
  `ncaa_{mbb,wbb}_*` row had a null `espn_game_id`. (#1160)
