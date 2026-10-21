- **Basketball-Reference:** `bref_team_roster()` returns `number` as String (was Float64), so Robert Parish's
  `"00"` is no longer `0.0`; any other zero-padded column also stays String. (#928)
- **Bart Torvik:** `torvik_player_stats()` / `parse_torvik_player_stats()` return `number` as String (was Int64);
  the 8 `"00"` jerseys of 2025 no longer merge into the 291 `0` rows. (#928)
- **247Sports site pages:** `primary_color`, `jersey` and `postal_code` are String (were Int64 when every value
  was digits, so event `"000000"` was `0`); a (0, 0) `latitude` / `longitude` pair, 247's no-location sentinel, is null. (#928)
- **KenPom:** the jersey column is String on every page and renamed to `jersey`: `team` / `team_players_expanded`
  `player_table*` `column_0` -> `jersey` (was Float64) and `box` `table_*` `column_0_rk` -> `jersey` (was Int64). A season of pages stacks
  without a String / Float64 clash, and no jersey reads `"2.0"`. (#928)
