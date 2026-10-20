- **Basketball-Reference:** `bref_team_roster()` returns `number` as String (was Float64), so Robert Parish's
  `"00"` is no longer `0.0`; any other zero-padded column also stays String. (#TBD)
- **Bart Torvik:** `torvik_player_stats()` / `parse_torvik_player_stats()` return `number` as String (was Int64);
  the 8 `"00"` jerseys of 2025 no longer merge into the 291 `0` rows. (#TBD)
- **247Sports site pages:** `primary_color`, `jersey` and `postal_code` are String (were Int64 when every value
  was digits, so event `"000000"` was `0`); a (0, 0) `latitude` / `longitude` pair, 247's no-location sentinel, is null. (#TBD)
- **NHL Records:** `nhl_records_comeback_wins(scope=)` raises `ValueError` for anything but `"league"` or
  `"franchise"`; `"Franchise"` or `"team"` returned the league-wide table. (#TBD)
