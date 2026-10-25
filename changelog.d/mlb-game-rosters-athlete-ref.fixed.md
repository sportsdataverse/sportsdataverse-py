- **`espn_mlb_game_rosters`:** each rostered athlete is now read from its Core v2 `$ref`, as the other Core v2
  roster chains do, so `athlete_id`, `athlete_full_name`, `athlete_jersey` and the three `athlete_position_*`
  columns come from the athlete record. ESPN's roster entry carries the athlete and the position as a bare `$ref`,
  so all six were null on every real game. An entry that carries the athlete inline uses that record. An entry
  with neither, or whose athlete `$ref` 404s, is left out with a warning; when no athlete can be read the result
  is the documented zero-row frame, never `NoDataError` for the whole game. A side whose roster 404s is still
  skipped, and a failed fetch still raises `AssetFetchError`. `raw=True` keeps its documented per-entry keys,
  read from the entry alone; `athlete_id` there is now the entry's `playerId` (it was null). (#1002, #1010)
