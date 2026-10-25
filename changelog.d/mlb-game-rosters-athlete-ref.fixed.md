- **`espn_mlb_game_rosters`:** each rostered athlete is now read from its Core v2 `$ref`, as the other Core v2
  roster chains do, so `athlete_id`, `athlete_full_name`, `athlete_jersey` and the three `athlete_position_*`
  columns come from the athlete record. ESPN's roster entry carries the athlete and the position as a bare `$ref`,
  so all six were null on every real game. `athlete_position_*` is the athlete's listed (season) position; the new
  `game_position_id` (Int64) is the position he was entered at in this game, from the entry's own position `$ref`
  (they differ for 11 of 21 entries on game 401701038). The athlete id is the entry's `playerId`, else the id in
  its athlete `$ref`. An entry that carries the athlete inline uses that record; an entry with neither, whose
  athlete `$ref` 404s, or that matches no athlete record read is left out with a warning. When roster entries
  exist but no athlete can be read the game raises `NoDataError`, as the six sibling chains do; no roster entry at
  all is the documented zero-row frame. A side whose roster 404s is still skipped, and a failed fetch still raises
  `AssetFetchError`. `raw=True` keeps its per-entry rows of the documented keys (now including
  `game_position_id`), read from the entry alone; `athlete_id` there is now the entry's `playerId` (it was null).
  (#1002, #1010)
