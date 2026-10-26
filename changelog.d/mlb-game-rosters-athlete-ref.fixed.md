- **MLB:** `espn_mlb_game_rosters` reads each rostered athlete from its Core v2 `$ref`, so `athlete_id`,
  `athlete_full_name`, `athlete_jersey` and the three `athlete_position_*` columns (the listed season position) are
  filled; all six were null on every real game. (#1002, #1010)
- **MLB:** the new `game_position_id` (Int64) is the position the player was entered at in this game. The athlete id
  is the entry's `playerId`, else the id in its athlete `$ref`; an inline athlete record is used as is. (#1002, #1010)
- **MLB:** an entry with no readable athlete is left out with a warning; when entries exist but no athlete can be read,
  the game raises `NoDataError`. No entries at all is the zero-row frame; a failed fetch raises `AssetFetchError`.
  (#1002, #1010)
- **MLB:** `espn_mlb_game_rosters(raw=True)` keeps its per-entry rows, now with `game_position_id`, and `athlete_id`
  there is the entry's `playerId` (it was null). (#1002, #1010)
