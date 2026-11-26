- **ESPN snapshots:** `parse_probables_snapshot` reads a scoreboard's probable starters (NHL goalies, MLB
  pitchers, with ESPN's Expected / Confirmed), which `parse_scoreboard` drops; `pregame_state_as_of` keeps the
  `valid_from` / `valid_to` rows known at a decision time; `load_nhl_pregame_snapshots` reads them. (#1307)
