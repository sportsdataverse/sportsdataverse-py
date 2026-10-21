- **NFL `advBoxScore` row order (usage / special teams):** `player_usage`, `team_usage`, `drive_scripting` and the
  `st_*` tables now list their rows sorted by their keys (team, then player), so an NFL game serialises to the same
  box in every process. This changes the row order of those tables; their values are unchanged. Before, on the 37
  committed NFL captures, `team_usage` and the `st_*` tables came out in a different order in two processes in
  15-23 games. (R2.22-4)
