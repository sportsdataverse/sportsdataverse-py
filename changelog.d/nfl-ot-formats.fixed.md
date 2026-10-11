- **NFL:** overtime `wp` / `vegas_wp` now follow the season's overtime rule format (`model_vars.OVERTIME_FORMATS`):
  sudden death, nflfastR's One_FG form with a per-game `Drive_Diff`, or a both-teams-possess form (postseason 2022+,
  regular season 2025+) under which an opening-drive touchdown no longer ends the game; spring-football overtime gets null.
