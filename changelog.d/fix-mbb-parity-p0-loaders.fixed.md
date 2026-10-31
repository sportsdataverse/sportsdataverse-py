- **MBB:** `load_mbb_shots` accepts every published season (2003, 2006 onward); its floor was 2025, so 20
  published seasons raised `SeasonNotFoundError`. 2004 and 2005 have no asset and are skipped with a warning. (#1044)
- **MBB:** `espn_mbb_pbp` leaves a game with no betting line null (`gameSpread`, `homeFavorite`, `homeTeamSpread`,
  with `gameSpreadAvailable` False), hoopR's NA; it invented a 2.5-point home-favored spread (and a 142 total). (#1044)
- **MBB / NBA:** the team-ratings and win-probability models turn ids into text through Int64, so a float-origin
  id can never become `"401….0"`. (#1044)
