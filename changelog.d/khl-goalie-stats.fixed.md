- **KHL:** `khl_player_stats(position="goalies")` declares the goalie columns (`w`, `l`, `sop`, `ga`, `sv`, `sv_pct`,
  `gaa`, `so`, `toi`), fetches every page and sorts by games played, wins, then name (page 1 used to be the goalies
  without a game). A partial page set raises `AssetFetchError`. (#865)
