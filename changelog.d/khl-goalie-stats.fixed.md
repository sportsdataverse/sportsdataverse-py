- **KHL:** `khl_player_stats(position="goalies")` declares the goalie columns (`w`, `l`, `sop`, `ga`, `sv`, `sv_pct`,
  `gaa`, `so`, `toi`), fetches every page and returns one list sorted by games played, then wins, then name (khl.ru
  orders goalies by goals-against average ascending and ignores sort fields, so page 1 was the goalies without a
  game). A partial page set raises `AssetFetchError`. (#865)
