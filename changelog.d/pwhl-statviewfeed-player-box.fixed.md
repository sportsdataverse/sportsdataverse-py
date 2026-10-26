- **PWHL:** `pwhl_game_info`, `pwhl_player_box` and `pwhl_player_info` return data again; they read the bare-object
  `statviewfeed` `gameSummary` / `player` replies as `SiteKit` envelopes and so returned an empty frame every time.
  (#851)
- **PWHL:** `pwhl_game_info` returns the published `load_pwhl_game_info` columns, `pwhl_player_box` the fastRhockey
  skater and goalie frames (ids Int64), and `pwhl_player_info` one row of player bio. (#851)
- **HockeyTech:** the client unwraps a JSONP body that ends in `);`. (#851)
