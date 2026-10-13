- **PWHL:** `pwhl_game_info`, `pwhl_player_box` and `pwhl_player_info` return data again. They read
  the `statviewfeed` `gameSummary` / `player` replies, which are bare JSON objects, as `SiteKit`
  envelopes and so returned an empty frame for every game and player. `pwhl_game_info` now returns
  the published `load_pwhl_game_info` columns, `pwhl_player_box` the fastRhockey skater and goalie
  frames (ids Int64), and `pwhl_player_info` one row of player bio. The HockeyTech client also
  unwraps a JSONP body that ends in `);`.
