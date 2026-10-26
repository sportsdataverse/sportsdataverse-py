- **PWHL:** `pwhl_player_box(game_id)` returns `{"skaters": DataFrame, "goalies": DataFrame}`, as
  fastRhockey's `pwhl_player_box` does and as the `fastRhockey-pwhl-raw` producer reads it. It used
  to return a single (always empty) DataFrame. (#851)
