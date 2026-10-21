- **NBA / WNBA player props:** `nba_player_props()` raises its documented `ValueError` when the
  player box logs carry no `game_date`; it used to fail with polars' `ColumnNotFoundError` before the
  check ran. (#NNN)
