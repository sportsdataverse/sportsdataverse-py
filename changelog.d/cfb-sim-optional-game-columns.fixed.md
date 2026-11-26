- **CFB:** `cfb_simulations()` accepts `cfb_games_from_schedule()` output as is: the title-game and playoff rows
  it generates no longer raise a polars `ShapeError` when the input carries `home_points`/`away_points`. (#1312)
