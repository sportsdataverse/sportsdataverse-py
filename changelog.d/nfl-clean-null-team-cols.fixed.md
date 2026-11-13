- **NFL:** `clean_nfl_pbp()` no longer raises `InvalidOperationError: expected String type, got: null` on a frame where a
  team column is null on every row (a season with no second fumble recovery, any published `nfl_model_pbp` season):
  team columns are cast to String before the team-code fold.
