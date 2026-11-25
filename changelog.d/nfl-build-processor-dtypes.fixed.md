- **NFL:** `build_nfl_season(source="espn")` keeps the processor's dtypes: an all-null `*_player_id` is `Int64`,
  not `pl.Null`, and `Int32` columns stay `Int32` (cached frames rebuild); its returns tables type 20 more
  columns (`cp`, `cpoe`, `xyac_*`, `air_yards`, ...) that were `character` only because they were all null. (#1272)
