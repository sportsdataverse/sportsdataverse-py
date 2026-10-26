- **NBA / WNBA / MBB / WBB pbp:** `espn_<lg>_pbp` keeps `coordinate.x` / `coordinate.y` but changes their meaning:
  they were ESPN's raw feet (hoop at (25, 0)) and are now the hoopR / wehoop full-court frame (`x = y - 41.75`,
  `y = x - 25`, both negated on home-team plays), always Float64. (#844)
- **NBA / WNBA / MBB / WBB pbp:** ESPN's values move to the new `coordinate.x.raw` / `coordinate.y.raw`
  (`coordinate_x_raw` / `coordinate_y_raw` snake-cased); on the NBA / WNBA free throws hoopR / wehoop's "Free Throw"
  match finds, the raw pair holds the pinned (25, 13.75), as in the published files. (#844)
- **NBA / WNBA / MBB / WBB pbp:** ESPN's missing-location sentinel reads about +-2.1e8 after the transform; filter on
  `coordinate_x_raw > nba_game_page.COORD_SENTINEL`. The release converters (`helper_<lg>_play_by_play`) rebuild from
  the raw pair, so a processed payload converts once. (#844)
