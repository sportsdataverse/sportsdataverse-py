- **NBA / WNBA / MBB / WBB pbp:** `espn_<lg>_pbp` plays carry shot coordinates in the hoopR /
  wehoop convention the published files use (free throws pinned to (25, 13.75), `x = y - 41.75`
  and `y = x - 25`, both negated on home-team plays), with ESPN's values kept as
  `coordinate.x.raw` / `coordinate.y.raw` (`coordinate_x_raw` / `coordinate_y_raw` once
  snake-cased). `coordinate.x` / `coordinate.y` were ESPN's raw values under the transformed names.
  The release converters (`helper_<lg>_play_by_play`) start over from the raw pair, so a processed
  payload converts exactly as before.
