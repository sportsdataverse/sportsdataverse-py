- **HockeyTech:** `add_coord_transforms` (every `<league>_pbp` and `pwhl_pbp`) put home-team events off the rink: it
  mirrored centre-origin feet with a 0-200 x 0-85 flip, and `y_coord_fixed` applied the y transform twice. The derived
  columns are now rotations of the feet frame; the feed's home team attacks toward x = 0 in every period. (#735)
- **HockeyTech:** `x_coord_fixed` / `y_coord_fixed` = (-x, -y), home team shooting right; `x_coord_right` /
  `y_coord_right` = home (-x, -y), visitor (x, y); `x_coord_vertical` / `y_coord_vertical` = (-y_right, x_right). (#735)
- **HockeyTech:** the right and vertical columns are null when an event's side is unknown (no team, as on faceoffs, or
  a null or empty home team id) instead of defaulting to the visitor's. `shot_distance`, `shot_angle` and xG are
  unchanged; the published PWHL play-by-play keeps the old values until it is rebuilt. (#735)
