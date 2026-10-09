- **HockeyTech derived coordinates put home-team events off the rink.** `add_coord_transforms`
  (every `<league>_pbp` and `pwhl_pbp`) flipped home events with a 0-200 x 0-85 mirror applied
  to centre-origin feet, so home `x_coord_right` ran 191 to 290 ft and `y_coord_right` 49 to
  125 ft (PWHL game 42), and `y_coord_fixed` applied the y transform twice. The feed puts the
  home team's attack toward x = 0 in every period (320 PWHL games; sdv-internal-refs #50), and
  the columns are now rotations of the feet frame (#51): `x_coord_fixed` / `y_coord_fixed` =
  (-x, -y), home team shooting right; `x_coord_right` / `y_coord_right` = home (-x, -y),
  visitor (x, y); `x_coord_vertical` / `y_coord_vertical` = (-y_right, x_right). The right and
  vertical columns are null when an event's side is unknown (no team, as on faceoffs, or a null
  or empty home team id) instead of defaulting to the visitor's. `shot_distance`, `shot_angle`
  and xG are unchanged. The published PWHL play-by-play releases keep the old values until they
  are rebuilt.
