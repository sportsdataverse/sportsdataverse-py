- **Spring football EP/WP:** `build_spring_football_pbp(..., league="cfl")` and
  `enrich_spring_football_pbp(..., league="cfl")` now raise `ValueError`. They used to score CFL plays
  with the NFL EP/WP boosters, whose support is `yardline_100` 1-99 while a CFL field is 110 yards and
  three downs; no CFL pipeline ships, as the constants module already said. `get_sf_constants("cfl")`
  still returns the descriptive CFL rule constants.
