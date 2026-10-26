- **Spring football EP/WP:** `build_spring_football_pbp(..., league="cfl")` and `enrich_spring_football_pbp(...,
  league="cfl")` raise `ValueError`; they scored CFL plays (110-yard field, three downs) with the NFL boosters, whose
  support is `yardline_100` 1-99. `get_sf_constants("cfl")` still returns the CFL rule constants. (#1010)
