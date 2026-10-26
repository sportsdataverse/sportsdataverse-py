- **NFL loaders:** `seasons` follows nflreadpy: `None` loads the current season, `True` every season from the loader's
  first to the current one (skipping a current season not yet published), and `[]` an empty frame without a request.
  (#898)
- **NFL loaders:** `seasons=None` raised `TypeError` in the helper-backed loaders, and in `load_nfl_player_stats` (its
  default) loaded every season since 1999. (#898)
- **NFL loaders:** `[]` returned every season from `load_nfl_schedule`, `load_nfl_nextgen_stats`, season-level
  `load_nfl_pfr_advstats` and `load_nfl_espn_qbr`, and raised from `load_nfl_ratings_weekly` / `load_nfl_ngs`; a
  mistyped season (`2024.5`, `True` in a list, `"2024x"`) now raises `ValueError` naming `seasons`. (#898)
- **NFL loaders:** `load_nfl_rosters` and `load_nfl_depth_charts` take the current season on the roster calendar (the
  year turns on March 15), as nflreadpy does; `load_nfl_nextgen_stats` raises `SeasonNotFoundError` before 2016. (#898)
- **NFL loaders:** `load_nfl_player_stats(True)`, an old positional call for the removed kicking flag, loads every
  season of the non-kicking stats where it raised `TypeError`; pass `kicking=True` for kicking stats.
  `load_nfl_player_stats(False)` still raises `TypeError`. (#898)
