- **Possession cache:** `nba_season_compile.PIPELINE_VERSION` is 5, so per-game caches keyed by it
  (`compile_nba_season`, and WNBA builders that import `_game_cache_key`) recompile once. (#818)
