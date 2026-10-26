- **NHL:** twelve release loaders accept 2010 (the 2009-10 season), where every NHL release starts: `game_info`,
  `goalie_boxscores`, `linescore`, `penalties`, `scoring`, `scratches`, `skater_boxscores` and `three_stars` started at
  2024, `officials`, `shifts`, `shootout` and `shots_by_period` at 2025 (all `load_nhl_*`). (#775)
- **NHL:** `load_nhl_pbp_lite()` reads the current `play_by_play_lite_<season>` files (2010-2027, every legacy column
  plus `game_date`); the legacy `play_by_play_<season>_lite` files stopped at 2026, so the current season was missing.
  (#775)
