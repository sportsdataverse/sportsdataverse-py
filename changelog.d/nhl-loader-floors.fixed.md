- **NHL:** twelve release loaders rejected seasons their releases publish:
  `load_nhl_game_info()`, `load_nhl_goalie_boxscores()`, `load_nhl_linescore()`,
  `load_nhl_penalties()`, `load_nhl_scoring()`, `load_nhl_scratches()`,
  `load_nhl_skater_boxscores()` and `load_nhl_three_stars()` started at 2024, and
  `load_nhl_officials()`, `load_nhl_shifts()`, `load_nhl_shootout()` and
  `load_nhl_shots_by_period()` at 2025. Every NHL release publishes from 2010
  (the 2009-10 season), and all of them now accept it. `load_nhl_pbp_lite()` reads
  the release's current `play_by_play_lite_<season>` files, now published for
  2010-2027. The legacy `play_by_play_<season>_lite` files stopped at 2026, so the
  current season was missing. The current files hold every legacy column, plus
  `game_date`.
