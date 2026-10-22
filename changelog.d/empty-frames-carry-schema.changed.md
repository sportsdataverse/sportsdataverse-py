- **Empty results keep their columns:** empty ESPN parser results, generated loaders and multi-file
  `load_nba_stats_*` loaders with no published season, four HockeyTech parsers and `<league>_pbp` for a
  game with no plays return zero rows with their returns table's columns and dtypes, not a (0, 0) frame.
