- **ESPN:** `espn_<lg>_player_stats` raises `AssetFetchError` when the athlete or team dereference fails (403, 429, 5xx,
  a dead connection) instead of returning a row with null identity columns; only a 404 leaves those columns null. (B-M1)
- **NCAA:** `ncaa_espn_team_crosswalk` falls back to the bundled copy only on a failed or absent release fetch or a
  drifted asset; any other error raises. `ncaa_mbb_box_scores` reports a dropped game through the module logger, not
  `print`. (MD-4, B-L41)
- **Parsers:** the NHL api-web / EDGE, MLB Stats API and stats.nba.com parsers return zero rows for a `None`, `""`,
  list or `{code, message}` error-envelope body instead of raising or flattening it into a row. (EX-1.3-a)
