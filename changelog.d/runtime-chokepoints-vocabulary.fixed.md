- **ESPN parsers:** the 30 `_common_espn_parsers` parsers that raised `AttributeError` on a non-dict body, or turned
  ESPN's 200 error envelope into a one-row frame, return zero rows. (#880)
- **ESPN parsers:** `parse_items` warns when ESPN answered one page of several (`espn_nba_athletes_index()` returns 100
  of 610 athletes by default). (#880)
- **NHL:** the `nhl_scoreboard(team=...)` route is never cached, like `/scoreboard/now`. (#880)
