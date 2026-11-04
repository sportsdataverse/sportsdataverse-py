- **WBB / WNBA player season stats:** `helper_{wbb,wnba}_player_season_stats` read the season's own `statistics[]` row,
  not ESPN's career `totals`, so a multi-season player no longer reports career numbers (Collier 2025: 33 games, not
  193). The team comes from that season's row. Fixes #1087. (#1094)
- **WBB / WNBA player season stats:** a traded player gets the whole-season "Totals" row, a season the payload does not
  cover gives no rows, and a payload without `statistics[]` still falls back to `totals`. (#1094)
