- **ESPN parsers:** `parse_athlete_stats` reads the Web v3 per-season rows (it returned an empty frame for every
  athlete); `parse_event_competitor_statistics` reads Core v2's `splits` object and `parse_draft` the season-draft index
  (both raised `AttributeError` on every real call). Their returns tables are documented from real captures. (#889)
