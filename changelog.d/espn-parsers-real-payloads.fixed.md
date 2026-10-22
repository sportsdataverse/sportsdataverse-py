- **ESPN parsers:** `parse_athlete_stats` reads the Web v3 per-season rows (it returned an empty frame for every
  athlete), `parse_event_competitor_statistics` reads Core v2's single `splits` object and `parse_draft` parses the
  Core v2 season-draft index to one row (both raised `AttributeError` on every real call); their returns tables are
  now documented from real captures.
