- **ESPN soccer:** `parse_soccer_scoreboard` (every `espn_<league>_scoreboard`) carries the event's `season_type` and
  `season_slug`, and it and the summary `header` frame carry a derived `tournament` (`"apertura"` / `"clausura"`, null
  for single-tournament leagues), so Liga MX's two competitions per ESPN season id can be split. (#742)
