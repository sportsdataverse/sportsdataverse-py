- **MBB / WBB:** `ncaa_mbb_box_scores(..., errors="skip")` (and the WBB twin) records a
  `parse_failed` status row for a game whose page parses to zero player rows (a page of another
  type); the game used to vanish from the season with no row. (#1014)
