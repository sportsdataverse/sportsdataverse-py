- **MBB / WBB pbp:** free throws of the 2014-2026 seasons (ESPN types made and missed ones alike
  "MadeFreeThrow") now sit on the line in `coordinate_x/_y`, transformed from (25, 13.75) like
  NBA / WNBA's; `coordinate_x_raw/_y_raw` keep ESPN's (25, 0), as the published files do. hoopR /
  wehoop's case-sensitive "Free Throw" match never caught them, so the published MBB / WBB files
  keep ESPN's coordinates on those rows (39, 44 and 26 rows in the three committed MBB games, 37
  in the WBB one); they are corrected at the next republish (REPROCESS run #3).
