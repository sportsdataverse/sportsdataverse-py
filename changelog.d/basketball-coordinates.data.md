- **MBB / WBB pbp:** 2014-2026 free throws (ESPN types made and missed alike "MadeFreeThrow") now sit on the line in
  `coordinate_x/_y`, transformed from (25, 13.75) like NBA / WNBA's; `coordinate_x_raw/_y_raw` keep ESPN's (25, 0).
  (#844)
- **MBB / WBB pbp:** hoopR / wehoop's case-sensitive "Free Throw" match never caught those rows, so the published MBB /
  WBB files keep ESPN's coordinates on them until the next republish corrects them. (#844)
