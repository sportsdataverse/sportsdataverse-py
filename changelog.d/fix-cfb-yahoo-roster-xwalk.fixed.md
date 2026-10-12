- **CFB:** `cfb_rosters_crosswalk(providers=(..., "yahoo"))` fills `yahoo_athlete_id` from Yahoo's team roster,
  matched by name + jersey, then by a unique name; it read the ~200-player stat-leaders feed and found 0-2 per team.
  New optional `yahoo_team_id` skips the Yahoo team lookup. (#762)
