- **ESPN rankings:** `espn_{cfb,mbb,wbb,mch,wch}_rankings()` returned zero rows (the Site v2 polls sit in a top-level
  `rankings` list, not Core v2 `items`). A dedicated `parse_rankings` gives one row per ranked or vote-receiving team
  per poll. (#732)
- **ESPN rankings:** columns are `poll_id`, `season`, `season_type`, `week` (within the season type), `rank` (null on
  vote-receiving rows), `previous_rank`, `points`, `first_place_votes` and the team fields; `team_id` is a string.
  (#732)
- **ESPN rankings:** the college baseball and softball wrappers use it too, but ESPN answers their rankings URL with a
  404. (#732)
