- **ESPN rankings parsed to an empty frame.** `espn_{cfb,mbb,wbb,mch,wch}_rankings()` ran the Site v2
  payload through `parse_items`, which reads a Core v2 `items` list; the polls sit in a top-level
  `rankings` list, so every league returned zero rows. A dedicated `parse_rankings` now gives one row
  per ranked or vote-receiving team per poll (live 2026-10-08: cfb 208 rows over 5 polls, mbb 77, wbb
  75, mch 35, wch 20), with `poll_id`, `season`, `season_type`, `week` (the week within the season
  type, which ESPN's Core v2 rankings URL uses), `rank` (null on vote-receiving rows), `previous_rank`,
  `points`, `first_place_votes` and the team fields; `team_id` is a string. The college baseball and
  softball wrappers route through it too, but ESPN answers their rankings URL with a 404. (#732)
