- **CFB:** `CFBPlayProcess` reads its game roster from the competitors and team-roster pages (3 requests for a
  current game; an older game's unnamed entries fetch only those athletes) instead of `espn_cfb_game_rosters`'
  one Core v2 request per athlete (~230 a game), which held every worker of GOP's API under throttling. (#1034)
- **CFB:** `CFBPlayProcess` keeps the roster it fetches as `game_roster`, so the usage box gets positions and
  tacklers' teams from it. (#1034)
