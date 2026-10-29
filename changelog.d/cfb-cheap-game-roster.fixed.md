- **CFB:** `CFBPlayProcess` reads its game roster from the competitors and two team-roster pages (3 requests)
  instead of `espn_cfb_game_rosters`' one Core v2 request per athlete (~230 a game), which under ESPN throttling
  held every worker of Game on Paper's API. The roster it fetches is kept as `game_roster`, so the usage box
  gets positions and tacklers' teams from it too.
