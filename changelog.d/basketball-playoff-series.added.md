- **NBA/WNBA:** `nba_playoff_series` / `wnba_playoff_series` key every postseason game to its series (one pair of teams
  per postseason; play-in games are not a series), with the round, the game number, the series score after each game
  and the winner once decided (BB-D43). (#1305)
- **NBA/WNBA:** `_playoff_series_teams` / `_playoff_series_players` sum the ESPN game boxes per series, and the player
  rows carry each per-game line against the player's regular season (`<stat>_pg_diff`, BB-D43). (#1305)
