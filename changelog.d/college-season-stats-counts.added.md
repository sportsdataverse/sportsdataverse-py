- **Basketball season stats:** a player's made-attempted text (`"83-111"`) is followed by numeric `freeThrowsMade` /
  `freeThrowsAttempted` rows (likewise FG, 3PT and the per-game averages), the names the team table uses. (#1201)
- **College season stats:** `helper_{mbb,wbb}_team_season_opponent_stats` sums each team's opponents' points, FG, 3PT
  and FT made / attempted from the team box, as `opponent` rows of `team_season_stats`. (#1201)
