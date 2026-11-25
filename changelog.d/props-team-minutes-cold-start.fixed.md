- **NBA / WNBA:** `nba_player_props` / `wnba_player_props` add `minutes_exp`, summing to 240 / 200 per
  team, and a team with no ratings row yet (an expansion team's first game) gets the league pace instead
  of raising `KeyError`. (#1276)
