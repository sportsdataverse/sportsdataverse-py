- **NBA player props:** `player_rates` returns `games` as Int64, as documented (it was UInt32 on
  non-empty input). Its docstring now says it drops zero- and negative-minute rows and how a traded
  player's `team_id` is chosen. (#1122)
