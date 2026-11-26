- **NBA:** `nba_player_props` shrinks per-minute rates toward the player's minutes-and-role tier (this season plus
  last regular season), so early-season lines no longer come from a raw two-game rate; on held-out 2021-22, CRPS
  improves on all nine stats. `player_rates(prior_logs=, league_id=)`; WNBA and G-League stay raw. (#1308)
