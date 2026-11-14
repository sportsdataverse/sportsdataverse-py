- **NBA / WNBA / MBB / WBB:** new `{nba,wnba,mbb,wbb}_game_player_box_splits`: each player's box from the pbp per game,
  per period (with `on_floor_at_end`) and with garbage time split out, on the situational splits' flag (GOP Q7, Q26).
  The new `nba_box_events` / `ncaa_box_events` give the counts per action. (#PR)
- **NBA / WNBA / MBB / WBB:** `{lg}_game_stints` carry the player's own `points`, `fgm` / `fga`, `fg3m` / `fg3a`,
  `ftm` / `fta`, `oreb` / `dreb` / `reb`, `ast`, `stl`, `blk`, `tov` and `pf` per stint, and the stint rows an
  `is_garbage_time` flag (GOP Q30). (#PR)
