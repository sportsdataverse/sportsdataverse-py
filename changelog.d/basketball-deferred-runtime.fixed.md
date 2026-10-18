- **International basketball dtypes:** a column name now has one dtype across a family's tables.
  `fiba_gamepage_box_teams.blocks_received` and `fiba_gdap_schedule.utc_offset` are Int64 (they were String because
  every sample was null); likewise VTB Infobasket `plus_minus` / `player_position` / `height` / `weight` and the KZS
  match `season` are Int64. SCB `shirt_number` is always String (a `00` jersey survives), Sportality per-game
  averages (`blkpg`, `orpg`, `plus_minus_pg`) are Float64, and the TBF team-box `efficiency` is Int64. (#TBD)
- **CBA:** without `CBA_PROXY` a request from a datacenter now fails in seconds (a 5 s connect timeout, no retry of a
  dropped or refused connection) instead of after about three minutes; set `CBA_DIRECT=1` where direct access works
  for the full timeout and retries. A POST route called without `json=` raises `TypeError` (the generated wrappers
  always send it, so their behaviour is unchanged). (#TBD)
