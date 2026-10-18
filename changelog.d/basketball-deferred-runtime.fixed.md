- **International basketball dtypes:** a column name now has one dtype across a family's tables.
  `fiba_gamepage_box_teams.blocks_received` and `fiba_gdap_schedule.utc_offset` are Int64 (they were String because
  every sample was null); likewise VTB Infobasket `plus_minus` / `player_position` / `height` / `weight` and the KZS
  match `season` are Int64. SCB `shirt_number` is always String (a `00` jersey survives), Sportality per-game
  averages (`blkpg`, `orpg`, `plus_minus_pg`) are Float64, and the TBF team-box `efficiency` is Int64. (#864)
- **CBA:** without `CBA_PROXY` a request from a datacenter now fails in seconds (a 5 s connect timeout, no retry of a
  dropped or refused connection) instead of after about three minutes; set `CBA_DIRECT=1` where direct access works
  for the full timeout and retries. A POST route called without `json=` raises `TypeError` (the generated wrappers
  always send it, so their behaviour is unchanged). (#864)
- **CBA numbers:** the display strings the API sends (`'21.6'` per-game averages, `'39.4%'` percentages; about 100
  columns across the season lists, team tables, advanced box and shot zones) are parsed to Float64. A percent string
  is stored as a FRACTION 0-1, like the player box percentages and the `*_sort` columns, except where the family
  already holds the column 0-100 (`turnovers_rate`). `season` and the schedule win/loss counts are Int64; jersey
  numbers and code-table values stay text. Columns null in every capture take the family's dtype. (#864)
- **VTB Infobasket:** an empty string in an id, rank or number column is null; the roster `rank` (blank in every
  sample) is documented as unobserved and typed Int64. (#864)
- **Lega Basket:** the play-by-play `side` is documented as what it is, the basket the court zone is measured from
  (0 = x 0, 1 = x 100), not the home/visitor team (that is `team_side`). (#864)
