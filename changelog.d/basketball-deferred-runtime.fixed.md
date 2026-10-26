- **International basketball dtypes:** a column name has one dtype across a family's tables:
  `fiba_gamepage_box_teams.blocks_received` is Int64 (was String), and `utc_offset` is Float64 on every GDAP table (a
  half-hour zone such as India +5.5 is a fraction). (#864)
- **International basketball dtypes:** VTB Infobasket `plus_minus` / `player_position` / `height` / `weight`, the KZS
  match `season` and the TBF team-box `efficiency` are Int64; SCB `shirt_number` is always String (a `00` jersey
  survives); Sportality per-game averages (`blkpg`, `orpg`, `plus_minus_pg`) are Float64. (#864)
- **CBA:** without `CBA_PROXY` a request from a datacenter fails in seconds (5 s connect timeout, no retry of a refused
  or timed-out connect) instead of after about three minutes; read timeouts, mid-response resets and proxied failures
  are still retried. Set `CBA_DIRECT=1` where direct access works for the full timeout and retries. (#864)
- **CBA:** a POST route called without `json=` raises `TypeError`; the generated wrappers always send it, so their
  behaviour is unchanged. (#864)
- **CBA numbers:** about 100 display-string columns (`'21.6'` averages, `'39.4%'` percentages) across the season lists,
  team tables, advanced box and shot zones are Float64. A percent string is a fraction 0-1, so
  `cba_player_offensive.turnovers_rate` ('13.5%') is 0.135, while `cba_team_four`'s numeric 13.5 stays 0-100. (#864)
- **CBA numbers:** thousands commas are dropped (`'1,234'`), `'12.0'` in an integer column is 12, `'nan'` / `'inf'` are
  null; `season` and the schedule win/loss counts are Int64, jersey numbers and code-table values stay text, and
  columns null in every capture take the family's dtype. (#864)
- **VTB Infobasket:** an empty string in an id, rank or number column is null; the roster `rank` (blank in every
  sample) is documented as unobserved and typed Int64. (#864)
- **Lega Basket:** the play-by-play `side` is documented as what it is, the basket the court zone is measured from
  (0 = x 0, 1 = x 100), not the home/visitor team (that is `team_side`). (#864)
- **Genius:** HTML pages served without a declared charset are read as UTF-8, so accented competition and team names
  (French, Greek) no longer come out garbled (`requests` assumed ISO-8859-1). (#864)
