- **KenPom:** `parse_kenpom_page()` keeps a zero-padded text column (jersey `"00"`) as text instead of casting it to 0. (#TBD)
- **stats.nba.com / stats.wnba.com:** a float or numpy `GameID` is sent as `"0022400001"`; it was sent as
  `"22400001.0"`, which the host rejects with HTTP 400. A malformed `SDV_PY_NBA_STATS_*` value raises naming the variable. (#TBD)
- **MLB Stats API:** `mlb_api_extra` sends a set, numpy array or float `league_id` / `sport_ids` through the shared `_csv`
  (`"103,104"`, `"103"`); a set went out as `"{103, 104}"`. (#TBD)
- **Fox adapters:** an id-map row with float ids adapts like one with text ids (`"401671775"`, not `"401671775.0"`), and a
  NaN team id counts as missing. (#TBD)
