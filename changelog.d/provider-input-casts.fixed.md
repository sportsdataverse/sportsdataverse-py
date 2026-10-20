- **KenPom:** `parse_kenpom_page()` keeps a jersey `"00"` (team and box pages): `read_html` typed the jersey column,
  so `"00"` became 0. Any other zero-padded text column also stays text. (#TBD)
- **stats.nba.com / stats.wnba.com:** a float or numpy `GameID` is sent as `"0022400001"`; it was sent as
  `"22400001.0"`, which the host rejects with HTTP 400. A malformed `SDV_PY_NBA_STATS_*` value raises naming the variable. (#TBD)
- **MLB Stats API:** `mlb_api_extra` sends a set, numpy array or float `league_id` / `sport_ids` through the shared `_csv`
  (`"103,104"`, `"103"`); a set went out as `"{103, 104}"`. (#TBD)
- **Fox / CBS / Yahoo / Shield / NCAA adapters:** an id-map row with float ids adapts like one with text ids (`"401671775"`,
  not `"401671775.0"`); a None / NaN / `pd.NA` id counts as missing. (#TBD)
