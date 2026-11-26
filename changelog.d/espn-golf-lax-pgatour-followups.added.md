- **PGA TOUR GraphQL:** eleven more runtime-only `pgatour_*` wrappers (21 in all): schedule years, tournaments,
  past champions and results, the field, hole-by-hole, leaderboard stats, course and hole difficulty, scorecard
  stats and a player's tournament results, each parsed to polars. (#1298)
- **PGA TOUR GraphQL:** `pgatour_past_results` raises `NoDataError` for a season the tournament has no edition in
  (the API answers the current edition instead); `pgatour_past_champions` carries its `tournament_id`. (#1298)
- **Golf:** `espn_golf_*` `league=` adds `ntw` (Korn Ferry), `tgl`, `wtgl`, `mens-olympics-golf` and
  `womens-olympics-golf`; the TGL / WTGL team match-play shorts answer for those two only. (#1298)
