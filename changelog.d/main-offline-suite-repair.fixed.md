- **Golf:** a 204 from the PGA TOUR API gives `pgatour_*` an empty answer and a 200 with no `data`
  raises `AssetFetchError` (both raised `NoDataError`); `pgatour_schedule` / `pgatour_tour_cup` type
  `season` as integer. (#1302)
- **Docs:** returns tables for `nhl_goalie_saves`, `nhl_score_distribution`, `nhl_total_goals_distribution`;
  `minutes_exp` described on `nba_player_props` / `wnba_player_props`; 32 `pgatour_*` columns reworded. (#1302)
