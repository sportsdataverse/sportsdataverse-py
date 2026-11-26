- **NFL:** `nfl_fantasy_projection` charges fumbles lost. The loader emits them only by play type (sack, rushing,
  receiving), so the scored `fumbles_lost` was always absent and counted zero; `season_player_rates` now sums the
  three when the single column is missing. (#1336)
