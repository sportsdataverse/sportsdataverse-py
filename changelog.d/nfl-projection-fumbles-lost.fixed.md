- **NFL:** `nfl_fantasy_projection` and `score_fantasy` charge fumbles lost. The loader emits them only by play type
  (sack, rushing, receiving), so the scored `fumbles_lost` was always absent and counted zero; both now sum the three
  when the single column is missing or all null. (#1339)
