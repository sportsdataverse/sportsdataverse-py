- **NBA / WNBA / MBB / WBB:** `team_game_expected` (BB-D40) puts the shot-quality model's expected points
  on every ESPN field-goal attempt and adds them up by situation (second chance, off turnover, fast
  break, paint) and zone (rim, short midrange, midrange, corner and above-the-break threes), with xTS%
  and the expected offensive and defensive ratings; `shot_cells` / `shot_xfg` port the per-shot
  expectation, and `game_page_frames(..., shot_cells=)` serves it live.
