- **NBA / WNBA / MBB / WBB:** `team_game_expected` (BB-D40) adds up the shot-quality model's expected points over the
  ESPN plays by situation (second chance, off turnover, fast break, paint) and zone, with xTS% and expected ratings;
  `game_page_frames(..., shot_cells=)` serves it live. (#1161)
- **NBA / WNBA / MBB / WBB:** `shot_cells` / `shot_xfg` port BB-D9's per-shot expectation, as of the game date. (#1161)
