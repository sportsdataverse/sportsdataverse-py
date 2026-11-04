- **Basketball:** `nba_team_luck`, `wnba_team_luck`, `mbb_team_luck` and `wbb_team_luck` give Pythagorean
  expected wins against actual wins (`luck_wins`) and the close-game record (BB-D3's close games, within 5
  in the last 5:00), with `_rank`, `_pct` and `_n`; the exponent is fitted per league on held-out-checked
  seasons by `tools/fit_team_luck_pythag.py` (NBA 14.02, WNBA 10.64, MBB 8.07, WBB 6.80) (BB-D22). (#NNN)
