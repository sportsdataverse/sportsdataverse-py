- **Basketball:** `nba_player_war`, `wnba_player_war`, `mbb_player_war` and `wbb_player_war` give our own WAR and VORP
  from our BPM, RAPM and BOBAN (college: box BPM), team-anchored and centred so replacement level is 0, with `_rank`,
  `_pct` and `_n` (BB-D26). (#1114)
- **Basketball:** the replacement level (the least-used 15% of possessions) and points per win are fitted per league
  by `tools/fit_player_war.py` (NBA -1.98 per 100 and 31.56, WNBA -3.18 and 31.31, MBB -1.66, WBB -1.99). (#1114)
- **Basketball:** WAR repairs the release's pro BPM team constant (five times too large) and ignores its `war`
  column, which double-counts possessions and adds up to more wins than the league played. (#1114)
