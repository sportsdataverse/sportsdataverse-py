- **NBA / WNBA / MBB / WBB:** the release loaders' season floors match the first season each release publishes after
  the 2026-11-13 backfills: `load_mbb_player_season_stats` 2003, `load_nba_game_rosters`, `load_nba_officials` and
  `load_nba_team_season_stats` 1994, `load_wnba_team_season_stats` 1997, `load_wbb_standings` 2001. (#1183)
- **NBA / WNBA / MBB / WBB:** `load_wnba_player_core` starts at 2002, `load_wbb_player_season_stats` at 2010,
  `load_mbb_pbp` / `_player_boxscore` / `_team_boxscore` and `load_wnba_team_boxscore` at 2003, `load_wbb_pbp` /
  `_player_boxscore` / `_team_boxscore` at 2004, `load_wbb_ratings` and `load_nba_player_impact` at their first. (#1183)
- **MBB / WBB / WNBA:** the notes for the filled WBB / WNBA holes are gone; `load_mbb_pbp` (2004-2005) and
  `load_mbb_officials` (2010) now name their remaining holes. (#1183)
