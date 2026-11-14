- **Basketball:** the release loaders' season floors match each release's first published season after the 2026-11-13
  backfills: `load_mbb_player_season_stats` 2003; `load_nba_game_rosters`, `_officials`, `_team_season_stats` 1994;
  `load_wnba_team_season_stats` 1997; `load_wbb_standings` 2001; `load_wnba_player_core` 2002. (#1183)
- **Basketball:** `load_wbb_player_season_stats` starts at 2010; `load_mbb_pbp` / `_player_boxscore` / `_team_boxscore`
  and `load_wnba_team_boxscore` at 2003; `load_wbb_pbp` / `_player_boxscore` / `_team_boxscore` at 2004. (#1183)
- **Basketball:** `load_wbb_ratings` and `load_nba_player_impact` start at their first published season; the filled
  WBB/WNBA hole notes are gone and `load_mbb_pbp` (2004-2005) / `load_mbb_officials` (2010) name their holes. (#1183)
