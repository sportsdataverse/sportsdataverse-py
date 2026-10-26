- **Hockey:** `sportsdataverse.hockey.sihf` wraps the keyless Swiss Ice Hockey Federation statistics API
  (`data.sihf.ch`) for every SIHF league (National League, Swiss League, MyHockey League, amateur, junior and women's
  leagues), with eleven `sihf_*` wrappers. (#779)
- **Hockey:** season tables `sihf_results`, `sihf_standings`, `sihf_player_stats` and `sihf_goalie_stats` take a
  `phase_id`; `sihf_phases` lists a league and season's phases (ids differ by league, season and table) with names and
  game counts. (#779)
- **Hockey:** per-game `sihf_pbp` (goals, penalties, goaltender changes, revoked goals, shootout attempts),
  `sihf_game_info`, `sihf_game_player_stats`, `sihf_game_goalie_stats`, `sihf_game_team_stats` and `sihf_game_lineups`.
  (#779)
- **Hockey:** SIHF answers an unknown season, league or phase with another one (`Season=1890` gives the current season);
  the runtime compares the body's filter echo with the request and raises `NoDataError` when they differ. (#779)
- **Hockey:** `sihf_standings` counts every game by default (`venue="All"`; unasked, SIHF answers the away-games table),
  and a table body with fewer rows than it announces raises `AssetFetchError`. (#779)
