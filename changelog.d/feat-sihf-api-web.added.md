- **Hockey:** `sportsdataverse.hockey.sihf` wraps the keyless Swiss Ice Hockey Federation statistics
  API (`data.sihf.ch`), covering every SIHF league (National League, Swiss League, MyHockey
  League, the amateur, junior and women's leagues). There are eleven `sihf_*` wrappers. Four return
  season tables: `sihf_results`, `sihf_standings`, `sihf_player_stats` and `sihf_goalie_stats`.
  `sihf_phases` lists the `phase_id` values those four take for a league and season (the ids
  differ by league, season and table), with each phase's name and game count.
  Six read one game: `sihf_pbp` (goals, penalties, goaltender changes, revoked goals and
  shootout attempts), `sihf_game_info`, `sihf_game_player_stats`, `sihf_game_goalie_stats`,
  `sihf_game_team_stats` and `sihf_game_lineups`. SIHF answers an unknown season, league or
  phase with another one (`Season=1890` returns the current season), and only the body's
  filter echo shows it. The runtime compares that echo with the request and raises
  `NoDataError` when they differ, so a wrong season is never returned under the requested label.
  `sihf_standings` counts every game by default (`venue="All"`; asked nothing, SIHF answers the
  away-games table), and a table body with fewer rows than it announces raises `AssetFetchError`.
