- **ESPN parsers (`_common_espn_parsers`, every `espn_<lg>_<short>` wrapper):** every `id` / `*_id` column is
  `Int64` (it was String, or Float64 where the pandas hop met a missing value), so `parse_fpi` joins
  `parse_weekly_powerindex`. `now_id` (`"1-44686270"`) is not an integer and stays String. (#981)
- **ESPN hand-written families:** `espn_<lg>_schedule` (`id`, `game_id`, `*_id`, `home_/away_id`,
  `*_venue_id`, `*_conference_id`, `groups_id`; `game_id` was Int32), `espn_<lg>_teams` (`team_id`),
  `espn_<lg>_game_rosters` (every `*_id`; `team_id` / `game_id` / `position_id` were Int32) are `Int64`. (#981)
- **ESPN snapshots / content / crosswalks:** `espn_injuries_snapshot` and `espn_depthcharts_snapshot` ids,
  `parse_espn_content` `id`, and `ncaa_espn_team_crosswalk` `espn_team_id` / `espn_conference_id` are `Int64`
  (were Utf8). `load_{nba,wnba,mbb,wbb}_{team,player,schedule}_crosswalk` cast their `espn_*` ids to `Int64`. (#981)
- **NFL processor (`NFLPlayProcess`):** the `*_player_id` columns (String) and `pos_team`, `lag_pos_team`,
  `lead_pos_team`, `lead_pos_team2`, `start.pos_team.id` (Int32) are `Int64`, as in the CFB processor.
  A source whose ids are not integers (NFL.com gsis `"00-0034796"`) keeps them String. (#981)
- **NHL API parsers (`parse_nhl_web_*`, `parse_nhl_records`, `parse_nhl_stats_rest`, `parse_nhl_edge_*`) and
  `nhl_edge_skating_value`:** every `id` / `*_id` is `Int64` (was Float64 / String by payload, `player_id` a
  Utf8 pin); stats-rest `formatted_season_id` and the country `id` are text and stay String. (#986)
- **HockeyTech (PWHL + 19 leagues: parsers, `<lg>_pbp`, schedules, rosters, leaders, summaries):** every `id` /
  `*_id` is `Int64` (`player_id` was Float64 in pbp, `team_id` / `game_id` String or the caller's dtype); the
  game-summary `home_team_id` / `away_team_id` / `game_season_id` are null, not `""`, when it is unavailable. (#PRC)
