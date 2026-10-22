- **ESPN parsers (`_common_espn_parsers`, every `espn_<lg>_<short>` wrapper):** every `id` / `*_id` column is
  `Int64` (it was String, or Float64 where the pandas hop met a missing value), so `parse_fpi` joins
  `parse_weekly_powerindex`. `now_id` (`"1-44686270"`) is not an integer and stays String.
- **ESPN hand-written families:** `espn_<lg>_schedule` (`id`, `game_id`, `*_id`, `home_/away_id`,
  `*_venue_id`, `*_conference_id`, `groups_id`; `game_id` was Int32), `espn_<lg>_teams` (`team_id`),
  `espn_<lg>_game_rosters` (every `*_id`; `team_id` / `game_id` / `position_id` were Int32) are `Int64`.
- **ESPN snapshots / content / crosswalks:** `espn_injuries_snapshot` and `espn_depthcharts_snapshot` ids,
  `parse_espn_content` `id`, and `ncaa_espn_team_crosswalk` `espn_team_id` / `espn_conference_id` are `Int64`
  (were Utf8). `load_{nba,wnba,mbb,wbb}_{team,player,schedule}_crosswalk` cast their `espn_*` ids to `Int64`.
- **NFL processor (`NFLPlayProcess`):** the `*_player_id` columns (String) and `pos_team`, `lag_pos_team`,
  `lead_pos_team`, `lead_pos_team2`, `start.pos_team.id` (Int32) are `Int64`, as in the CFB processor.
  A source whose ids are not integers (NFL.com gsis `"00-0034796"`) keeps them String.
