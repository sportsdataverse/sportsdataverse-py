---
title: WNBA — additional Python functions
sidebar_label: Additional functions
description: "WNBA — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# WNBA — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.wnba`
not covered by the generated API-endpoint reference above.

## Fox

| Function | Summary |
|---|---|
| [fox_wnba_boxscore](additional/fox.md#fox_wnba_boxscore) | WNBA boxscore (long: one row per player-stat). |
| [fox_wnba_event_matchup](additional/fox.md#fox_wnba_event_matchup) | Fox Sports wnba pregame team-stat comparison (one row per stat). |
| [fox_wnba_event_recap](additional/fox.md#fox_wnba_event_recap) | Fox Sports wnba postgame top performers (one row per player). |
| [fox_wnba_event_standings](additional/fox.md#fox_wnba_event_standings) | Fox Sports wnba the two teams' standings context. |
| [fox_wnba_league_conferences](additional/fox.md#fox_wnba_league_conferences) | Fox Sports wnba conference / group directory. |
| [fox_wnba_league_header](additional/fox.md#fox_wnba_league_header) | Fox Sports wnba league header (one row). |
| [fox_wnba_league_leaders](additional/fox.md#fox_wnba_league_leaders) | WNBA statistical leaders (`stats-con`); who=player\|team. |
| [fox_wnba_league_odds](additional/fox.md#fox_wnba_league_odds) | Fox Sports wnba league odds board (one row per team per game). |
| [fox_wnba_league_player_news](additional/fox.md#fox_wnba_league_player_news) | Fox Sports wnba league-wide player news feed. |
| [fox_wnba_league_polls](additional/fox.md#fox_wnba_league_polls) | Fox Sports wnba rankings / polls rendered as standings tables. |
| [fox_wnba_league_schedule](additional/fox.md#fox_wnba_league_schedule) | Fox Sports wnba league schedule nav selections. |
| [fox_wnba_league_scores](additional/fox.md#fox_wnba_league_scores) | Fox Sports wnba league scores nav selections. |
| [fox_wnba_league_standings](additional/fox.md#fox_wnba_league_standings) | Fox Sports wnba league-wide standings tables. |
| [fox_wnba_league_stat_leaders](additional/fox.md#fox_wnba_league_stat_leaders) | Fox Sports wnba league stats landing leaders. |
| [fox_wnba_odds](additional/fox.md#fox_wnba_odds) | WNBA game odds six-pack (spread / to-win / total per team). |
| [fox_wnba_pbp](additional/fox.md#fox_wnba_pbp) | WNBA play-by-play (one row per play; period-based). |
| [fox_wnba_scoreboard](additional/fox.md#fox_wnba_scoreboard) | Fox Sports wnba scoreboard nav selections (weeks / dates / groups). |
| [fox_wnba_scorechip](additional/fox.md#fox_wnba_scorechip) | Fox Sports wnba compact live score chip (raw dict -- live-only, uncaptured shape). |
| [fox_wnba_scores_segment](additional/fox.md#fox_wnba_scores_segment) | Fox Sports wnba one row per game in a scoreboard segment. |
| [fox_wnba_standings](additional/fox.md#fox_wnba_standings) | WNBA standings for a team's conference/division. |
| [fox_wnba_team_gamelog](additional/fox.md#fox_wnba_team_gamelog) | WNBA team game log (long: one row per game-stat). |
| [fox_wnba_team_header](additional/fox.md#fox_wnba_team_header) | Fox Sports wnba team header (one row). |
| [fox_wnba_team_roster](additional/fox.md#fox_wnba_team_roster) | WNBA team roster (one row per player). |
| [fox_wnba_team_stats](additional/fox.md#fox_wnba_team_stats) | WNBA team stat leaders by category. |
| [fox_wnba_teamnav](additional/fox.md#fox_wnba_teamnav) | Fox Sports wnba team directory (one row per team). |
| [fox_wnba_teams](additional/fox.md#fox_wnba_teams) | WNBA team directory (`fox_team_id` / `fox_team_name` / `fox_section`). |

## Play-by-play, schedule & rosters

| Function | Summary |
|---|---|
| [espn_wnba_game_officials](additional/play-by-play-schedule-rosters.md#espn_wnba_game_officials) | Pull the officials assigned to a WNBA game. |
| [espn_wnba_player_stats](additional/play-by-play-schedule-rosters.md#espn_wnba_player_stats) | Pull a WNBA athlete's ESPN **season** stat line. |
| [espn_wnba_schedule](additional/play-by-play-schedule-rosters.md#espn_wnba_schedule) | espn_wnba_schedule - look up the WNBA schedule for a given season |
| [espn_wnba_team_stats](additional/play-by-play-schedule-rosters.md#espn_wnba_team_stats) | Pull ESPN team season stats for a WNBA team. |

## Wnba

| Function | Summary |
|---|---|
| [wnba_aging_curve](additional/wnba.md#wnba_aging_curve) | WNBA aging curve -- the NBA core bound to `league="wnba"`. |
| [wnba_availability](additional/wnba.md#wnba_availability) | WNBA availability -- the NBA core bound to `league="wnba"`. |
| [wnba_career_trajectory](additional/wnba.md#wnba_career_trajectory) | WNBA career trajectory -- the NBA core bound to `league="wnba"`. |
| [wnba_draft_model](additional/wnba.md#wnba_draft_model) | Project WNBA prospect career value + draft probability from draft slot. |
| [wnba_enhanced_pbp](additional/wnba.md#wnba_enhanced_pbp) | Return a normalised enhanced play-by-play frame for a WNBA game. |
| [wnba_expected_turnovers](additional/wnba.md#wnba_expected_turnovers) | WNBA expected turnovers / ball-security skill (`league_id="10"`). |
| [wnba_foul_drawing](additional/wnba.md#wnba_foul_drawing) | WNBA foul-drawing / FT-generation (`league_id="10"`). |
| [wnba_in_game_win_prob](additional/wnba.md#wnba_in_game_win_prob) | WNBA in-game win probability (league_id='10'). See sportsdataverse.nba.nba_game_predict.nba_in_game_win_prob. |
| [wnba_live_boxscore](additional/wnba.md#wnba_live_boxscore) | Fetch and parse WNBA cdn.wnba.com liveData boxscore for a game. |
| [wnba_live_pbp](additional/wnba.md#wnba_live_pbp) | Fetch and parse WNBA cdn.wnba.com liveData play-by-play for a game. |
| [wnba_matchup_drapm](additional/wnba.md#wnba_matchup_drapm) | WNBA matchup defensive RAPM (`league_id="10"`). |
| [wnba_on_court](additional/wnba.md#wnba_on_court) | Return the rotation-keyed on-court player frame for a WNBA game. |
| [wnba_pbp_disk](additional/wnba.md#wnba_pbp_disk) | _No description available._ |
| [wnba_play_context](additional/wnba.md#wnba_play_context) | Return a WNBA game's possessions with the full CTG play-context surface. |
| [wnba_player_crosswalk](additional/wnba.md#wnba_player_crosswalk) | Build the WNBA cross-source player crosswalk (ESPN / WNBA Stats / Fox). |
| [wnba_player_props](additional/wnba.md#wnba_player_props) | WNBA player props (league_id='10'). See sportsdataverse.nba.nba_player_props.nba_player_props. |
| [wnba_playtype_ratings](additional/wnba.md#wnba_playtype_ratings) | WNBA Synergy play-type-adjusted offense/defense (`league_id="10"`). |
| [wnba_possessions](additional/wnba.md#wnba_possessions) | Return the possession-level lineup stint matrix for a WNBA game. |
| [wnba_predict_games](additional/wnba.md#wnba_predict_games) | WNBA vectorized pregame predictions (league_id='10'). See sportsdataverse.nba.nba_game_predict.nba_predict_games. |
| [wnba_predict_margin](additional/wnba.md#wnba_predict_margin) | WNBA expected margin (league_id='10'). See sportsdataverse.nba.nba_game_predict.predict_margin. |
| [wnba_predict_total](additional/wnba.md#wnba_predict_total) | WNBA expected total (league_id='10'). See sportsdataverse.nba.nba_game_predict.predict_total. |
| [wnba_rapm_from_games](additional/wnba-2.md#wnba_rapm_from_games) | Compute per-player RAPM estimates over a sequence of WNBA games. |
| [wnba_referee_assignments](additional/wnba-2.md#wnba_referee_assignments) | Fetch and parse WNBA referee assignments for a given date from official.nba.com. |
| [wnba_rookie_projection](additional/wnba-2.md#wnba_rookie_projection) | WNBA rookie/sophomore projection -- composes the WNBA draft/aging/availability pieces. |
| [wnba_schedule_crosswalk](additional/wnba-2.md#wnba_schedule_crosswalk) | Build the WNBA cross-source schedule crosswalk (ESPN / WNBA Stats). |
| [wnba_shot_value](additional/wnba-2.md#wnba_shot_value) | WNBA one-call shot-value spine (`league_id="10"`). |
| [wnba_team_clutch](additional/wnba-2.md#wnba_team_clutch) | WNBA clutch skill (league_id='10'). See sportsdataverse.nba.nba_clutch.nba_team_clutch. |
| [wnba_team_crosswalk](additional/wnba-2.md#wnba_team_crosswalk) | Build the WNBA cross-source team crosswalk (ESPN / WNBA Stats / Fox). |
| [wnba_team_ratings](additional/wnba-2.md#wnba_team_ratings) | WNBA team ratings (league_id='10'). See sportsdataverse.nba.nba_team_ratings.nba_team_ratings. |
| [wnba_tracking_drive_value](additional/wnba-2.md#wnba_tracking_drive_value) | WNBA drive value + rim-pressure (`league_id="10"` by-reference shim). |
| [wnba_tracking_pass_value](additional/wnba-2.md#wnba_tracking_pass_value) | WNBA expected-assists / passer value (`league_id="10"` by-reference shim). |
| [wnba_tracking_reb_oe](additional/wnba-2.md#wnba_tracking_reb_oe) | WNBA rebounding-over-expected (`league_id="10"` by-reference shim). |
| [wnba_tracking_rim_protect_value](additional/wnba-2.md#wnba_tracking_rim_protect_value) | WNBA rim-protection / shot-defend points-saved (`league_id="10"` |
| [wnba_tracking_shot_diet_value](additional/wnba-2.md#wnba_tracking_shot_diet_value) | WNBA catch-&-shoot vs pull-up points-over-expected (`league_id="10"` |
| [wnba_tracking_touch_value](additional/wnba-2.md#wnba_tracking_touch_value) | WNBA touch / possession-time value (`league_id="10"` by-reference shim). |
| [wnba_win_prob_from_margin](additional/wnba-2.md#wnba_win_prob_from_margin) | WNBA home win probability (league_id='10'). See sportsdataverse.nba.nba_game_predict.win_prob_from_margin. |

## Other

| Function | Summary |
|---|---|
| [load_wnba_stats_leaguedash](additional/other.md#load_wnba_stats_leaguedash) | Load one asset family of the `wnba_stats_leaguedash` release. |
| [load_wnba_stats_lineups](additional/other.md#load_wnba_stats_lineups) | Load season-level WNBA 5-man lineup statistics (deprecated). |
| [load_wnba_stats_player_season_stats](additional/other.md#load_wnba_stats_player_season_stats) | Load season-level WNBA player statistics (deprecated). |
| [load_wnba_stats_standings](additional/other.md#load_wnba_stats_standings) | Load season-level WNBA standings (deprecated). |
| [load_wnba_stats_team_season_stats](additional/other.md#load_wnba_stats_team_season_stats) | Load season-level WNBA team statistics (deprecated). |
| [most_recent_wnba_season](additional/other.md#most_recent_wnba_season) | most_recent_wnba_season - return the most recent (likely-completed) WNBA season year. |
| [build_athlete_identity_lookup](additional/other.md#build_athlete_identity_lookup) | R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters. |
| [build_wnba_season_wp](additional/other.md#build_wnba_season_wp) | A WNBA season's play-by-play with win-probability columns joined in. |
| [espn_wnba_teams](additional/other.md#espn_wnba_teams) | espn_wnba_teams - look up WNBA teams |
| [make_prob_by_context](additional/other.md#make_prob_by_context) | Marginal FG% tables by defender distance and by shot clock. |
| [make_prob_joint](additional/other.md#make_prob_joint) | Independence-combined defender x shot-clock make probability. |
| [score_shot_xpoints](additional/other.md#score_shot_xpoints) | Score each shot with expected points from the league-average baseline. |
| [scoreboard_event_parsing](additional/other.md#scoreboard_event_parsing) | _No description available._ |
| [shooter_talent](additional/other.md#shooter_talent) | Regressed shooter true-talent: make%-above-expected, shrunk to the mean. |
| [shot_selection_quality](additional/other.md#shot_selection_quality) | Player shot-selection quality: mean expected value vs the league mean. |
| [zone_value_map](additional/other.md#zone_value_map) | Per-player per-zone value map: points and expected points per shot. |
